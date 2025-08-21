#!/usr/bin/env python3
import os, re, json, math, hashlib, tempfile
from typing import List, Dict, Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import pdfplumber
import requests
import numpy as np
from openai import OpenAI

# ==== CONFIG ====
CSV_FILE     = "ctg-studies (informed consent).csv"
RIS_FILE     = "ctg-studies (informed consent).ris"
DOWNLOAD_DIR = "/Users/sg/Desktop/nih/informed consent/downloads"
EMBED_MODEL  = "text-embedding-3-small"   # cheap + good; switch to -large if you want
CHAT_MODEL   = "gpt-4o-mini"              # adjust to what you have
TOP_K        = 8
CHUNK_TOKENS = 700                        # approx chars; simple splitter below uses chars
OVERLAP      = 120

# Expect OPENAI_API_KEY in environment (do NOT put keys in the browser)
client = OpenAI()

# ==== HELPERS ====
def normalize_nct(x: str) -> str:
    s = str(x).strip().upper().replace("NCT", "")
    s = re.sub(r"\D", "", s)
    return "NCT" + s.zfill(8)

def load_ris(path: str) -> Dict[str, Dict[str, str]]:
    meta = {}
    if not os.path.exists(path): return meta
    rec = {}
    with open(path, encoding="utf-8", errors="ignore") as fh:
        for ln in fh:
            tag, val = ln[:2].strip(), ln[6:].strip()
            if not tag: continue
            if tag == "TY" and rec:
                if "ID" in rec:
                    nid = normalize_nct(rec["ID"])
                    meta[nid] = {
                        "title": rec.get("TI",""),
                        "year":  rec.get("PY",""),
                        "authors": [rec.get("AU","")] if rec.get("AU") else []
                    }
                rec = {}
            rec[tag] = val
        if "ID" in rec:
            nid = normalize_nct(rec["ID"])
            meta[nid] = {
                "title": rec.get("TI",""),
                "year":  rec.get("PY",""),
                "authors": [rec.get("AU","")] if rec.get("AU") else []
            }
    return meta

def extract_pairs(cell: str):
    out = []
    if not cell or str(cell).strip().lower() in ("nan","none"): return out
    parts = [p.strip() for p in str(cell).split("|") if p.strip()]
    url_re = re.compile(r"(https?://[^\s,|]+?\.pdf)(?!\S)", re.I)
    for p in parts:
        urls = url_re.findall(p)
        if urls:
            for u in urls:
                label = p.split(u,1)[0].strip().strip(",")
                out.append((label or "Document", u))
        else:
            if "," in p:
                lab, rest = p.split(",",1)
                if rest.strip().lower().startswith("http"):
                    out.append((lab.strip(), rest.strip()))
    return out

# Load CSV to map NCT -> doc URLs + registry URL
df = pd.read_csv(CSV_FILE)
CSV_MAP = {}
for _, r in df.iterrows():
    nct = normalize_nct(r.get("NCT Number",""))
    CSV_MAP[nct] = {
        "study_url": r.get("Study URL",""),
        "docs": extract_pairs(r.get("Study Documents","")),
        "study_title": r.get("Study Title",""),
    }

RIS_MAP = load_ris(RIS_FILE)

def get_local_pdfs(nct: str) -> List[str]:
    folder = os.path.join(DOWNLOAD_DIR, nct)
    if not os.path.isdir(folder): return []
    return [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(".pdf")]

def read_pdf_text(path_or_url: str) -> str:
    # if HTTP(S), stream to temp; else open local
    if path_or_url.lower().startswith("http"):
        with requests.get(path_or_url, timeout=60) as r:
            r.raise_for_status()
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(r.content)
                tmp_path = tmp.name
        try:
            text = ""
            with pdfplumber.open(tmp_path) as pdf:
                for page in pdf.pages:
                    text += (page.extract_text() or "") + "\n"
            return text
        finally:
            os.unlink(tmp_path)
    else:
        text = ""
        with pdfplumber.open(path_or_url) as pdf:
            for page in pdf.pages:
                text += (page.extract_text() or "") + "\n"
        return text

def chunk_text(s: str, chunk_chars: int = CHUNK_TOKENS, overlap: int = OVERLAP) -> List[str]:
    s = re.sub(r"\s+\n", "\n", s)
    s = re.sub(r"\n{2,}", "\n\n", s)
    chunks = []
    i = 0
    while i < len(s):
        chunk = s[i:i+chunk_chars]
        chunks.append(chunk)
        i += max(1, chunk_chars - overlap)
    return [c.strip() for c in chunks if c.strip()]

def embed(texts: List[str]) -> np.ndarray:
    if not texts:
        return np.zeros((0, 1536), dtype=np.float32)
    resp = client.embeddings.create(model=EMBED_MODEL, input=texts)
    vecs = [np.array(d.embedding, dtype=np.float32) for d in resp.data]
    return np.vstack(vecs)

def topk(query: str, chunks: List[str], chunk_vecs: np.ndarray, k: int = TOP_K) -> List[str]:
    if not chunks: return []
    qv = embed([query])[0]
    # cosine sim
    denom = (np.linalg.norm(chunk_vecs, axis=1) * np.linalg.norm(qv) + 1e-8)
    sims = (chunk_vecs @ qv) / denom
    idx = np.argsort(-sims)[:k]
    return [chunks[i] for i in idx]

# Simple in-memory cache per NCT
CACHE: Dict[str, Dict] = {}

def build_context_for_nct(nct: str) -> Dict:
    if nct in CACHE:
        return CACHE[nct]

    # Prefer local PDFs (offline snapshot); if none, use first CSV URL(s)
    locals_ = get_local_pdfs(nct)
    sources = []
    if locals_:
        sources = locals_
    else:
        sources = [u for _, u in CSV_MAP.get(nct, {}).get("docs", [])]

    # Read, chunk, embed
    all_chunks = []
    src_map = []  # keep (source, chunk_index_in_source)
    for src in sources[:5]:  # cap to first 5 docs per study for speed; adjust as needed
        try:
            txt = read_pdf_text(src)
            chunks = chunk_text(txt)
            start = len(all_chunks)
            all_chunks.extend(chunks)
            for i in range(len(chunks)):
                src_map.append({"src": src, "i": i})
        except Exception:
            continue

    vecs = embed(all_chunks) if all_chunks else np.zeros((0,1536), dtype=np.float32)

    meta = {
        "nct": nct,
        "title": RIS_MAP.get(nct, {}).get("title") or CSV_MAP.get(nct, {}).get("study_title",""),
        "year": RIS_MAP.get(nct, {}).get("year",""),
        "authors": RIS_MAP.get(nct, {}).get("authors", []),
        "registry_url": CSV_MAP.get(nct, {}).get("study_url",""),
        "doc_urls": [u for _, u in CSV_MAP.get(nct, {}).get("docs", [])],
    }

    CACHE[nct] = {"meta": meta, "chunks": all_chunks, "vecs": vecs, "srcmap": src_map}
    return CACHE[nct]

# ==== API ====
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatIn(BaseModel):
    nct: str
    question: str

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/chat")
def chat(inp: ChatIn):
    nct = normalize_nct(inp.nct)
    ctx = build_context_for_nct(nct)

    chunks = ctx["chunks"]
    if not chunks:
        return {"answer": "I couldn't load any text for this study yet (no readable PDFs). Try again or check the CSV links.", "meta": ctx["meta"]}

    top = topk(inp.question, chunks, ctx["vecs"], k=TOP_K)

    system = (
        "You answer questions about a single clinical study. "
        "Use only the provided context chunks (RIS + PDF text). Cite short quotes. "
        "If you don't see an answer in context, say so honestly."
    )

    ris_bits = json.dumps(ctx["meta"], ensure_ascii=False)
    context_text = "\n\n".join([f"[Chunk {i+1}]\n{c}" for i, c in enumerate(top)])

    msg = [
        {"role": "system", "content": system},
        {"role": "user", "content": f"RIS metadata:\n{ris_bits}\n\nContext chunks from PDFs:\n{context_text}\n\nQuestion: {inp.question}\n\nAnswer clearly and concisely, cite page numbers if visible in the text."}
    ]

    resp = client.chat.completions.create(model=CHAT_MODEL, messages=msg, temperature=0.1)
    answer = resp.choices[0].message.content.strip()
    return {"answer": answer, "meta": ctx["meta"]}
