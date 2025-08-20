#!/usr/bin/env python3
import os, re, json, shutil
import pandas as pd
from pathlib import Path
from html import escape

# ====== CONFIG ======
DOWNLOAD_DIR = "/Users/sg/Desktop/nih/informed consent/downloads"  # your folders
CSV_FILE     = "ctg-studies (informed consent).csv"
RIS_FILE     = "ctg-studies (informed consent).ris"
SITE_DIR     = "site"
COPY_PDFS    = True   # True = copy PDFs into site/ (big!). False = keep file:// links to your local PDFs OR link to original URLs if present
OPEN_IN_BROWSER = False
# ====================

def normalize_nct(x: str) -> str:
    x = str(x).strip().upper().replace("NCT","")
    x = re.sub(r"\D", "", x)
    return "NCT" + x.zfill(8)

def safe(s: str, maxlen=120) -> str:
    return re.sub(r"[^a-zA-Z0-9._ -]", "_", (s or "")).strip()[:maxlen]

def parse_ris(path: str):
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
    """
    Pull (doc_type, url) pairs from CSV 'Study Documents' text.
    Handles 'label, https://...pdf' and '... | https://...pdf' etc.
    """
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

def main():
    # load CSV + RIS
    df = pd.read_csv(CSV_FILE)
    ris = parse_ris(RIS_FILE)

    # build lookup from CSV
    rows = {}
    for _, r in df.iterrows():
        nct = normalize_nct(r.get("NCT Number",""))
        rows[nct] = {
            "nct": nct,
            "study_title": r.get("Study Title",""),
            "study_url": r.get("Study URL",""),
            "study_docs": extract_pairs(r.get("Study Documents","")),
        }

    # scan local PDFs
    dl_root = Path(DOWNLOAD_DIR)
    studies = []
    for nct_dir in sorted([p for p in dl_root.iterdir() if p.is_dir() and p.name.upper().startswith("NCT")]):
        nct = normalize_nct(nct_dir.name)
        pdfs = sorted([p for p in nct_dir.iterdir() if p.is_file() and p.suffix.lower()==".pdf"])
        row = rows.get(nct, {"nct": nct, "study_title": "", "study_url": "", "study_docs": []})
        rmeta = ris.get(nct, {"title":"", "year":"", "authors":[]})

        studies.append({
            "nct": nct,
            "title": rmeta["title"] or row["study_title"],
            "year": rmeta["year"],
            "authors": rmeta["authors"],
            "registry_url": row["study_url"],
            "csv_docs": [{"label": d[0], "url": d[1]} for d in row["study_docs"]],
            "local_pdfs": [str(p.resolve()) for p in pdfs],  # absolute paths
        })

    # ensure site structure
    site = Path(SITE_DIR)
    if site.exists(): shutil.rmtree(site)
    (site / "studies").mkdir(parents=True, exist_ok=True)
    (site / "assets").mkdir(parents=True, exist_ok=True)

    # optional copy PDFs (warning: huge; otherwise we link to file:// or original URL)
    if COPY_PDFS:
        (site / "pdfs").mkdir(parents=True, exist_ok=True)
        for s in studies:
            tgt = site / "pdfs" / s["nct"]
            tgt.mkdir(parents=True, exist_ok=True)
            for src in s["local_pdfs"]:
                srcp = Path(src)
                if not srcp.exists(): continue
                shutil.copy2(srcp, tgt / srcp.name)

    # write a JSON index for client-side search
    index = []
    for s in studies:
        index.append({
            "nct": s["nct"],
            "title": s["title"],
            "year": s["year"],
            "authors": s["authors"],
            "registry_url": s["registry_url"],
        })
    (site / "assets" / "studies.json").write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")

    # simple CSS
    (site / "assets" / "styles.css").write_text("""
:root{--bg:#0b1020;--fg:#e7ecff;--card:#121833;--muted:#9fb0ff;}
*{box-sizing:border-box}body{margin:0;font-family:ui-sans-serif,system-ui,Segoe UI,Roboto;color:var(--fg);background:linear-gradient(180deg,#0b1020,#0f1733);}
a{color:#a5c3ff;text-decoration:none}a:hover{text-decoration:underline}
.container{max-width:1100px;margin:0 auto;padding:24px}
h1{margin:.2rem 0 1rem 0;font-size:28px}
.card{background:var(--card);border-radius:14px;padding:16px;margin:12px 0;box-shadow:0 6px 24px rgba(0,0,0,.25)}
.meta{color:var(--muted);font-size:14px;margin-top:.25rem}
input[type=search]{width:100%;padding:12px 14px;border-radius:12px;border:1px solid #2b3969;background:#0d1430;color:var(--fg)}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;background:#1c2550;color:#cbd6ff;font-size:12px;margin-right:6px}
.pdf-iframe{width:100%;height:75vh;border:0;border-radius:12px;box-shadow:0 4px 18px rgba(0,0,0,.24)}
.list{list-style:none;padding:0;margin:0}
.list li{margin:6px 0}
""", encoding="utf-8")

    # index.html with Fuse.js search
    (site / "index.html").write_text(f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Informed Consent – Trials Explorer</title>
<link rel="stylesheet" href="assets/styles.css"/>
<script src="https://cdn.jsdelivr.net/npm/fuse.js@7.0.0"></script>
</head><body>
<div class="container">
  <h1>Informed Consent – Trials Explorer</h1>
  <input id="q" type="search" placeholder="Search by NCT, title, author, year… (client-side, instant)"/>
  <div id="results"></div>
</div>
<script>
const fmt = (s)=> (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;")
const card = (r)=> `
  <div class="card">
    <div><a href="studies/${{r.nct}}.html"><strong>${{fmt(r.nct)}}</strong></a> – ${'{'}fmt(r.title){'}'}</div>
    <div class="meta">
      ${{(r.year?`<span class="badge">${{r.year}}</span>`:"")}}
      ${{(r.authors && r.authors.length?`<span class="badge">${{fmt(r.authors[0])}}</span>`:"")}}
      ${{(r.registry_url?`<a class="badge" href="${{r.registry_url}}" target="_blank" rel="noopener">Registry</a>`:"")}}
    </div>
  </div>`

let data = [];
async function init() {{
  data = await (await fetch('assets/studies.json')).json();
  render(data);
  const fuse = new Fuse(data, {{
    keys: ['nct','title','authors','year'],
    threshold: 0.35,
    ignoreLocation: true
  }});
  document.getElementById('q').addEventListener('input', (e) => {{
    const v = e.target.value.trim();
    if(!v) return render(data);
    render(fuse.search(v).map(x=>x.item));
  }});
}}
function render(items) {{
  const el = document.getElementById('results');
  el.innerHTML = items.map(card).join('') || '<div class="meta">No matches.</div>';
}}
init();
</script>
</body></html>
""", encoding="utf-8")

    # per-study pages
    tmpl = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title}</title>
<link rel="stylesheet" href="../assets/styles.css"/>
</head><body><div class="container">
  <a href="../index.html">← Back to index</a>
  <h1>{nct}</h1>
  <div class="card">
    <div><strong>Title:</strong> {title}</div>
    <div class="meta"><strong>Year:</strong> {year} &nbsp; <strong>Authors:</strong> {authors}</div>
    <div class="meta">{reglink}</div>
  </div>

  <div class="card">
    <h3>Documents (from CSV)</h3>
    <ul class="list">
      {csv_docs}
    </ul>
  </div>

  <div class="card">
    <h3>Local PDFs</h3>
    <ul class="list">
      {local_list}
    </ul>
    {preview}
  </div>

</div></body></html>
"""
    for s in studies:
        nct = s["nct"]
        title = escape(s["title"] or "(untitled)")
        year = escape(s["year"] or "")
        authors = escape(", ".join([a for a in s["authors"] if a]) or "")
        reglink = f'<a href="{escape(s["registry_url"])}" target="_blank" rel="noopener">Registry page</a>' if s["registry_url"] else ""

        # CSV-doc links (external CDN)
        csv_items = []
        for d in s["csv_docs"]:
            lab = escape(d["label"] or "Document")
            url = escape(d["url"])
            csv_items.append(f'<li>📄 <a href="{url}" target="_blank" rel="noopener">{lab}</a></li>')
        csv_html = "\n".join(csv_items) if csv_items else "<li>No CSV-linked docs.</li>"

        # Local PDFs (either copied into site/pdfs/ or linked via file://)
        local_items, preview_html = [], ""
        if COPY_PDFS:
            for lp in s["local_pdfs"]:
                name = escape(Path(lp).name)
                rel = f"../pdfs/{nct}/{name}"
                local_items.append(f'<li>📎 <a href="{rel}" target="_blank" rel="noopener">{name}</a></li>')
            # embed first pdf preview if any
            if s["local_pdfs"]:
                first = escape(f"../pdfs/{nct}/{Path(s['local_pdfs'][0]).name}")
                preview_html = f'<iframe class="pdf-iframe" src="{first}#view=FitH&toolbar=1"></iframe>'
        else:
            for lp in s["local_pdfs"]:
                name = escape(Path(lp).name)
                # Link to local file path (will work when opened locally; for web hosting, prefer COPY_PDFS=True)
                local_items.append(f'<li>📎 <a href="file://{escape(lp)}" target="_blank">{name}</a></li>')
            if s["local_pdfs"]:
                preview_html = f'<div class="meta">Preview disabled (COPY_PDFS=False). Enable it to embed PDFs on the page.</div>'

        (Path(SITE_DIR)/"studies"/f"{nct}.html").write_text(
            tmpl.format(
                nct=escape(nct),
                title=title,
                year=year,
                authors=authors or "—",
                reglink=reglink or "",
                csv_docs=csv_html,
                local_list="\n".join(local_items) if local_items else "<li>No local PDFs found.</li>",
                preview=preview_html
            ),
            encoding="utf-8"
        )

    print(f"Built static site → {SITE_DIR}/")
    if OPEN_IN_BROWSER:
        import webbrowser
        webbrowser.open(f"file://{Path(SITE_DIR).resolve()}/index.html")

if __name__ == "__main__":
    main()
