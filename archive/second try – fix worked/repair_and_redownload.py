#!/usr/bin/env python3
"""
Repair downloader:
- Detect empty NCT folders under DOWNLOAD_DIR
- Read CSV/RIS
- Parse messy 'Study Documents' strings to extract (doc_type, url) pairs
- Download PDFs ONLY for the empty NCTs
"""

import os
import re
import time
import json
import pandas as pd
import requests
from typing import List, Tuple, Dict

# ============== CONFIG ==============
DOWNLOAD_DIR = "/Users/sg/Desktop/nih/informed consent/downloads"
CSV_FILE     = "ctg-studies (informed consent).csv"
RIS_FILE     = "ctg-studies (informed consent).ris"
DOCS_COLUMN  = "Study Documents"
NCT_COLUMN   = "NCT Number"

# Networking
TIMEOUT_SECS = 60
PAUSE_SECS   = 0.1   # small delay to be polite
HEADERS = {
    # a vanilla UA helps avoid some CDNs rejecting requests
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

# Filenames
MAX_NAME = 80
# ====================================


def normalize_nct(x: str) -> str:
    """Convert variations like 'nct4019' or 'NCT125' -> 'NCT00004019' / 'NCT00000125'."""
    s = str(x).strip().upper()
    s = s.replace(" ", "")
    s = s.replace("NCT", "")
    s = re.sub(r"\D", "", s)  # keep digits
    return "NCT" + s.zfill(8)


def safe(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip())
    return cleaned[:MAX_NAME].strip("._-")


def find_empty_folders(root: str) -> List[str]:
    """Return normalized NCT IDs for any *existing* subfolders with zero files."""
    empties = []
    if not os.path.isdir(root):
        print(f"[WARN] Download root not found: {root}")
        return empties
    for entry in os.listdir(root):
        path = os.path.join(root, entry)
        if not os.path.isdir(path):
            continue
        # consider files only (ignore subdirs just in case)
        files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        if len(files) == 0:
            # normalize the folder name to a canonical NCT just in case
            empties.append(normalize_nct(entry))
    return sorted(set(empties))


def parse_ris(path: str) -> Dict[str, Dict[str, str]]:
    """Very light RIS parser keyed by normalized NCT ID -> {author, year, title}."""
    if not os.path.exists(path):
        print(f"[WARN] RIS not found: {path} (filenames will default to NA)")
        return {}
    meta = {}
    with open(path, encoding="utf-8", errors="ignore") as fh:
        rec = {}
        for ln in fh:
            tag = ln[:2].strip()
            val = ln[6:].strip()
            if not tag:
                continue
            if tag == "TY" and rec:  # new record
                if "ID" in rec:
                    nid = normalize_nct(rec["ID"])
                    meta[nid] = {
                        "author": rec.get("AU", "").split(",")[0],
                        "year": rec.get("PY", ""),
                        "title": rec.get("TI", ""),
                    }
                rec = {}
            rec[tag] = val
        # last
        if "ID" in rec:
            nid = normalize_nct(rec["ID"])
            meta[nid] = {
                "author": rec.get("AU", "").split(",")[0],
                "year": rec.get("PY", ""),
                "title": rec.get("TI", ""),
            }
    return meta


def extract_pairs(cell: str) -> List[Tuple[str, str]]:
    """
    Robustly extract (doc_type, url) from a Study Documents cell.
    Handles:
      - "X, https://...pdf" (canonical)
      - "X | https://...pdf"
      - "... Prot_SAP_ICF_000.pdf" (combined)
      - extra commas in descriptors
    Strategy:
      - split by pipe to get chunks
      - for each chunk, find *all* PDF URLs via regex
      - doc_type = chunk text before URL occurrence (fallback 'Document')
    """
    results: List[Tuple[str, str]] = []
    if not cell or str(cell).strip().lower() in ("nan", "none"):
        return results

    chunks = [c.strip() for c in str(cell).split("|") if c.strip()]
    url_re = re.compile(r"(https?://[^\s,|]+?\.pdf)(?!\S)", re.IGNORECASE)

    for ch in chunks:
        # find all pdf links in chunk
        urls = url_re.findall(ch)
        if urls:
            # for each url, take the prefix text as doc type if present
            for u in urls:
                prefix = ch.split(u, 1)[0].strip().strip(",")
                doc_type = prefix if prefix else "Document"
                results.append((doc_type, u.strip()))
        else:
            # fallback: try simple "label, url" split
            if "," in ch:
                typ, url = ch.split(",", 1)
                if url.strip().lower().startswith("http"):
                    results.append((typ.strip(), url.strip()))
    return results


def download(url: str, outpath: str) -> Tuple[bool, str]:
    try:
        r = requests.get(url, timeout=TIMEOUT_SECS, headers=HEADERS)
        r.raise_for_status()
        with open(outpath, "wb") as fh:
            fh.write(r.content)
        return True, ""
    except Exception as e:
        return False, str(e)


def main():
    print("[1] Scanning for empty NCT folders…")
    empty_ncts = find_empty_folders(DOWNLOAD_DIR)
    print(f"    → found {len(empty_ncts)} empty folders")

    if not empty_ncts:
        print("No empty folders—nothing to do.")
        return

    # Optional: also merge with an existing list if present (best-effort)
    merged = set(empty_ncts)
    if os.path.exists("empty_ncts.txt"):
        with open("empty_ncts.txt") as f:
            extra = [normalize_nct(x) for x in f if x.strip()]
        merged.update(extra)
        print(f"    → merged with empty_ncts.txt, total targets = {len(merged)}")

    # Load RIS (for filenames)
    print("[2] Loading RIS metadata…")
    ris = parse_ris(RIS_FILE)
    print(f"    → metadata for {len(ris)} trials")

    # Load CSV
    print("[3] Loading CSV…")
    df = pd.read_csv(CSV_FILE)
    # Build row map by normalized NCT
    rows_by_nct: Dict[str, Dict] = {}
    for _, row in df.iterrows():
        nct = normalize_nct(row.get(NCT_COLUMN, ""))
        rows_by_nct[nct] = row

    # Iterate missing NCTs that exist in the CSV
    print("[4] Starting selective re-download…")
    success_count = 0
    fail_count = 0
    skipped_missing_in_csv = 0
    errors = []

    for nct in sorted(merged):
        if nct not in rows_by_nct:
            skipped_missing_in_csv += 1
            continue

        row = rows_by_nct[nct]
        docs_cell = str(row.get(DOCS_COLUMN, "") or "")
        pairs = extract_pairs(docs_cell)
        if not pairs:
            print(f"[{nct}] no parsable docs in CSV row; skipping")
            continue

        # RIS metadata for naming
        meta = ris.get(nct, {})
        author = safe(meta.get("author", "") or "NA")
        year   = safe(meta.get("year", "") or "NA")
        title  = safe(meta.get("title", "") or "NA")

        # Ensure folder exists and is still empty (or at least writable)
        folder = os.path.join(DOWNLOAD_DIR, nct)
        os.makedirs(folder, exist_ok=True)

        print(f"[{nct}] retrying {len(pairs)} doc(s)")
        for doc_type, url in pairs:
            if not url.lower().startswith("http"):
                continue
            fname = f"{author}{year}_{safe(doc_type)}_{title}.pdf"
            fpath = os.path.join(folder, fname)

            ok, err = download(url, fpath)
            if ok:
                print(f"   ✔ {os.path.basename(fpath)}")
                success_count += 1
            else:
                print(f"   ✘ {url} – {err}")
                errors.append({"nct": nct, "url": url, "error": err})
                fail_count += 1

            time.sleep(PAUSE_SECS)

    print("\n[Summary]")
    print(f"  Downloads OK : {success_count}")
    print(f"  Downloads ✘  : {fail_count}")
    print(f"  NCTs missing in CSV rows: {skipped_missing_in_csv}")

    # Persist an error report if any
    if errors:
        with open("redownload_errors.json", "w") as fh:
            json.dump(errors, fh, indent=2)
        print("  Wrote error details to redownload_errors.json")


if __name__ == "__main__":
    main()
