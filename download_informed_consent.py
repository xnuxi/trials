#!/usr/bin/env python3

import os
import re
import requests
import pandas as pd

# === CONFIG ===
CSV_FILE = "ctg-studies (informed consent).csv"
RIS_FILE = "ctg-studies (informed consent).ris"
DOCS_COLUMN = "Study Documents"
NCT_COLUMN = "NCT Number"
MAX_TITLE_LEN = 50    # truncate title to avoid excessively long filenames
DOWNLOAD_DIR = "downloads"

# === UTILS ===
def safe(s):
    """Clean string so it's safe for filenames."""
    return re.sub(r'[^a-zA-Z0-9_-]+', '_', s)[:MAX_TITLE_LEN]

def parse_ris(path):
    """Parse RIS and return dict keyed by NCT number containing (author, year, title)."""
    meta = {}
    with open(path, encoding='utf-8', errors='ignore') as f:
        record = {}
        for line in f:
            tag = line[:2].strip()
            val = line[6:].strip()
            if not tag:
                continue
            if tag == 'TY' and record:
                # start of new record → save previous
                if 'ID' in record:
                    nct = record['ID']
                    meta[nct] = {
                        'title': record.get('TI', ''),
                        'year': record.get('PY', ''),
                        'author': record.get('AU', '').split(',')[0]
                    }
                record = {}
            record[tag] = val
        # save last
        if 'ID' in record:
            nct = record['ID']
            meta[nct] = {
                'title': record.get('TI', ''),
                'year': record.get('PY', ''),
                'author': record.get('AU', '').split(',')[0]
            }
    return meta

def download_file(url, outpath):
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(outpath, 'wb') as fh:
            fh.write(r.content)
        print(f"✔ downloaded {outpath}")
    except Exception as e:
        print(f"✘ FAILED {url} → {e}")

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print("[1] loading RIS metadata...")
    ris = parse_ris(RIS_FILE)
    print(f"   → loaded metadata for {len(ris)} trials")

    print("[2] loading CSV...")
    df = pd.read_csv(CSV_FILE)

    for idx, row in df.iterrows():
        nct = str(row[NCT_COLUMN]).strip()
        doclist = str(row.get(DOCS_COLUMN, "")).split('|')
        meta = ris.get(nct, {})
        author = safe(meta.get("author", "") or "NA")
        year   = safe(meta.get("year", "") or "NA")
        title  = safe(meta.get("title", "") or "NA")

        trial_dir = os.path.join(DOWNLOAD_DIR, nct)
        os.makedirs(trial_dir, exist_ok=True)

        for item in doclist:
            item = item.strip()
            if not item:
                continue
            # "Descriptor, URL"
            try:
                doc_type, url = item.split(',', 1)
                doc_type = safe(doc_type.strip())
                url = url.strip()
            except ValueError:
                continue
            # build filename
            filename = f"{author}{year}_{doc_type}_{title}.pdf"
            outpath  = os.path.join(trial_dir, filename)
            if not url.lower().startswith("http"):
                print(f"Skipping malformed url: {url}")
                continue
            download_file(url, outpath)

    print("DONE.")

if __name__ == "__main__":
    main()
