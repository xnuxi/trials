#!/usr/bin/env python3
import os, re, json, shutil
import pandas as pd
from pathlib import Path
from html import escape

# ===== CONFIG =====
CSV_FILE = "ctg-studies (informed consent).csv"
RIS_FILE = "ctg-studies (informed consent).ris"
SITE_DIR = "site"
TITLE = "Informed Consent – Trials Explorer"
# ==================

PDFJS_VIEWER = "https://mozilla.github.io/pdf.js/web/viewer.html?file="

def normalize_nct(x: str) -> str:
    s = str(x).strip().upper().replace(" ", "").replace("NCT", "")
    s = re.sub(r"\D", "", s)
    return "NCT" + s.zfill(8)

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
    Pull (label, url) pairs from CSV 'Study Documents' text.
    Handles 'label, https://...pdf' and '... | https://...pdf' formats.
    """
    out = []
    if not cell or str(cell).strip().lower() in ("nan","none"):
        return out
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
    # Load CSV + RIS
    df = pd.read_csv(CSV_FILE)
    ris = parse_ris(RIS_FILE)

    # Build per-study records from CSV
    studies = []
    for _, r in df.iterrows():
        nct = normalize_nct(r.get("NCT Number",""))
        csv_docs = extract_pairs(r.get("Study Documents",""))
        if not csv_docs:
            # Still include the study so it appears in search
            csv_docs = []
        studies.append({
            "nct": nct,
            "title": ris.get(nct, {}).get("title") or r.get("Study Title", ""),
            "year":  ris.get(nct, {}).get("year", ""),
            "authors": ris.get(nct, {}).get("authors", []),
            "registry_url": r.get("Study URL", ""),
            "csv_docs": [{"label": d[0], "url": d[1]} for d in csv_docs],
        })

    # Ensure site structure
    site = Path(SITE_DIR)
    if site.exists():
        shutil.rmtree(site)
    (site / "studies").mkdir(parents=True, exist_ok=True)
    (site / "assets").mkdir(parents=True, exist_ok=True)

    # Write client-side search index
    index = [{
        "nct": s["nct"],
        "title": s["title"],
        "year": s["year"],
        "authors": s["authors"],
        "registry_url": s["registry_url"],
        "doc_count": len(s["csv_docs"]),
    } for s in studies]
    (site / "assets" / "studies.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    # Styles
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
.small{font-size:12px;color:#9fb0ff}
""", encoding="utf-8")

    # Index page with Fuse.js search
    (site / "index.html").write_text(f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{escape(TITLE)}</title>
<link rel="stylesheet" href="assets/styles.css"/>
<script src="https://cdn.jsdelivr.net/npm/fuse.js@7.0.0"></script>
</head><body>
<div class="container">
  <h1>{escape(TITLE)}</h1>
  <input id="q" type="search" placeholder="Search by NCT, title, author, year…"/>
  <div class="small" style="margin:6px 0 12px 0;">Tip: click a result to open per-study page (with PDF preview).</div>
  <div id="results"></div>
</div>
<script>
const esc=(s)=> (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;")
const card=(r)=> `
  <div class="card">
    <div><a href="studies/${{r.nct}}.html"><strong>${{esc(r.nct)}}</strong></a> – ${{esc(r.title) || "(untitled)"}}</div>
    <div class="meta">
      ${{r.year?`<span class="badge">${{esc(r.year)}}</span>`:""}}
      ${{(r.authors && r.authors.length)?`<span class="badge">${{esc(r.authors[0])}}</span>`:""}}
      ${{r.doc_count?`<span class="badge">${{r.doc_count}} document(s)</span>`:"<span class='badge'>0 document(s)</span>"}}
      ${{r.registry_url?`<a class="badge" href="${{r.registry_url}}" target="_blank" rel="noopener">Registry</a>`:""}}
    </div>
  </div>`;

let DATA=[];
async function init(){{
  DATA = await (await fetch('assets/studies.json')).json();
  render(DATA);
  const fuse = new Fuse(DATA, {{
    keys:['nct','title','authors','year'],
    threshold:0.35,
    ignoreLocation:true
  }});
  document.getElementById('q').addEventListener('input', (e)=>{{
    const v=e.target.value.trim();
    if(!v) return render(DATA);
    render(fuse.search(v).map(x=>x.item));
  }});
}}
function render(items){{
  const el=document.getElementById('results');
  el.innerHTML = items.map(card).join('') || '<div class="meta">No matches.</div>';
}}
init();
</script>
</body></html>
""", encoding="utf-8")

    # Per-study pages (CSV docs only + pdf.js preview of first doc)
    tmpl = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{ptitle}</title>
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
    <h3>Preview</h3>
    {preview}
  </div>
</div></body></html>
"""
    for s in studies:
        nct = s["nct"]
        title = escape(s["title"] or "(untitled)")
        ptitle = f"{nct} – {title}"
        year = escape(s["year"] or "")
        authors = escape(", ".join([a for a in s["authors"] if a]) or "—")
        reglink = f'<a href="{escape(s["registry_url"])}" target="_blank" rel="noopener">Registry page</a>' if s["registry_url"] else ""

        # CSV-doc links (external URLs)
        csv_items = []
        for d in s["csv_docs"]:
            lab = escape(d["label"] or "Document")
            url = escape(d["url"])
            csv_items.append(f'<li>📄 <a href="{url}" target="_blank" rel="noopener">{lab}</a></li>')
        csv_html = "\n".join(csv_items) if csv_items else "<li>No CSV-linked docs.</li>"

        # Preview first doc via pdf.js (if any)
        if s["csv_docs"]:
            first = s["csv_docs"][0]["url"]
            preview_html = f'<iframe class="pdf-iframe" src="{PDFJS_VIEWER}{escape(first)}"></iframe>'
        else:
            preview_html = '<div class="meta">No document to preview.</div>'

        (Path(SITE_DIR)/"studies"/f"{nct}.html").write_text(
            tmpl.format(
                ptitle=ptitle, nct=escape(nct),
                title=title, year=year, authors=authors,
                reglink=reglink,
                csv_docs=csv_html,
                preview=preview_html
            ),
            encoding="utf-8"
        )

    print(f"Built static site → {SITE_DIR}/")

if __name__ == "__main__":
    main()
