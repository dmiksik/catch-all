#!/usr/bin/env python3
import json, re
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests

# ====== Konfigurace cest ======
BASE_URL = "https://data.narodni-repozitar.cz"
OUT_DIR  = Path("nrp_dump")
PARQUET  = OUT_DIR / "records_flat.parquet"
RAW_JSONL= OUT_DIR / "records.jsonl"

# HTTP session s povinnou hlavičkou pro JSON
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

# ====== Pomocné funkce ======
def human_bytes(n):
    units = ["B","KB","MB","GB","TB","PB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units)-1:
        f /= 1024.0; i += 1
    return f"{f:,.2f} {units[i]}"

def safe_get(d, path, default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

def normalize_doi(s: str | None) -> str | None:
    """Vrátí „holé“ 10.xxxx/... pokud najde, a očistí běžné prefixy."""
    if not s:
        return None
    s2 = str(s).strip()
    s2 = re.sub(r"(?i)^doi:\s*", "", s2)
    s2 = re.sub(r"(?i)^https?://(?:dx\.)?doi\.org/", "", s2)
    m = re.search(r"(10\.\d{4,9}/\S+)", s2)
    if m:
        return m.group(1).rstrip(" .,)];")
    return s2.rstrip(" .,)];") if s2.startswith("10.") else None

def extract_doi(rec: dict) -> str | None:
    doi = safe_get(rec, ["pids", "doi", "identifier"])
    if doi: 
        return normalize_doi(doi)

    md = rec.get("metadata") or {}

    doi = md.get("doi")
    if doi:
        return normalize_doi(doi)

    for key in ("identifiers", "related_identifiers", "alternate_identifiers"):
        arr = md.get(key)
        if isinstance(arr, list):
            for it in arr:
                if isinstance(it, dict):
                    scheme = str(it.get("scheme") or it.get("type") or "").lower()
                    ident  = it.get("identifier") or it.get("id") or it.get("value") or it.get("text")
                    if scheme == "doi" and ident:
                        cand = normalize_doi(ident)
                        if cand: 
                            return cand
                    for v in it.values():
                        if isinstance(v, str):
                            cand = normalize_doi(v)
                            if cand:
                                return cand
                elif isinstance(it, str):
                    cand = normalize_doi(it)
                    if cand:
                        return cand
    for v in rec.values():
        if isinstance(v, str):
            cand = normalize_doi(v)
            if cand:
                return cand
    return None

def parse_year(date_str: str | int | None) -> int | None:
    if date_str is None:
        return None
    if isinstance(date_str, int):
        return date_str
    s = str(date_str)
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(s, fmt).year
        except Exception:
            pass
    m = re.search(r"\d{4}", s)
    return int(m.group(0)) if m else None

def extract_publication_year(rec: dict, flat_year=None) -> int | None:
    if flat_year is not None and str(flat_year).strip() != "":
        try:
            return int(flat_year)
        except Exception:
            pass

    for path in (["publication_year"],
                 ["metadata","publication_year"],
                 ["metadata","publication_date"],
                 ["metadata","date"]):
        v = safe_get(rec, path)
        y = parse_year(v)
        if y:
            return y

    md = rec.get("metadata") or {}
    dates = md.get("dates")
    if isinstance(dates, list):
        preferred = ("issued", "publication", "published", "pub")
        first_any = None
        for d in dates:
            if not isinstance(d, dict): 
                continue
            val = d.get("date") or d.get("value")
            typ = str(d.get("type") or d.get("description") or "").lower()
            y = parse_year(val) if val else None
            if y:
                if typ in preferred:
                    return y
                if first_any is None:
                    first_any = y
        if first_any:
            return first_any

    for key in ("updated","created"):
        y = parse_year(rec.get(key))
        if y:
            return y
    return None

def extract_title(rec: dict) -> str | None:
    # varianty: metadata.title, metadata.titles[0].title, titles[0].title, title
    title = safe_get(rec, ["metadata","title"])
    if isinstance(title, str) and title.strip():
        return title.strip()

    tarr = safe_get(rec, ["metadata","titles"])
    if isinstance(tarr, list):
        for t in tarr:
            if isinstance(t, dict) and isinstance(t.get("title"), str) and t["title"].strip():
                return t["title"].strip()

    tarr2 = rec.get("titles")
    if isinstance(tarr2, list):
        for t in tarr2:
            if isinstance(t, dict) and isinstance(t.get("title"), str) and t["title"].strip():
                return t["title"].strip()

    t2 = rec.get("title")
    if isinstance(t2, str) and t2.strip():
        return t2.strip()

    return None

def _collect_affils_from_person(p: dict, bag: set):
    for key in ("affiliation", "affiliations"):
        aff = p.get(key)
        if isinstance(aff, list):
            for a in aff:
                if isinstance(a, dict):
                    name = a.get("fullName") or a.get("name") or a.get("organization") or a.get("value")
                    if isinstance(name, str) and name.strip():
                        bag.add(name.strip())
                elif isinstance(a, str) and a.strip():
                    bag.add(a.strip())
        elif isinstance(aff, dict):
            name = aff.get("fullName") or aff.get("name") or aff.get("organization") or aff.get("value")
            if isinstance(name, str) and name.strip():
                bag.add(name.strip())
        elif isinstance(aff, str) and aff.strip():
            bag.add(aff.strip())

def extract_affiliations(rec: dict) -> str | None:
    bag = set()
    md = rec.get("metadata") or {}
    for key in ("creators","contributors"):
        arr = md.get(key)
        if isinstance(arr, list):
            for p in arr:
                if isinstance(p, dict):
                    _collect_affils_from_person(p, bag)
    return "; ".join(sorted(bag)) if bag else None

def extract_community_slug(rec: dict) -> str | None:
    # běžné varianty v InvenioRDM
    default = safe_get(rec, ["parent","communities","default"])
    if isinstance(default, str) and default.strip():
        return default.strip()
    default2 = safe_get(rec, ["communities","default"])
    if isinstance(default2, str) and default2.strip():
        return default2.strip()
    # někdy je jen single komunita v poli ids
    ids = safe_get(rec, ["parent","communities","ids"]) or safe_get(rec, ["communities","ids"])
    if isinstance(ids, list) and ids:
        # první jako fallback
        first = ids[0]
        if isinstance(first, str) and first.strip():
            return first.strip()
        if isinstance(first, dict):
            # někdy objekty se slugem/id
            slug = first.get("slug") or first.get("id") or first.get("identifier")
            if isinstance(slug, str) and slug.strip():
                return slug.strip()
    return None

def extract_ui_url(detail_or_raw: dict, rid: str) -> str:
    # 1) preferuj HTML linky z detailu
    for k in ("self_html", "html", "landing_page", "record_html"):
        v = safe_get(detail_or_raw, ["links", k])
        if isinstance(v, str) and v.strip():
            return v.rstrip("/")  # odstraníme trailing slash

    # 2) poskládej z komunity + id
    comm = extract_community_slug(detail_or_raw) or "general"
    return f"{BASE_URL}/{comm}/datasets/{rid}".rstrip("/")

def detail_api_url(rec_raw: dict, rid: str) -> str:
    # API detail (JSON) – buď links.self, nebo /datasets/<id>/
    self_link = safe_get(rec_raw, ["links","self"])
    if isinstance(self_link, str) and self_link.strip():
        return self_link
    return f"{BASE_URL}/datasets/{rid}"

def fetch_detail_json(rec_raw: dict, rid: str) -> dict:
    url = detail_api_url(rec_raw, rid)
    try:
        r = SESSION.get(url, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

# ====== Hlavní běh ======
def main():
    # 1) TOP10 podle bytes_total
    df = pd.read_parquet(PARQUET)
    top10 = (df.dropna(subset=["bytes_total"])
               .sort_values("bytes_total", ascending=False)
               .head(10)
               .copy())

    # 2) Načti RAW hity (id -> rec)
    raw_by_id = {}
    with open(RAW_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            rid = rec.get("id") or rec.get("pid") or rec.get("record_id")
            if rid:
                raw_by_id[rid] = rec

    # 3) Pro každý záznam stáhni detail a vytěž DOI/rok/title/afiliace/URL
    rows = []
    details_dump = []
    for _, row in top10.iterrows():
        rid = row["id"]
        size_b = int(row["bytes_total"])
        raw = raw_by_id.get(rid, {})

        detail = fetch_detail_json(raw, rid)
        # Titulek, DOI, rok, afiliace s fallbacky
        title = extract_title(detail) or extract_title(raw) or (row.get("title") if pd.notna(row.get("title")) else None) or ""
        doi = extract_doi(detail) or extract_doi(raw) or ""
        pub_year = extract_publication_year(detail, flat_year=row.get("publication_year"))
        affils = extract_affiliations(detail) or extract_affiliations(raw) or ""

        url_html = extract_ui_url(detail if detail else raw, rid)

        rows.append({
            "id": rid,
            "title": title,
            "size_human": human_bytes(size_b),
            "bytes_total": size_b,
            "doi": doi,
            "publication_year": int(pub_year) if pub_year is not None else "",
            "affiliations": affils,
            "url": url_html
        })
        details_dump.append(detail if detail else raw)

    # 4) Ulož výstupy
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df_out = pd.DataFrame(rows)

    csv_path = OUT_DIR / "top10_datasets_enriched_v2.csv"
    md_path  = OUT_DIR / "top10_datasets_enriched_v2.md"
    json_path= OUT_DIR / "top10_detail_v2.json"

    df_out.to_csv(csv_path, index=False)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("| pořadí | id | title | size_human | bytes_total | doi | publication_year | affiliations | url |\n")
        f.write("|---:|---|---|---:|---:|---|---:|---|---|\n")
        for i, r in enumerate(rows, start=1):
            f.write(
                f"| {i} | {r['id']} | {r['title']} | {r['size_human']} | {r['bytes_total']:,} | "
                f"{r['doi']} | {r['publication_year']} | {r['affiliations']} | {r['url']} |\n"
            )

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(details_dump, f, ensure_ascii=False, indent=2)

    print("[✓] Zapsáno:")
    print(f" - {csv_path}")
    print(f" - {md_path}")
    print(f" - {json_path}")

if __name__ == "__main__":
    main()

