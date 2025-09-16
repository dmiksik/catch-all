#!/usr/bin/env python3
import argparse, json, os, sys, time
from urllib.parse import urljoin
import requests

DEFAULT_URL = "https://data.narodni-repozitar.cz/datasets/all/"

def get_session(token: str | None):
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    if token:
        s.headers.update({"Authorization": f"Bearer {token}"})
    return s

def polite_get(s: requests.Session, url: str, params=None, retries=6):
    for i in range(retries):
        r = s.get(url, params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep((2 ** i) + 0.5)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r

def _extract_hits(payload: dict):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    if "hits" in payload:
        hits = payload["hits"]
        if isinstance(hits, dict) and "hits" in hits:
            return hits.get("hits") or []
        if isinstance(hits, list):
            return hits
    if "items" in payload and isinstance(payload["items"], list):
        return payload["items"]
    return []

def iter_datasets(session: requests.Session, start_url: str, page_size: int, max_records: int | None):
    url = start_url
    params = {}
    if "?" not in url and page_size:
        params["size"] = page_size
    seen = 0
    while True:
        r = polite_get(session, url, params=params if "?" not in url else None)
        data = r.json()
        hits = _extract_hits(data)
        for h in hits:
            yield h
        seen += len(hits)
        if max_records and seen >= max_records:
            return
        next_url = (data.get("links") or {}).get("next") if isinstance(data, dict) else None
        if not next_url or not hits:
            return
        url, params = next_url, None

def safe_get(d, path, default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

# ---------- výpočty velikostí ----------

def compute_files_inline_aggregates(obj: dict):
    files = obj.get("files") if isinstance(obj, dict) else None
    if isinstance(files, dict):
        count = files.get("count")
        size = files.get("size")
        if count is not None or size is not None:
            return count, size
        entries = files.get("entries")
        if isinstance(entries, list) and entries:
            c = 0
            total = 0
            for e in entries:
                c += 1
                total += int(e.get("size") or 0)
            return c, total
    # alternativní názvy
    count = obj.get("files_count")
    size = obj.get("bytes_total") or obj.get("files_size") or obj.get("size_bytes")
    if count is not None or size is not None:
        return count, size
    return None, None

def fetch_files_via_link(session: requests.Session, link_url: str):
    """
    Očekává JSON pole/objekt se seznamem souborů.
    Podporované tvary:
      - {"entries":[{"size":...}, ...]}
      - [{"size":...}, ...]
      - {"hits":{"hits":[{"size":...}]}}
    Vrací (count, total_bytes) nebo (None, None) pokud se nepodaří.
    """
    try:
        data = polite_get(session, link_url).json()
    except Exception:
        return None, None

    # 1) entries list
    entries = safe_get(data, ["entries"])
    if isinstance(entries, list) and entries:
        return len(entries), sum(int(x.get("size") or 0) for x in entries)

    # 2) plain list
    if isinstance(data, list) and data:
        return len(data), sum(int(x.get("size") or 0) for x in data if isinstance(x, dict))

    # 3) hits.hits
    hits = safe_get(data, ["hits", "hits"])
    if isinstance(hits, list) and hits:
        return len(hits), sum(int(x.get("size") or 0) for x in hits if isinstance(x, dict))

    # 4) někdy bývá "objects" nebo "files"
    objects = data.get("objects") if isinstance(data, dict) else None
    if isinstance(objects, list) and objects:
        return len(objects), sum(int(x.get("size") or 0) for x in objects if isinstance(x, dict))

    files = data.get("files") if isinstance(data, dict) else None
    if isinstance(files, list) and files:
        return len(files), sum(int(x.get("size") or 0) for x in files if isinstance(x, dict))

    return None, None

def fetch_detail_if_needed(session: requests.Session, hit: dict, base_for_detail: str | None):
    """
    Nejprve zkusí `links.files`. Pokud není k dispozici nebo vrací nic použitelného,
    teprve pak sáhne pro detail přes `links.self` nebo /datasets/<id>/.
    Vrací tuple: (files_count, bytes_total, detail_obj_or_None)
    """
    # 1) Inline agregáty?
    fc, bt = compute_files_inline_aggregates(hit)
    if fc is not None or bt is not None:
        return fc, bt, None

    # 2) /files link?
    files_link = safe_get(hit, ["links", "files"]) or safe_get(hit, ["links", "bucket"])  # někdy bývá bucket
    if files_link:
        fc2, bt2 = fetch_files_via_link(session, files_link)
        if fc2 is not None or bt2 is not None:
            return fc2, bt2, None

    # 3) detail přes self
    self_link = safe_get(hit, ["links", "self"])
    if self_link:
        try:
            detail = polite_get(session, self_link).json()
            # zkus inline/entries i u detailu
            fc3, bt3 = compute_files_inline_aggregates(detail)
            if (fc3 is not None or bt3 is not None):
                return fc3, bt3, detail
            # a ještě jednou /files z detailu
            files_link2 = safe_get(detail, ["links", "files"]) or safe_get(detail, ["links", "bucket"])
            if files_link2:
                fc4, bt4 = fetch_files_via_link(session, files_link2)
                if fc4 is not None or bt4 is not None:
                    return fc4, bt4, detail
            return None, None, detail
        except Exception:
            pass

    # 4) fallback: /datasets/<id>/
    rid = hit.get("id") or hit.get("pid") or hit.get("record_id")
    if base_for_detail and rid:
        url = urljoin(base_for_detail, f"{rid}/")
        try:
            detail = polite_get(session, url).json()
            fc5, bt5 = compute_files_inline_aggregates(detail)
            if (fc5 is not None or bt5 is not None):
                return fc5, bt5, detail
            files_link3 = safe_get(detail, ["links", "files"]) or safe_get(detail, ["links", "bucket"])
            if files_link3:
                fc6, bt6 = fetch_files_via_link(session, files_link3)
                if fc6 is not None or bt6 is not None:
                    return fc6, bt6, detail
            return None, None, detail
        except Exception:
            pass

    return None, None, None

# ---------- extrakce řádku ----------

def extract_row(hit: dict, fc: int | None, bt: int | None, detail: dict | None):
    rec = detail or hit
    rid = rec.get("id") or rec.get("pid") or rec.get("record_id")
    created = rec.get("created") or safe_get(rec, ["metadata", "created"])
    updated = rec.get("updated") or safe_get(rec, ["metadata", "updated"])
    title = (safe_get(rec, ["metadata", "title"]) or
             rec.get("title") or
             safe_get(rec, ["metadata", "titles", 0, "title"]) or
             rec.get("titles"))
    pub_date = (safe_get(rec, ["metadata", "publication_date"]) or
                safe_get(rec, ["metadata", "dates", 0, "date"]) or
                rec.get("publication_date"))
    access_status = safe_get(rec, ["access", "record"]) or rec.get("access_status")

    return {
        "id": rid,
        "created": created,
        "updated": updated,
        "title": title,
        "publication_date": pub_date,
        "access_status": access_status,
        "files_count": fc,
        "bytes_total": bt,
    }

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Harvest NRP /datasets/all/ with pagination and files sizes via links.files.")
    ap.add_argument("--url", default=DEFAULT_URL, help="Start URL (default: %(default)s)")
    ap.add_argument("--out", required=True, help="Output folder")
    ap.add_argument("--page-size", type=int, default=100, help="Requested page size if not present in URL")
    ap.add_argument("--max-records", type=int, default=None, help="Limit for testing")
    ap.add_argument("--token", default=os.getenv("NRP_TOKEN"), help="Bearer token (optional)")
    ap.add_argument("--no-duckdb", action="store_true", help="Skip DuckDB creation")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    raw_path = os.path.join(args.out, "records.jsonl")
    flat_parquet = os.path.join(args.out, "records_flat.parquet")
    duckdb_path = os.path.join(args.out, "nrp.duckdb")

    s = get_session(args.token)
    print(f"[i] Start URL: {args.url}", file=sys.stderr)

    base_for_detail = None
    if "/datasets/all" in args.url:
        base_for_detail = args.url.split("/datasets/all")[0] + "/datasets/"

    # Harvest RAW
    n = 0
    with open(raw_path, "w", encoding="utf-8") as f:
        for hit in iter_datasets(s, args.url, page_size=args.page_size, max_records=args.max_records):
            f.write(json.dumps(hit, ensure_ascii=False) + "\n")
            n += 1
            if n % 1000 == 0:
                print(f"[i] harvested: {n}", file=sys.stderr)
    print(f"[✓] Harvested {n} hits → {raw_path}", file=sys.stderr)

    # Flatten + dopočet velikostí
    import pandas as pd
    rows = []
    got_sizes = 0
    with open(raw_path, "r", encoding="utf-8") as f:
        for line in f:
            hit = json.loads(line)
            fc, bt, detail = fetch_detail_if_needed(s, hit, base_for_detail)
            if (fc is not None and fc != 0) or (bt is not None and bt != 0):
                got_sizes += 1
            rows.append(extract_row(hit, fc, bt, detail))

    df = pd.DataFrame(rows)
    if "publication_date" in df.columns:
        df["publication_year"] = pd.to_datetime(df["publication_date"], errors="coerce").dt.year
    if "bytes_total" in df.columns:
        df["bytes_total"] = pd.to_numeric(df["bytes_total"], errors="coerce")
    if "files_count" in df.columns:
        df["files_count"] = pd.to_numeric(df["files_count"], errors="coerce")

    df.to_parquet(flat_parquet, index=False)
    print(f"[✓] Flattened view → {flat_parquet}", file=sys.stderr)

    if not args.no_duckdb:
        import duckdb
        con = duckdb.connect(duckdb_path)
        con.execute("INSTALL parquet; LOAD parquet;")
        con.execute("CREATE OR REPLACE TABLE records_flat AS SELECT * FROM parquet_scan(?)", [flat_parquet])
        con.close()
        print(f"[✓] DuckDB database → {duckdb_path}", file=sys.stderr)

    try:
        total_bytes = int(df["bytes_total"].fillna(0).sum())
        total_files = int(df["files_count"].fillna(0).sum())
        print(f"[i] Records with computed sizes: {got_sizes}/{len(df)}", file=sys.stderr)
        print(f"[i] Total bytes (sum over records): {total_bytes:,}", file=sys.stderr)
        print(f"[i] Total files (sum over records): {total_files:,}", file=sys.stderr)
    except Exception:
        pass

if __name__ == "__main__":
    main()

