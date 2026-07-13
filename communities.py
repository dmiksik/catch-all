#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests, json, re, sys, datetime
from urllib.parse import urljoin, urlencode

BASE = "https://datarepo.eosc.cz"
COMMUNITIES_URL = f"{BASE}/api/communities"

S = requests.Session()
S.headers.update({
    "Accept": "application/json",
    "User-Agent": "nrp-community-scan/1.1"
})

def parse_dt(s):
    if not s: return None
    # pár běžných ISO tvarů
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        # poslední záchrana: zkusit fromisoformat bez TZ
        return datetime.datetime.fromisoformat(s.replace("Z","+00:00"))
    except Exception:
        return None

def safe_get_json(url):
    r = S.get(url, timeout=45)
    r.raise_for_status()
    try:
        return r.json()
    except ValueError:
        m = re.search(r'<script[^>]+type=["\']application/json["\'][^>]*>(.*?)</script>', r.text or "", re.S|re.I)
        if m:
            return json.loads(m.group(1))
        raise

def collect_community_ids():
    data = safe_get_json(COMMUNITIES_URL)
    ids, titles = [], {}
    def add(slug, title=""):
        if slug and slug not in ids:
            ids.append(slug)
            titles[slug] = title or titles.get(slug, "")
    if isinstance(data, list):
        for it in data:
            add(it.get("slug") or it.get("id") or it.get("identifier") or it.get("code") or it.get("name"),
                (it.get("metadata") or {}).get("title") or it.get("title") or it.get("name") or "")
    elif isinstance(data, dict):
        for key in ("communities","items","hits","results","data"):
            seq = data.get(key)
            if isinstance(seq, list):
                for it in seq:
                    add(it.get("slug") or it.get("id") or it.get("identifier") or it.get("code") or it.get("name"),
                        ((it.get("metadata") or {}).get("title") or it.get("title") or it.get("name") or ""))
        if isinstance(data.get("hits"), dict):
            for it in data["hits"].get("hits") or []:
                add(it.get("slug") or it.get("id") or it.get("identifier") or it.get("code") or it.get("name"),
                    ((it.get("metadata") or {}).get("title") or it.get("title") or it.get("name") or ""))
    return ids, titles

def normalize_hits(data):
    """Vrátí list záznamů + total, tolerantně ke struktuře."""
    hits, total = [], None
    if isinstance(data, dict):
        if isinstance(data.get("hits"), dict):
            hh = data["hits"].get("hits") or []
            hits = hh if isinstance(hh, list) else []
            t = data["hits"].get("total")
            total = (t.get("value") if isinstance(t, dict) else t)
        elif isinstance(data.get("items"), list):
            hits = data["items"]; total = len(hits)
        else:
            for k in ("results", "data", "records"):
                if isinstance(data.get(k), list):
                    hits = data[k]; total = len(hits); break
    return hits, total

def record_id(r):
    md = r.get("metadata", {}) if isinstance(r, dict) else {}
    return (r.get("id") if isinstance(r, dict) else None) or md.get("id")

def record_updated(r):
    md = r.get("metadata", {}) if isinstance(r, dict) else {}
    return (r.get("updated") if isinstance(r, dict) else None) or md.get("updated") or md.get("publication_date")

def record_link(r):
    # preferuj self_html, pak fallback /records/<id>
    links = r.get("links") if isinstance(r, dict) else None
    if isinstance(links, dict):
        href = links.get("self_html") or links.get("html") or links.get("self")
        if href: return href
    rid = record_id(r)
    if rid:
        return f"{BASE}/datasets/records/{rid}"
    return None

def _newest_links_from_url(url):
    """Z daného search URL vrátí (total, [markdown odkazy na 5 nejnovějších])."""
    data = safe_get_json(url)
    hits, total = normalize_hits(data)

    # server-side sort nemusí být spolehlivý → seřaď i klientsky
    def key_dt(r):
        return parse_dt(record_updated(r)) or datetime.datetime.min
    hits_sorted = sorted(hits, key=key_dt, reverse=True)[:5] if hits else []

    links = []
    for r in hits_sorted:
        rid = record_id(r)
        href = record_link(r)
        if rid and href:
            links.append(f"[{rid}]({href})")
        elif rid:
            links.append(rid)
    return total, links

def fetch_5_newest_links(cid):
    url = f"{BASE}/api/communities/{cid}/records?{urlencode({'sort':'newest'})}"
    return _newest_links_from_url(url)

def fetch_no_community_links():
    # záznamy, které nepatří do žádné komunity (dřív musel být každý záznam
    # v komunitě – základní byla „General“; nově komunita není povinná)
    q = "NOT _exists_:parent.communities.ids"
    url = f"{BASE}/api/datasets?{urlencode({'q': q, 'sort': 'newest'})}"
    return _newest_links_from_url(url)

def main():
    ids, titles = collect_community_ids()
    lines = []
    lines.append("| Community (ID) | Name | Records | Links (5 newest) |")
    lines.append("|---|---|---:|---|")
    grand_total = 0
    for cid in ids:
        total, links = fetch_5_newest_links(cid)
        try:
            grand_total += int(total)
        except (TypeError, ValueError):
            pass
        name = titles.get(cid, "")
        sample = "<br>".join(links) if links else "—"
        lines.append(f"| `{cid}` | {name} | {total if total is not None else '—'} | {sample} |")
    # záznamy mimo komunity – stejný výpočet jako pro komunity
    nc_total, nc_links = fetch_no_community_links()
    try:
        grand_total += int(nc_total)
    except (TypeError, ValueError):
        pass
    nc_sample = "<br>".join(nc_links) if nc_links else "—"
    lines.append(f"| `—` | No Community | {nc_total if nc_total is not None else '—'} | {nc_sample} |")
    # poslední řádek tabulky s celkovým počtem záznamů (tučně)
    lines.append(f"| **Total** | — | **{grand_total}** | — |\n  ")
    lines.append(f"_Source: {BASE}_\n")

    out = "nrp_by_community.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Hotovo: {out}")

if __name__ == "__main__":
    main()
