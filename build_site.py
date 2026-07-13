#!/usr/bin/env python3
"""Sestaví statickou stránku (public/index.html) z vygenerovaných reportů.

Vstupy (co existuje, to se zobrazí):
  - nrp_by_community.md                       – tabulka komunit
  - nrp_dump/size_histogram.png               – graf velikostí
  - nrp_dump/cumulative_distribution.png      – kumulativní křivka
  - nrp_dump/records_by_quarter.png           – počty záznamů po čtvrtletích
  - nrp_dump/size_stats.md                    – souhrn statistik velikostí
  - nrp_dump/top10_datasets_enriched_v2.md    – TOP 10 datasetů

Grafy (PNG) se kopírují do public/ a odkazují se relativně.
"""
import datetime
import pathlib
import shutil

import markdown

# Barvy EOSC
EOSC_GREEN = "#008691"   # Lively Green
EOSC_PINK  = "#FF5C80"   # Mild Pink
EOSC_GREY  = "#E4E3E3"   # Light Grey

ROOT = pathlib.Path(__file__).resolve().parent
DUMP = ROOT / "nrp_dump"
OUT = ROOT / "public"

CHARTS = [
    ("size_histogram.png", "Rozdělení velikostí datasetů"),
    ("cumulative_distribution.png", "Kumulativní rozdělení velikostí"),
]
QUARTER_CHART = ("records_by_quarter.png", "Počet záznamů podle čtvrtletí publikování")


def md_to_html(path: pathlib.Path, demote: int = 0) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    html = markdown.markdown(text, extensions=["tables"])
    # posuň úrovně nadpisů níž, aby nekolidovaly s <h2> sekce
    for level in range(4, 0, -1):
        html = html.replace(f"<h{level}>", f"<h{min(level + demote, 6)}>")
        html = html.replace(f"</h{level}>", f"</h{min(level + demote, 6)}>")
    return html


def figure(name: str, caption: str) -> str:
    if not (DUMP / name).exists():
        return ""
    return (
        f'<figure class="chart">'
        f'<img src="{name}" alt="{caption}" loading="lazy">'
        f'<figcaption>{caption}</figcaption>'
        f"</figure>"
    )


def section(title: str, body: str) -> str:
    if not body.strip():
        return ""
    return f'<section><h2>{title}</h2>{body}</section>'


def main() -> None:
    OUT.mkdir(exist_ok=True)

    # zkopíruj dostupné grafy do public/
    for name, _ in CHARTS + [QUARTER_CHART]:
        src = DUMP / name
        if src.exists():
            shutil.copyfile(src, OUT / name)

    communities = md_to_html(ROOT / "nrp_by_community.md")
    size_stats = md_to_html(DUMP / "size_stats.md", demote=1)
    top10 = md_to_html(DUMP / "top10_datasets_enriched_v2.md")

    size_figs = "".join(figure(n, c) for n, c in CHARTS)
    quarter_fig = figure(*QUARTER_CHART)

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    parts = [
        section("Komunity a záznamy", communities),
        section("Velikosti datasetů",
                (f'<div class="charts">{size_figs}</div>' if size_figs else "") + size_stats),
        section("Záznamy podle čtvrtletí publikování", quarter_fig),
        section("TOP 10 největších datasetů",
                f'<div class="tablewrap">{top10}</div>' if top10 else ""),
    ]
    body = "\n".join(p for p in parts if p)

    html_out = f"""<!doctype html>
<html lang="cs">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Catch-all Repository report</title>
  <style>
    :root {{
      --green: {EOSC_GREEN}; --pink: {EOSC_PINK}; --grey: {EOSC_GREY};
      --bg: #ffffff; --fg: #1f2937; --muted: #6b7280;
      --border: var(--grey); --stripe: #f6fbfb; --card: #ffffff;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #0b0f14; --fg: #e5e7eb; --muted: #9ca3af;
        --border: #1f2937; --stripe: #0f141b; --card: #ffffff;
      }}
    }}
    html,body{{background:var(--bg);color:var(--fg);margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      line-height:1.55}}
    .wrap{{max-width:1100px;margin:0 auto;padding:0 1rem 3rem}}
    header{{border-top:6px solid var(--green);padding:2rem 0 1rem;margin-bottom:1rem}}
    h1{{margin:0 0 .35rem}}
    h2{{margin:0 0 .75rem;color:var(--green);
      border-bottom:2px solid var(--grey);padding-bottom:.35rem}}
    section{{margin:2.25rem 0}}
    .meta{{color:var(--muted);margin:.25rem 0 0}}
    a{{color:var(--green);text-decoration:none}} a:hover{{text-decoration:underline}}
    .tablewrap{{overflow-x:auto}}
    table{{width:100%;border-collapse:collapse;border:1px solid var(--border);
      border-radius:12px;overflow:hidden;font-size:.95rem}}
    thead th{{text-align:left;background:var(--stripe);color:var(--fg);
      border-bottom:2px solid var(--green);padding:.7rem .85rem;font-weight:600}}
    tbody td{{border-bottom:1px solid var(--border);padding:.6rem .85rem;vertical-align:top}}
    tbody tr:nth-child(even) td{{background:color-mix(in srgb, var(--stripe) 60%, transparent)}}
    td:last-child a{{word-break:break-word}}
    code{{font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace}}
    .charts{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;margin-bottom:1rem}}
    figure.chart{{margin:0;background:var(--card);border:1px solid var(--grey);
      border-radius:12px;padding:.75rem}}
    figure.chart img{{width:100%;height:auto;display:block}}
    figure.chart figcaption{{color:#374151;font-size:.85rem;margin-top:.4rem;text-align:center}}
    .footer{{margin-top:2.5rem;color:var(--muted);font-size:.9rem;
      border-top:1px solid var(--border);padding-top:1rem}}
  </style>
</head>
<body>
  <main class="wrap">
    <header>
      <h1>Catch-all Repository report</h1>
      <div class="meta">Publikováno: {now} · zdroj dat: <a href="https://datarepo.eosc.cz">datarepo.eosc.cz</a></div>
    </header>
    {body}
    <p class="footer">Generováno GitHub Actions z dat repozitáře <code>datarepo.eosc.cz</code>.</p>
  </main>
</body>
</html>"""

    (OUT / "index.html").write_text(html_out, encoding="utf-8")
    print(f"[✓] Zapsáno: {OUT / 'index.html'}")


if __name__ == "__main__":
    main()
