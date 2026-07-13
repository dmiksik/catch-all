import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter  # added
from pathlib import Path

PARQUET = "nrp_dump/records_flat.parquet"
OUT_DIR = Path("nrp_dump")

# ====== Barvy EOSC ======
EOSC_GREEN = "#008691"   # Lively Green  RGB 0/134/145
EOSC_PINK  = "#FF5C80"   # Mild Pink     RGB 255/92/128
EOSC_GREY  = "#E4E3E3"   # Light Grey    RGB 228/227/227
EOSC_WHITE = "#FFFFFF"   # White         RGB 255/255/255
INK        = "#1f2937"   # čitelný text/osy na bílém podkladu

plt.rcParams.update({
    "figure.facecolor": EOSC_WHITE,
    "axes.facecolor":   EOSC_WHITE,
    "axes.edgecolor":   INK,
    "axes.labelcolor":  INK,
    "text.color":       INK,
    "xtick.color":      INK,
    "ytick.color":      INK,
    "font.size":        11,
})

def _save(fig, path):
    fig.savefig(path, dpi=150, facecolor=EOSC_WHITE)
    plt.close(fig)
    print(f"[✓] Saved: {path}")

# Načtení a příprava dat
df = pd.read_parquet(PARQUET)
sizes_b = pd.to_numeric(df["bytes_total"], errors="coerce").dropna()
sizes_b = sizes_b[sizes_b > 0]  # jen kladné
sizes_gb = sizes_b / (1024**3)

OUT_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# 1) Histogram (logaritmická osa X)
# -------------------------
fig = plt.figure(figsize=(9, 5.5))

# Log-binning pro přehlednost: 50 logaritmicky rovnoměrných košů
xmin = sizes_gb.min()
xmax = sizes_gb.max()
# ošetření, kdyby byly všechny stejné (vzácné)
if xmin == xmax:
    bins = 10
else:
    bins = np.geomspace(xmin, xmax, 50)

plt.hist(sizes_gb, bins=bins, color=EOSC_GREEN, edgecolor=EOSC_WHITE, linewidth=0.5)

# POŽADAVEK: hlavní značky 0.1, 1, 10, 100, ...
ax = plt.gca()
ax.set_xscale("log")
lo = int(np.floor(np.log10(xmin)))
hi = int(np.ceil(np.log10(xmax)))
major_ticks = [10 ** k for k in range(lo, hi + 1)]
ax.set_xticks(major_ticks)
ax.xaxis.set_major_formatter(StrMethodFormatter("{x:g}"))
ax.grid(True, which="major", axis="y", color=EOSC_GREY, linestyle="--", alpha=0.9)
ax.set_axisbelow(True)
for spine in ("top", "right"):
    ax.spines[spine].set_visible(False)

plt.xlabel("Dataset size [GB] (log scale)")
plt.ylabel("Number of datasets")
plt.title("Distribution of dataset sizes (catch-all)")
plt.tight_layout()
_save(fig, OUT_DIR / "size_histogram.png")

# -------------------------
# 2) Kumulativní křivka (CDF)
# -------------------------
fig = plt.figure(figsize=(9, 5.5))

sizes_sorted = np.sort(sizes_gb.values)
cdf = np.arange(1, len(sizes_sorted) + 1) / len(sizes_sorted)

plt.plot(sizes_sorted, cdf, color=EOSC_GREEN, linewidth=2)

# POŽADAVEK: hlavní značky 0.1, 1, 10, 100, ... (stejně jako u histogramu)
ax = plt.gca()
ax.set_xscale("log")
lo2 = int(np.floor(np.log10(xmin)))
hi2 = int(np.ceil(np.log10(xmax)))
major_ticks2 = [10 ** k for k in range(lo2, hi2 + 1)]
ax.set_xticks(major_ticks2)
ax.xaxis.set_major_formatter(StrMethodFormatter("{x:g}"))
# medián jako pink referenční linka
median_gb = float(np.median(sizes_gb.values))
ax.axvline(median_gb, color=EOSC_PINK, linewidth=2, linestyle="--")
ax.text(median_gb, 0.03, "  median", color=EOSC_PINK, fontsize=9, ha="left", va="bottom")

for spine in ("top", "right"):
    ax.spines[spine].set_visible(False)

plt.xlabel("Dataset size [GB] (log scale)")
plt.ylabel("Cumulative fraction of records")
plt.title("Cumulative distribution of sizes (catch-all)")
plt.grid(True, which="both", axis="both", color=EOSC_GREY, linestyle="--", alpha=0.9)
ax.set_axisbelow(True)
plt.tight_layout()
_save(fig, OUT_DIR / "cumulative_distribution.png")

# -------------------------
# 3) Počet záznamů podle čtvrtletí publikování
# -------------------------
pub = pd.to_datetime(df["publication_date"], errors="coerce").dropna()
if not pub.empty:
    q = pub.dt.to_period("Q")
    counts = q.value_counts().sort_index()
    # doplň chybějící čtvrtletí nulou (souvislá osa min..max)
    full_idx = pd.period_range(counts.index.min(), counts.index.max(), freq="Q")
    counts = counts.reindex(full_idx, fill_value=0)

    labels = [f"{p.year} Q{p.quarter}" for p in counts.index]
    x = np.arange(len(counts))

    fig = plt.figure(figsize=(max(9, len(counts) * 0.5), 5.5))
    ax = plt.gca()
    ax.bar(x, counts.values, color=EOSC_GREEN, edgecolor=EOSC_WHITE, linewidth=0.8, width=0.8)

    # přímé popisky nad nenulovými sloupci
    for xi, v in zip(x, counts.values):
        if v > 0:
            ax.text(xi, v, str(int(v)), ha="center", va="bottom", fontsize=8, color=INK)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=9)
    ax.grid(True, which="major", axis="y", color=EOSC_GREY, linestyle="--", alpha=0.9)
    ax.set_axisbelow(True)
    ax.margins(y=0.12)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)

    plt.ylabel("Number of records")
    plt.title("Records by publication quarter (catch-all)")
    plt.tight_layout()
    _save(fig, OUT_DIR / "records_by_quarter.png")
else:
    print("[!] Bez publication_date – čtvrtletní graf přeskočen")

# Volitelný textový souhrn do konzole
def fmt_bytes(n):
    units = ["B","KB","MB","GB","TB"]
    i = 0
    n = float(n)
    while n >= 1024 and i < len(units)-1:
        n /= 1024.0; i += 1
    return f"{n:,.2f} {units[i]}"

mean_b = sizes_b.mean()
median_b = sizes_b.median()
p90_b = np.quantile(sizes_b, 0.90)
p99_b = np.quantile(sizes_b, 0.99)

print(f"Number of datasets: {len(sizes_b)}")
print(f"Average size: {fmt_bytes(mean_b)}")
print(f"Median:           {fmt_bytes(median_b)}")
print(f"90th percentile:  {fmt_bytes(p90_b)}")
print(f"99th percentile:  {fmt_bytes(p99_b)}")
