import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

PARQUET = "nrp_dump/records_flat.parquet"
OUT_DIR = Path("nrp_dump")

# Načtení a příprava dat
df = pd.read_parquet(PARQUET)
sizes_b = pd.to_numeric(df["bytes_total"], errors="coerce").dropna()
sizes_b = sizes_b[sizes_b > 0]  # jen kladné
sizes_gb = sizes_b / (1024**3)

OUT_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------
# 1) Histogram (logaritmická osa X)
# -------------------------
plt.figure(figsize=(9, 5.5))

# Log-binning pro přehlednost: 50 logaritmicky rovnoměrných košů
xmin = sizes_gb.min()
xmax = sizes_gb.max()
# ošetření, kdyby byly všechny stejné (vzácné)
if xmin == xmax:
    bins = 10
else:
    bins = np.geomspace(xmin, xmax, 50)

plt.hist(sizes_gb, bins=bins, edgecolor="black")
plt.xscale("log")
plt.xlabel("Velikost datasetu [GB] (log měřítko)")
plt.ylabel("Počet datasetů")
plt.title("Rozložení velikostí datasetů (NRP)")
plt.tight_layout()
hist_path = OUT_DIR / "histogram_velikosti.png"
plt.savefig(hist_path, dpi=150)
plt.close()

# -------------------------
# 2) Kumulativní křivka (CDF)
# -------------------------
plt.figure(figsize=(9, 5.5))

sizes_sorted = np.sort(sizes_gb.values)
cdf = np.arange(1, len(sizes_sorted) + 1) / len(sizes_sorted)

plt.plot(sizes_sorted, cdf)
plt.xscale("log")
plt.xlabel("Velikost datasetu [GB] (log měřítko)")
plt.ylabel("Kumulativní podíl záznamů")
plt.title("Kumulativní distribuce velikostí (NRP)")
plt.grid(True, which="both", axis="both", linestyle="--", alpha=0.4)
plt.tight_layout()
cdf_path = OUT_DIR / "kumulativni_krivka.png"
plt.savefig(cdf_path, dpi=150)
plt.close()

print(f"[✓] Uloženo: {hist_path}")
print(f"[✓] Uloženo: {cdf_path}")

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

print(f"Počet datasetů: {len(sizes_b)}")
print(f"Průměrná velikost: {fmt_bytes(mean_b)}")
print(f"Medián:           {fmt_bytes(median_b)}")
print(f"90. percentil:    {fmt_bytes(p90_b)}")
print(f"99. percentil:    {fmt_bytes(p99_b)}")

