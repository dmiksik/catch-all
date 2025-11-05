import pandas as pd
import numpy as np


# python objem-zaznamu_en.py --parquet nrp_dump/records_flat.parquet --out-md size_stats.md


PARQUET = "nrp_dump/records_flat.parquet"

def fmt_bytes(n):
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "NA"
    units = ["B","KB","MB","GB","TB","PB"]
    i = 0
    n = float(n)
    while n >= 1024 and i < len(units)-1:
        n /= 1024.0
        i += 1
    return f"{n:,.2f} {units[i]}"

# Load data
df = pd.read_parquet(PARQUET)

# Use only records with computed total size
s = pd.to_numeric(df["bytes_total"], errors="coerce").dropna()

# Basic stats (sample / unbiased where applicable)
n        = s.size
total_b  = s.sum()
mean_b   = s.mean()
median_b = s.median()
var_sam  = s.var(ddof=1)  # variance (sample)
std_sam  = s.std(ddof=1)  # standard deviation (sample)

# Percentiles for distribution overview
percentiles = [0.10, 0.25, 0.50, 0.75, 0.90]
perc_values = {int(q*100): s.quantile(q) for q in percentiles}

# Print nice Markdown
print("## Record Size Statistics\\n")
print(f"- **Records with size:** {n:,}")
print(f"- **Total volume:** {fmt_bytes(total_b)} ({total_b:,.0f} B)")
print(f"- **Mean:** {fmt_bytes(mean_b)} ({mean_b:,.0f} B)")
print(f"- **Median:** {fmt_bytes(median_b)} ({median_b:,.0f} B)")
print(f"- **Variance (sample):** {var_sam:,.0f} B^2")
print(f"- **Standard deviation (sample):** {fmt_bytes(std_sam)}\\n")

print("### Selected percentiles")
print("| Percentile | Size | Bytes |")
print("|---:|---:|---:|")
for p in [10, 25, 50, 75, 90]:
    val = perc_values[p]
    print(f"| {p}% | {fmt_bytes(val)} | {val:,.0f} |")
