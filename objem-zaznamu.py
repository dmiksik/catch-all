import pandas as pd
import numpy as np

PARQUET = "nrp_dump/records_flat.parquet"

def fmt_bytes(n):
    if n is None or np.isnan(n):
        return "NA"
    units = ["B","KB","MB","GB","TB","PB"]
    i = 0
    n = float(n)
    while n >= 1024 and i < len(units)-1:
        n /= 1024.0
        i += 1
    return f"{n:,.2f} {units[i]}"

# Načti data
df = pd.read_parquet(PARQUET)

# Vyber jen záznamy s dopočtenou velikostí
s = pd.to_numeric(df["bytes_total"], errors="coerce").dropna()

# Základní statistiky
mean_b   = s.mean()                    # průměr
median_b = s.median()                  # medián
var_pop  = s.var(ddof=0)               # rozptyl (populační)
var_sam  = s.var(ddof=1)               # rozptyl (výběrový)
std_pop  = s.std(ddof=0)               # směrodatná odchylka (populační)
std_sam  = s.std(ddof=1)               # směrodatná odchylka (výběrová)
n        = s.size

print(f"Počet záznamů s velikostí: {n}")
print(f"Průměr:                  {fmt_bytes(mean_b)}  ({mean_b:,.0f} B)")
print(f"Medián:                  {fmt_bytes(median_b)} ({median_b:,.0f} B)")
print(f"Rozptyl (populační):     {var_pop:,.0f} B^2")
print(f"Rozptyl (výběrový):      {var_sam:,.0f} B^2")
print(f"Směr. odchylka (pop.):   {fmt_bytes(std_pop)}")
print(f"Směr. odchylka (výb.):   {fmt_bytes(std_sam)}")

# Volitelně pár percentilů pro lepší představu o rozložení
for q in [0.1, 0.25, 0.5, 0.75, 0.9]:
    val = s.quantile(q)
    print(f"Percentil {int(q*100):>2d}%:         {fmt_bytes(val)} ({val:,.0f} B)")

