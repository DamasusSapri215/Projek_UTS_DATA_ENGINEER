
from pathlib import Path
import json
import time

import pandas as pd
from mlxtend.frequent_patterns import apriori, fpgrowth, association_rules

BASE_DIR = Path(__file__).resolve().parent
ITEMS_DIR = BASE_DIR / "data" / "mart" / "mart_basket_items"
OUTPUT_DIR = BASE_DIR / "data" / "analysis"

MIN_SUPPORT = 0.01
MIN_CONFIDENCE = 0.30

# Baca hasil data mart Parquet.
if not ITEMS_DIR.exists():
    raise FileNotFoundError(
        f"Data mart tidak ditemukan: {ITEMS_DIR}"
    )

items = pd.read_parquet(ITEMS_DIR)

required = {"order_id", "item"}
if not required.issubset(items.columns):
    raise ValueError(
        f"Kolom wajib: {required}. "
        f"Kolom tersedia: {set(items.columns)}"
    )

items = items.dropna(subset=["order_id", "item"])
items["order_id"] = items["order_id"].astype(str)
items["item"] = items["item"].astype(str).str.strip()
items = items[items["item"] != ""]
items = items.drop_duplicates(["order_id", "item"])

if items.empty:
    raise ValueError("Tidak ada item yang bisa dianalisis.")

# Satu baris = satu transaksi; satu kolom = satu item.
basket = (
    items.assign(present=1)
    .pivot_table(
        index="order_id",
        columns="item",
        values="present",
        aggfunc="max",
        fill_value=0
    )
    .astype(bool)
)

print("Jumlah transaksi:", len(basket))
print("Jumlah item unik:", len(basket.columns))

if len(basket.columns) < 2:
    raise ValueError(
        "Minimal diperlukan dua item berbeda "
        "untuk mencari kombinasi item."
    )

# Apriori
start = time.perf_counter()
frequent_apriori = apriori(
    basket,
    min_support=MIN_SUPPORT,
    use_colnames=True
)
apriori_seconds = time.perf_counter() - start

# FP-Growth
start = time.perf_counter()
frequent_fpgrowth = fpgrowth(
    basket,
    min_support=MIN_SUPPORT,
    use_colnames=True
)
fpgrowth_seconds = time.perf_counter() - start

# Aturan asosiasi.
def create_rules(frequent_itemsets):
    if frequent_itemsets.empty:
        return pd.DataFrame()

    return association_rules(
        frequent_itemsets,
        metric="confidence",
        min_threshold=MIN_CONFIDENCE
    )

rules_apriori = create_rules(frequent_apriori)
rules_fpgrowth = create_rules(frequent_fpgrowth)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

frequent_apriori.to_csv(
    OUTPUT_DIR / "frequent_itemsets_apriori.csv",
    index=False
)
frequent_fpgrowth.to_csv(
    OUTPUT_DIR / "frequent_itemsets_fpgrowth.csv",
    index=False
)
rules_apriori.to_csv(
    OUTPUT_DIR / "rules_apriori.csv",
    index=False
)
rules_fpgrowth.to_csv(
    OUTPUT_DIR / "rules_fpgrowth.csv",
    index=False
)

summary = {
    "transactions": int(len(basket)),
    "unique_items": int(len(basket.columns)),
    "min_support": MIN_SUPPORT,
    "min_confidence": MIN_CONFIDENCE,
    "apriori_frequent_itemsets": int(len(frequent_apriori)),
    "fpgrowth_frequent_itemsets": int(len(frequent_fpgrowth)),
    "apriori_rules": int(len(rules_apriori)),
    "fpgrowth_rules": int(len(rules_fpgrowth)),
    "apriori_runtime_seconds": round(apriori_seconds, 4),
    "fpgrowth_runtime_seconds": round(fpgrowth_seconds, 4),
}

with open(
    OUTPUT_DIR / "analysis_summary.json",
    "w",
    encoding="utf-8"
) as file:
    json.dump(summary, file, indent=4, ensure_ascii=False)

print("\n=== HASIL ANALISIS ===")
print(json.dumps(summary, indent=4, ensure_ascii=False))

print("\nContoh aturan Apriori:")
if not rules_apriori.empty:
    print(
        rules_apriori[
            ["antecedents", "consequents",
             "support", "confidence", "lift"]
        ].head(10).to_string(index=False)
    )
else:
    print("Belum ada aturan yang memenuhi ambang.")

print("\nContoh aturan FP-Growth:")
if not rules_fpgrowth.empty:
    print(
        rules_fpgrowth[
            ["antecedents", "consequents",
             "support", "confidence", "lift"]
        ].head(10).to_string(index=False)
    )
else:
    print("Belum ada aturan yang memenuhi ambang.")

print("\nHasil disimpan di:", OUTPUT_DIR)
