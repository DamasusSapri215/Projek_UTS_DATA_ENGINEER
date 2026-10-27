import pandas as pd
from pathlib import Path

path = Path("data/mart/mart_basket_items")
df = pd.read_parquet(path)

print("Total baris item:", len(df))
print("Total transaksi:", df["order_id"].nunique())

items_per_order = df.groupby("order_id")["item"].nunique()

print("\nJumlah item unik per transaksi:")
print(items_per_order.value_counts().sort_index())

print("\nPersentase transaksi dengan satu item:")
print(f"{(items_per_order == 1).mean() * 100:.2f}%")

print("\nPersentase transaksi dengan minimal dua item:")
print(f"{(items_per_order >= 2).mean() * 100:.2f}%")