
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "clean" / "ecommerce"
MART_DIR = BASE_DIR / "data" / "mart"

TRANSACTIONS_DIR = MART_DIR / "mart_transactions"
ITEMS_DIR = MART_DIR / "mart_basket_items"

spark = (
    SparkSession.builder
    .appName("BuildEcommerceDataMart")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

if not INPUT_DIR.exists():
    raise FileNotFoundError(
        f"Data bersih tidak ditemukan: {INPUT_DIR}. "
        "Pastikan tahap Data Quality sudah menghasilkan "
        "data/clean/ecommerce."
    )

df = spark.read.parquet(str(INPUT_DIR))

required = ["order_id", "product_categories"]
missing = [c for c in required if c not in df.columns]

if missing:
    raise ValueError(
        f"Kolom wajib tidak tersedia: {missing}. "
        f"Kolom yang tersedia: {df.columns}"
    )

transactions = df.select(
    "order_id",
    *[
        c for c in ["tanggal_pesanan", "status_pesanan"]
        if c in df.columns
    ]
)

items = (
    df.select(
        "order_id",
        F.explode(
            F.split(
                F.col("product_categories"),
                r"\s*[,;]\s*"
            )
        ).alias("item")
    )
    .withColumn("item", F.trim(F.col("item")))
    .filter(
        F.col("item").isNotNull()
        & (F.col("item") != "")
    )
    .dropDuplicates(["order_id", "item"])
)

# Validasi data mart.
transaction_count = transactions.select(
    "order_id"
).distinct().count()

item_count = items.count()

basket_sizes = (
    items.groupBy("order_id")
    .agg(F.countDistinct("item").alias("item_count"))
)

print("Jumlah transaksi unik:", transaction_count)
print("Jumlah baris item:", item_count)
print("Ringkasan ukuran basket:")
basket_sizes.select(
    F.min("item_count").alias("minimum_item"),
    F.avg("item_count").alias("rata_rata_item"),
    F.max("item_count").alias("maksimum_item")
).show()

print("Contoh data item:")
items.show(10, truncate=False)

# Parquet.
(
    transactions.write
    .mode("overwrite")
    .parquet(str(TRANSACTIONS_DIR))
)

(
    items.write
    .mode("overwrite")
    .parquet(str(ITEMS_DIR))
)

print("Data mart berhasil disimpan.")
print("Transaksi:", TRANSACTIONS_DIR)
print("Basket items:", ITEMS_DIR)

spark.stop()
