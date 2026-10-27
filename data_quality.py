
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "curated" / "ecommerce"
REPORT_DIR = BASE_DIR / "data" / "quality"

spark = (
    SparkSession.builder
    .appName("EcommerceDataQuality")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

if not INPUT_DIR.exists():
    raise FileNotFoundError(
        f"Folder Parquet tidak ditemukan: {INPUT_DIR}"
    )

df = spark.read.parquet(str(INPUT_DIR))

total_rows = df.count()

# Periksa apakah nama kolom yang diperlukan tersedia.
required_columns = ["order_id"]

missing_columns = [
    c for c in required_columns if c not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Kolom wajib tidak tersedia: {missing_columns}"
    )

# Pemeriksaan kualitas.
null_order_ids = df.filter(
    F.col("order_id").isNull()
).count()

duplicate_order_ids = (
    df.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Hitung nilai kosong untuk semua kolom.
null_counts = {}

for c in df.columns:
    null_counts[c] = df.filter(
        F.col(c).isNull()
    ).count()

# Periksa jumlah kategori bila kolom tersedia.
category_check = "Tidak diperiksa"

if {
    "product_categories",
    "num_product_categories"
}.issubset(df.columns):

    category_check = (
        df.filter(
            F.col("product_categories").isNotNull()
            & F.col("num_product_categories").isNotNull()
            & (
                F.size(
                    F.split(
                        F.col("product_categories"),
                        r"\s*[,;]\s*"
                    )
                ) != F.col("num_product_categories")
            )
        ).count()
    )

report = {
    "total_rows": total_rows,
    "unique_order_ids": df.select("order_id").distinct().count(),
    "null_order_ids": null_order_ids,
    "duplicate_order_ids": duplicate_order_ids,
    "null_counts_by_column": null_counts,
    "category_consistency_issues": category_check,
}

REPORT_DIR.mkdir(parents=True, exist_ok=True)

report_path = REPORT_DIR / "quality_report.json"

with open(report_path, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=4, ensure_ascii=False)

print("=== DATA QUALITY REPORT ===")
print(json.dumps(report, indent=4, ensure_ascii=False))
print(f"\nLaporan tersimpan di: {report_path}")

spark.stop()
