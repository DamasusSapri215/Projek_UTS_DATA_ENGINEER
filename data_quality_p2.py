
from pathlib import Path
import json

from pyspark.sql import SparkSession, functions as F

BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data" / "curated" / "ecommerce"
CLEAN_DIR = BASE_DIR / "data" / "clean" / "ecommerce"
QUARANTINE_DIR = BASE_DIR / "data" / "quarantine" / "ecommerce"
REPORT_DIR = BASE_DIR / "data" / "quality"

spark = (
    SparkSession.builder
    .appName("EcommerceDataQualityV2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

if not INPUT_DIR.exists():
    raise FileNotFoundError(f"Input tidak ditemukan: {INPUT_DIR}")

df = spark.read.parquet(str(INPUT_DIR))

required = ["order_id"]
missing = [c for c in required if c not in df.columns]

if missing:
    raise ValueError(f"Kolom wajib tidak ditemukan: {missing}")

# Susun aturan validasi hanya untuk kolom yang tersedia.
checks = []

checks.append(
    F.col("order_id").isNull()
    | (F.trim(F.col("order_id")) == "")
)

if "total_qty" in df.columns:
    checks.append(
        F.col("total_qty").isNull()
        | (F.col("total_qty") <= 0)
    )

if "total_returned_qty" in df.columns:
    checks.append(
        F.col("total_returned_qty").isNull()
        | (F.col("total_returned_qty") < 0)
    )

if "num_product_categories" in df.columns:
    checks.append(
        F.col("num_product_categories").isNull()
        | (F.col("num_product_categories") < 0)
    )

if "total_pembayaran" in df.columns:
    checks.append(
        F.col("total_pembayaran").isNull()
        | (F.col("total_pembayaran") < 0)
    )

# Tandai ID duplikat. Jangan memilih satu baris secara acak.
duplicate_ids = (
    df.filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .select("order_id")
    .withColumn("_duplicate_id", F.lit(True))
)

df = df.join(duplicate_ids, on="order_id", how="left")

reasons = []

for condition in checks:
    reasons.append(
        F.when(condition, F.lit("Invalid field"))
    )

reasons.append(
    F.when(
        F.col("_duplicate_id") == True,
        F.lit("Duplicate order_id")
    )
)

df = df.withColumn(
    "quality_issues",
    F.filter(
        F.array(*reasons),
        lambda x: x.isNotNull()
    )
)

df = df.withColumn(
    "quality_status",
    F.when(
        F.size("quality_issues") == 0,
        F.lit("PASS")
    ).otherwise(F.lit("QUARANTINE"))
).drop("_duplicate_id")

clean_df = df.filter(F.col("quality_status") == "PASS")
quarantine_df = df.filter(F.col("quality_status") == "QUARANTINE")

total = df.count()
clean_count = clean_df.count()
quarantine_count = quarantine_df.count()

# Simpan kedua kelompok secara terpisah.
(
    clean_df.drop("quality_issues", "quality_status")
    .write.mode("overwrite").parquet(str(CLEAN_DIR))
)

quarantine_df.write.mode("overwrite").parquet(
    str(QUARANTINE_DIR)
)

report = {
    "input_rows": total,
    "clean_rows": clean_count,
    "quarantine_rows": quarantine_count,
    "reconciliation_passed": (
        total == clean_count + quarantine_count
    ),
    "validation_rules": [
        "order_id wajib terisi",
        "order_id harus unik",
        "total_qty harus lebih dari nol jika tersedia",
        "total_returned_qty tidak boleh negatif jika tersedia",
        "num_product_categories tidak boleh negatif jika tersedia",
        "total_pembayaran tidak boleh negatif jika tersedia",
    ],
}

REPORT_DIR.mkdir(parents=True, exist_ok=True)

with open(
    REPORT_DIR / "quality_report_v2.json",
    "w",
    encoding="utf-8"
) as file:
    json.dump(report, file, indent=4, ensure_ascii=False)

print(json.dumps(report, indent=4, ensure_ascii=False))
print("Data bersih:", CLEAN_DIR)
print("Data karantina:", QUARANTINE_DIR)

spark.stop()
