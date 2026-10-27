
from pathlib import Path
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
)

# ==================================================
# 1. KONFIGURASI
# ==================================================

BASE_DIR = Path(__file__).resolve().parent

INPUT_FILE = BASE_DIR / "dataset_ecommerce_sintetis.csv"
OUTPUT_DIR = BASE_DIR / "data" / "curated" / "ecommerce"

# ==================================================
# 2. INISIALISASI SPARK
# ==================================================

spark = (
    SparkSession.builder
    .appName("EcommerceDataTransformation")
    .config("spark.sql.session.timeZone", "Asia/Jakarta")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==================================================
# 3. DATA INGESTION
# ==================================================

if not INPUT_FILE.exists():
    raise FileNotFoundError(
        f"File input tidak ditemukan: {INPUT_FILE}"
    )

df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", False)
    .option("encoding", "UTF-8")
    .csv(str(INPUT_FILE))
)

print("Jumlah baris awal:", df.count())
print("Kolom awal:", df.columns)

# ==================================================
# 4. NORMALISASI NAMA KOLOM
# ==================================================

def normalize_column(name):
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

new_names = [normalize_column(c) for c in df.columns]

if len(new_names) != len(set(new_names)):
    raise ValueError(
        "Ada nama kolom yang sama setelah normalisasi. "
        "Periksa header CSV."
    )

for old_name, new_name in zip(df.columns, new_names):
    df = df.withColumnRenamed(old_name, new_name)

# ==================================================
# 5. PEMBERSIHAN NILAI STRING
# ==================================================

string_cols = [
    field.name
    for field in df.schema.fields
    if isinstance(field.dataType, StringType)
]

for col_name in string_cols:
    df = df.withColumn(
        col_name,
        F.when(
            F.trim(F.col(col_name)) == "",
            F.lit(None)
        ).otherwise(F.trim(F.col(col_name)))
    )

# ==================================================
# 6. VALIDASI ID PESANAN
# ==================================================

if "order_id" not in df.columns:
    raise ValueError(
        "Kolom order_id tidak ditemukan setelah normalisasi."
    )

df = df.withColumn(
    "order_id",
    F.upper(F.trim(F.col("order_id")))
)

null_id_count = df.filter(
    F.col("order_id").isNull()
).count()

if null_id_count > 0:
    raise ValueError(
        f"Ditemukan {null_id_count} baris tanpa order_id."
    )

duplicate_count = (
    df.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Jumlah ID pesanan duplikat:", duplicate_count)

if duplicate_count > 0:
    raise ValueError(
        "Terdapat order_id duplikat. "
        "Periksa data sebelum melanjutkan."
    )

# ==================================================
# 7. KONVERSI TIPE DATA NUMERIK
# ==================================================

numeric_cols = {
    "total_qty",
    "total_weight_gr",
    "total_returned_qty",
    "total_diskon",
    "num_product_categories",
    "ongkos_kirim_dibayar_oleh_pembeli",
    "estimasi_potongan_biaya_pengiriman",
    "total_pembayaran",
    "perkiraan_ongkos_kirim",
}

for col_name in numeric_cols.intersection(df.columns):
    cleaned = F.regexp_replace(
        F.col(col_name),
        r"(?<=\d)\.(?=\d{3}(\D|$))",
        ""
    )
    cleaned = F.regexp_replace(cleaned, ",", ".")
    
    df = df.withColumn(
        col_name,
        cleaned.cast(DoubleType())
    )
# ==================================================
# 8. KONVERSI KOLOM TANGGAL
# ==================================================

for col_name in df.columns:
    if (
        col_name.endswith("_date")
        or "tanggal" in col_name
        or col_name == "waktu_pesanan_dibuat"
    ):
        df = df.withColumn(
            col_name,
            F.coalesce(
                F.try_to_timestamp(
                    F.col(col_name),
                    F.lit("yyyy-MM-dd HH:mm")
                ),
                F.try_to_timestamp(
                    F.col(col_name),
                    F.lit("yyyy-MM-dd")
                ),
                F.try_to_timestamp(
                    F.col(col_name),
                    F.lit("dd-MM-yyyy HH:mm")
                ),
                F.try_to_timestamp(
                    F.col(col_name),
                    F.lit("dd-MM-yyyy")
                )
            )
        )

# ==================================================
# 9. NORMALISASI STATUS PESANAN
# ==================================================

if "status_pesanan" in df.columns:
    df = df.withColumn(
        "status_pesanan",
        F.initcap(F.lower(F.trim(F.col("status_pesanan"))))
    )

# ==================================================
# 10. PEMERIKSAAN DATA HASIL TRANSFORMASI
# ==================================================

print("\nJumlah baris hasil transformasi:", df.count())
print("Jumlah kolom:", len(df.columns))

print("\nSkema data:")
df.printSchema()

print("\nContoh data:")
df.show(5, truncate=False)

print("\nJumlah nilai kosong per kolom:")
null_exprs = [
    F.sum(
        F.when(F.col(c).isNull(), 1).otherwise(0)
    ).alias(c)
    for c in df.columns
]

df.select(*null_exprs).show(truncate=False)

# ==================================================
# 11. PENYIMPANAN DATA CURATED KE PARQUET
# ==================================================

OUTPUT_DIR.parent.mkdir(parents=True, exist_ok=True)

(
    df.write
    .mode("overwrite")
    .parquet(str(OUTPUT_DIR))
)

print("\nTransformasi selesai.")
print("Lokasi Parquet:", OUTPUT_DIR)

spark.stop()
