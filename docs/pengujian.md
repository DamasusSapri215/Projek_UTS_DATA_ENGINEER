# Pengujian Pipeline Data Engineering

## 1. Tujuan Pengujian

Memastikan setiap tahapan pipeline berjalan sesuai rancangan.

## 2. Hasil Pengujian

| ID | Komponen | Hasil | Status |
|---|---|---|---|
| TC-01 | Dataset sumber | 100.000 baris | Lulus |
| TC-02 | Transformasi | Curated Parquet terbentuk | Lulus |
| TC-03 | Data quality | 100.000 order_id unik | Lulus |
| TC-04 | Konsistensi kategori | 0 masalah terdeteksi | Lulus |
| TC-05 | Validasi data | 100.000 clean, 0 quarantine | Lulus |
| TC-06 | Data mart | 100.000 transaksi | Lulus |
| TC-07 | Apriori | 18 frequent itemsets, 0 rules | Perlu evaluasi |
| TC-08 | FP-Growth | 18 frequent itemsets, 0 rules | Perlu evaluasi |
| TC-09 | Pemeriksaan basket | 109.788 baris item | Lulus |
| TC-10 | Airflow | Semua task selesai | Lulus |

