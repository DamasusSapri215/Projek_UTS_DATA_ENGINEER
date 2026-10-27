# Evaluasi Pipeline Data Engineering

## 1. Hasil Implementasi

Pipeline berhasil memproses 100.000 transaksi sintetis
dan menghasilkan data dalam format Parquet.

## 2. Hasil Market Basket Analysis

- Frequent itemsets Apriori: 18
- Frequent itemsets FP-Growth: 18
- Association rules Apriori: 0
- Association rules FP-Growth: 0
- Minimum support: 0,01
- Minimum confidence: 0,30

## 3. Evaluasi

Kedua algoritma berhasil menghasilkan frequent itemsets,
tetapi belum menghasilkan association rules pada parameter
yang digunakan. Hal ini perlu ditelusuri melalui pemeriksaan
pembentukan basket, distribusi item, dan parameter analisis.

Sebanyak 93,34% transaksi hanya memiliki satu item.
Kondisi ini menjadi salah satu aspek yang perlu diperhatikan
dalam evaluasi pola pembelian.

## 4. Keterbatasan

Dataset transaksi bersifat sintetis dan merepresentasikan
simulasi bisnis, bukan data transaksi aktual Shopee.

## 5. Rencana Perbaikan

- Memeriksa proses pembentukan basket.
- Menguji beberapa nilai minimum support dan confidence.
- Mengevaluasi kualitas serta variasi transaksi.
- Mengukur waktu eksekusi pipeline secara menyeluruh.
