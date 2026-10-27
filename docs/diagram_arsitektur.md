# Diagram Arsitektur Pipeline Data Engineering

```mermaid
flowchart TD
    A["Dataset CSV"] --> B["Apache Airflow"]
    B --> C["Data Transformation"]
    C --> D["Curated Data (Parquet)"]
    D --> E["Data Quality"]
    E --> F["Validasi Data"]
    F --> G["Data Mart"]
    G --> H["Apriori"]
    G --> I["FP-Growth"]
    H --> J["Hasil Analisis"]
    I --> J
