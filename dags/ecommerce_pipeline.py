
from datetime import datetime
from pathlib import Path
import subprocess

from airflow.sdk import dag, task

PROJECT_DIR = Path.home() / "proj_de"
PROJECT_PYTHON = Path.home() / "venvs" / "proj_de" / "bin" / "python"


def run_script(script_name: str) -> None:
    script_path = PROJECT_DIR / script_name

    if not PROJECT_DIR.exists():
        raise FileNotFoundError(
            f"Folder proyek tidak ditemukan: {PROJECT_DIR}"
        )

    if not PROJECT_PYTHON.exists():
        raise FileNotFoundError(
            f"Python environment tidak ditemukan: {PROJECT_PYTHON}"
        )

    if not script_path.exists():
        raise FileNotFoundError(
            f"Skrip tidak ditemukan: {script_path}"
        )

    subprocess.run(
        [str(PROJECT_PYTHON), str(script_path)],
        cwd=str(PROJECT_DIR),
        check=True,
    )


@dag(
    dag_id="ecommerce_market_basket_pipeline",
    description=(
        "Pipeline end-to-end untuk transformasi, "
        "pemeriksaan kualitas, data mart, dan Market Basket Analysis"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "pyspark", "market-basket"],
)
def ecommerce_pipeline():

    @task
    def transform_data():
        run_script("data_transformation.py")

    @task
    def check_data_quality():
        run_script("data_quality.py")

    @task
    def clean_data():
        run_script("data_quality_p2.py")

    @task
    def build_data_mart():
        run_script("data_mart.py")

    @task
    def run_market_basket_analysis():
        run_script("market_basket_analyst.py")

    @task
    def verify_data_mart():
        run_script("check_basket_market.py")

    transformed = transform_data()
    quality_checked = check_data_quality()
    cleaned = clean_data()
    mart_built = build_data_mart()
    analysis_done = run_market_basket_analysis()
    mart_verified = verify_data_mart()

    transformed >> quality_checked >> cleaned
    cleaned >> mart_built >> analysis_done >> mart_verified


ecommerce_pipeline()
