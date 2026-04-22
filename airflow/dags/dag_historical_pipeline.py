"""
airflow/dags/dag_historical_pipeline.py

Orchestrates the HISTORICAL medallion pipeline end-to-end:
  Ingest Weather → Ingest Retail → Bronze → Silver → Gold → Dims → Fact

Schedule: runs once on trigger (schedule_interval=None).
To re-run, trigger manually from the Airflow UI or CLI.

Usage:
    # One-time historical run
    airflow dags trigger dag_historical_pipeline

    # OR set schedule_interval="@monthly" to run monthly
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Make project root importable ────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # dags/ → airflow/ → project root
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

# ── Default DAG arguments ────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "capstone",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS  –  each wraps one pipeline stage
# ══════════════════════════════════════════════════════════════════════════════

def task_ingest_weather(**ctx):
    """Pull historical weather data from Open-Meteo API → raw layer."""
    from ingestion.api_extractor import APIExtractor
    extractor = APIExtractor(config_path=str(PROJECT_ROOT / "configs/api_config.yaml"))
    written = extractor.extract(entity="weather")
    logger.info(f"[ingest_weather] Written {len(written)} file(s)")
    return [str(p) for p in written]


def task_ingest_retail(**ctx):
    """Ingest UCI Online Retail CSV from manual-uploads → raw layer."""
    from ingestion.file_extractor import FileExtractor
    extractor = FileExtractor(config_path=str(PROJECT_ROOT / "configs/api_config.yaml"))
    written = extractor.extract(
        entity="retail_sales",
        source_name="uci_online_retail",
        output_format="parquet",
    )
    logger.info(f"[ingest_retail] Written {len(written)} file(s)")
    return [str(p) for p in written]


def task_weather_bronze(**ctx):
    """Raw JSON → typed Parquet (bronze)."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.bronze.weather_bronze import WeatherBronzeTransformer
    path = WeatherBronzeTransformer().extract()
    logger.info(f"[weather_bronze] → {path}")


def task_sales_bronze(**ctx):
    """Raw Parquet → cleaned city-partitioned Parquet (bronze)."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.bronze.sales_bronze import SalesBronzeTransformer
    path = SalesBronzeTransformer().extract()
    logger.info(f"[sales_bronze] → {path}")


def task_weather_silver(**ctx):
    """Bronze Parquet → cleaned Delta (silver)."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.silver.weather_silver import WeatherSilverTransformer
    path = WeatherSilverTransformer().extract()
    logger.info(f"[weather_silver] → {path}")


def task_sales_silver(**ctx):
    """Bronze Parquet → aggregated daily Delta (silver)."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.silver.sales_silver import SalesSilverTransformer
    path = SalesSilverTransformer().extract()
    logger.info(f"[sales_silver] → {path}")


def task_gold(**ctx):
    """Silver → joined sales+weather Gold Delta table."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.gold.sales_weather_gold import SalesWeatherGold
    path = SalesWeatherGold().extract()
    logger.info(f"[gold] → {path}")


def task_dim_date(**ctx):
    """Build dim_date dimension table."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.gold.dim_date import DimDate
    path = DimDate().extract()
    logger.info(f"[dim_date] → {path}")


def task_dim_location(**ctx):
    """Build dim_location dimension table."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.gold.dim_location import DimLocation
    path = DimLocation().extract()
    logger.info(f"[dim_location] → {path}")


def task_fact(**ctx):
    """Build fact_sales_weather star-schema fact table."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.gold.fact_sales_weather import FactSalesWeather
    path = FactSalesWeather().extract()
    logger.info(f"[fact] → {path}")

def task_dim_product(**ctx):
    """Build fact_sales_weather star-schema fact table."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.gold.dim_product import dim_product
    path = dim_product().extract()
    logger.info(f"[fact] → {path}")

def task_dim_sales_weather(**ctx):
    """Build fact_sales_weather star-schema fact table."""
    import os; os.chdir(PROJECT_ROOT)
    from processing.gold.sales_weather_gold import SalesWeatherGold
    path = SalesWeatherGold().extract()
    logger.info(f"[fact] → {path}")
# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="dag_historical_pipeline",
    default_args=DEFAULT_ARGS,
    description="Global Retail & Weather – full historical medallion pipeline",
    schedule_interval=None,          # trigger manually; set "@monthly" to automate
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "historical", "etl"],
) as dag:

    ingest_weather   = PythonOperator(task_id="ingest_weather",   python_callable=task_ingest_weather)
    ingest_retail    = PythonOperator(task_id="ingest_retail",    python_callable=task_ingest_retail)
    weather_bronze   = PythonOperator(task_id="weather_bronze",   python_callable=task_weather_bronze)
    sales_bronze     = PythonOperator(task_id="sales_bronze",     python_callable=task_sales_bronze)
    weather_silver   = PythonOperator(task_id="weather_silver",   python_callable=task_weather_silver)
    sales_silver     = PythonOperator(task_id="sales_silver",     python_callable=task_sales_silver)
    gold             = PythonOperator(task_id="gold",             python_callable=task_gold)
    dim_date         = PythonOperator(task_id="dim_date",         python_callable=task_dim_date)
    dim_location     = PythonOperator(task_id="dim_location",     python_callable=task_dim_location)
    dim_product      = PythonOperator(task_id="dim_product",      python_callable=task_dim_product)
    sales_weather    = PythonOperator(task_id="sales_weather",    python_callable=task_dim_sales_weather)
    fact             = PythonOperator(task_id="fact",             python_callable=task_fact)

    # ── Task dependency graph ──────────────────────────────────────────
    #
    #   ingest_weather ──► weather_bronze ──► weather_silver ──┐
    #                                                          ├──► gold ──► dim_date ──► fact
    #   ingest_retail  ──► sales_bronze   ──► sales_silver   ──┘         ──► dim_location ──► fact
    #

    ingest_weather >> weather_bronze >> weather_silver
    ingest_retail  >> sales_bronze   >> sales_silver
    [weather_silver, sales_silver]   >> gold
    gold >> [dim_date, dim_location,dim_product,sales_weather] >> fact
