"""
airflow/dags/dag_live_pipeline.py

Orchestrates the LIVE (CDC-driven) medallion pipeline daily:
  Generate Weather → Generate Retail → CDC Detect → Bronze Live →
  Silver Live → Gold Live

Schedule: daily at 01:00 UTC  →  processes "tomorrow's" data each night.
          Change `schedule_interval` to any cron string as needed.

Usage:
    # Trigger one-time backfill (first-ever run)
    airflow dags trigger dag_live_pipeline --conf '{"stage": "backfill"}'

    # Let the scheduler run it daily (default behaviour after enabling)
    airflow dags unpause dag_live_pipeline
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# ── Make project root importable ────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "capstone",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# ══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def task_generate_weather(**ctx):
    """Generate tomorrow's live weather JSON files into raw layer."""
    import os; os.chdir(PROJECT_ROOT)
    from scripts.generate_live_weather import run_daily
    target_date_str = ctx["dag_run"].conf.get("target_date") if ctx["dag_run"].conf else None
    target = datetime.strptime(target_date_str, "%Y-%m-%d").date() if target_date_str else None
    run_daily(target)
    logger.info(f"[generate_weather] complete (target={target})")


def task_generate_retail(**ctx):
    """Generate tomorrow's live retail CSV files into manual-uploads."""
    import os; os.chdir(PROJECT_ROOT)
    from scripts.generate_live_retail import run_daily
    target_date_str = ctx["dag_run"].conf.get("target_date") if ctx["dag_run"].conf else None
    target = datetime.strptime(target_date_str, "%Y-%m-%d").date() if target_date_str else None
    run_daily(target)
    logger.info(f"[generate_retail] complete (target={target})")


def task_cdc_detect(**ctx):
    """
    Run CDC watermark check. Returns True (via XCom) if new data was found.
    Branch: if new data → proceed to pipeline; else → skip_pipeline.
    """
    import os; os.chdir(PROJECT_ROOT)
    from scripts.cdc_manager import run_cdc_weather, run_cdc_retail
    weather_new = run_cdc_weather(dry_run=False)
    retail_new  = run_cdc_retail(dry_run=False)
    has_new = weather_new or retail_new
    ctx["ti"].xcom_push(key="has_new_data", value=has_new)
    logger.info(f"[cdc_detect] has_new_data={has_new}")
    return "weather_bronze_live" if has_new else "skip_pipeline"


def task_weather_bronze_live(**ctx):
    import os; os.chdir(PROJECT_ROOT)
    from processing.bronze.weather_bronze_live import WeatherBronzeLiveTransformer
    WeatherBronzeLiveTransformer().extract()
    logger.info("[weather_bronze_live] complete")


def task_sales_bronze_live(**ctx):
    import os; os.chdir(PROJECT_ROOT)
    from processing.bronze.sales_bronze_live import SalesBronzeLiveTransformer
    SalesBronzeLiveTransformer().extract()
    logger.info("[sales_bronze_live] complete")


def task_weather_silver_live(**ctx):
    import os; os.chdir(PROJECT_ROOT)
    from processing.silver.weather_silver_live import WeatherSilverLiveTransformer
    WeatherSilverLiveTransformer().extract()
    logger.info("[weather_silver_live] complete")


def task_sales_silver_live(**ctx):
    import os; os.chdir(PROJECT_ROOT)
    from processing.silver.sales_silver_live import SalesSilverLiveTransformer
    SalesSilverLiveTransformer().extract()
    logger.info("[sales_silver_live] complete")


def task_gold_live(**ctx):
    import os; os.chdir(PROJECT_ROOT)
    from processing.gold.sales_weather_gold_live import SalesWeatherGoldLive
    SalesWeatherGoldLive().extract()
    logger.info("[gold_live] complete")


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="dag_live_pipeline",
    default_args=DEFAULT_ARGS,
    description="Global Retail & Weather – daily live CDC pipeline",
    schedule_interval="0 1 * * *",     # 01:00 UTC every day
    start_date=datetime(2026, 4, 14),
    catchup=False,
    tags=["capstone", "live", "cdc", "daily"],
) as dag:

    gen_weather  = PythonOperator(task_id="generate_weather",    python_callable=task_generate_weather)
    gen_retail   = PythonOperator(task_id="generate_retail",     python_callable=task_generate_retail)

    # Branch: proceed only if CDC finds new data
    cdc_detect = BranchPythonOperator(
        task_id="cdc_detect",
        python_callable=task_cdc_detect,
    )

    skip_pipeline      = EmptyOperator(task_id="skip_pipeline")

    w_bronze_live = PythonOperator(task_id="weather_bronze_live", python_callable=task_weather_bronze_live)
    s_bronze_live = PythonOperator(task_id="sales_bronze_live",   python_callable=task_sales_bronze_live)
    w_silver_live = PythonOperator(task_id="weather_silver_live", python_callable=task_weather_silver_live)
    s_silver_live = PythonOperator(task_id="sales_silver_live",   python_callable=task_sales_silver_live)
    gold_live     = PythonOperator(task_id="gold_live",           python_callable=task_gold_live)

    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed_min_one_success",   # runs after either branch
    )

    # ── Dependency graph ───────────────────────────────────────────────
    #
    #  gen_weather ──┐
    #                ├──► cdc_detect ──► weather_bronze_live ──► weather_silver_live ──┐
    #  gen_retail  ──┘         │                                                       ├──► gold_live → done
    #                          │         sales_bronze_live   ──► sales_silver_live   ──┘
    #                          └──► skip_pipeline ──────────────────────────────────────────► done
    #

    [gen_weather, gen_retail] >> cdc_detect
    cdc_detect >> skip_pipeline >> done
    cdc_detect >> w_bronze_live >> w_silver_live
    cdc_detect >> s_bronze_live >> s_silver_live
    [w_silver_live, s_silver_live] >> gold_live >> done
