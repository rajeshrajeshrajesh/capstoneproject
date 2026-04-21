# Global Retail & Weather Analytics Platform

A end-to-end data engineering platform that ingests, processes, and analyses **historical UK retail sales (2022–2024)** alongside **live daily weather and sales data**, using a medallion architecture (Raw → Bronze → Silver → Gold) orchestrated with Apache Airflow and powered by Apache Spark and Delta Lake.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Project Structure](#2-project-structure)
3. [Data Sources](#3-data-sources)
4. [Medallion Layers](#4-medallion-layers)
5. [Pipeline Modes](#5-pipeline-modes)
6. [Analytics & Insights](#6-analytics--insights)
7. [Assumptions](#7-assumptions)
8. [Setup Instructions](#8-setup-instructions)
9. [Running the Pipelines](#9-running-the-pipelines)
10. [Daily Live Operations](#10-daily-live-operations)
11. [Airflow Orchestration](#11-airflow-orchestration)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Architecture Overview

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                   GLOBAL RETAIL & WEATHER ANALYTICS PLATFORM                    ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                  ║
║  DATA SOURCES                                                                    ║
║  ┌─────────────────────────┐    ┌──────────────────────────────────────────┐    ║
║  │  Open-Meteo Archive API │    │  UCI Online Retail Dataset (CSV)         │    ║
║  │  (free, no API key)     │    │  2010–2011 → date-shifted to 2022–2023   │    ║
║  │  Historical: 2022–2024  │    │  Live: simulated daily CSV files         │    ║
║  │  Live: today − 5 days   │    │  500 records/day × 5 cities              │    ║
║  └────────────┬────────────┘    └──────────────────────┬───────────────────┘    ║
║               │                                         │                        ║
║  ─────────────────────────────────────────────────────────────────────────────  ║
║  RAW LAYER   (data_lake/raw/)                                                    ║
║               │                                         │                        ║
║  ┌────────────▼────────────┐    ┌────────────────────────▼──────────────────┐   ║
║  │  weather/open_meteo/    │    │  retail_sales/uci_online_retail/          │   ║
║  │  city=London/           │    │  year=YYYY/month=MM/*.parquet             │   ║
║  │    weather_*_hist.json  │    │                                           │   ║
║  │    weather_*_live.json  │    │  retail_sales/uci_online_retail_live/     │   ║
║  └────────────┬────────────┘    │  year=2026/month=04/*.parquet             │   ║
║               │                 └──────────────────────┬───────────────────┘   ║
║               │   CDC Watermark                        │                        ║
║               │   (data_lake/raw/live/cdc/             │                        ║
║               │    watermark.json)                     │                        ║
║  ─────────────────────────────────────────────────────────────────────────────  ║
║  BRONZE LAYER  (data_lake/bronze/)   — Cleaned, typed, city-partitioned         ║
║               │                                         │                        ║
║  ┌────────────▼────────────┐    ┌────────────────────────▼──────────────────┐   ║
║  │  weather/open_meteo/    │    │  sales/online_retail/                     │   ║
║  │  [HISTORICAL ≤2024]     │    │  [HISTORICAL ≤2024, city-partitioned]     │   ║
║  ├─────────────────────────┤    ├───────────────────────────────────────────┤   ║
║  │  weather/open_meteo_live│    │  sales/online_retail_live/                │   ║
║  │  [LIVE today−5 → today] │    │  [LIVE today−5 → today, city-partitioned] │   ║
║  └────────────┬────────────┘    └──────────────────────┬───────────────────┘   ║
║               │                                         │                        ║
║  ─────────────────────────────────────────────────────────────────────────────  ║
║  SILVER LAYER  (data_lake/silver/)   — Aggregated daily totals, Delta format    ║
║               │                                         │                        ║
║  ┌────────────▼────────────┐    ┌────────────────────────▼──────────────────┐   ║
║  │ weather/open_meteo      │    │  sales/online_retail                      │   ║
║  │ [date, city, avg_temp,  │    │  [date, city, total_sales]                │   ║
║  │  precipitation]         │    │  HISTORICAL only                          │   ║
║  │  HISTORICAL only        │    ├───────────────────────────────────────────┤   ║
║  ├─────────────────────────┤    │  sales/online_retail_live                 │   ║
║  │ weather/open_meteo_live │    │  [date, city, total_sales]                │   ║
║  │  LIVE only              │    │  LIVE only                                │   ║
║  └────────────┬────────────┘    └──────────────────────┬───────────────────┘   ║
║               │                                         │                        ║
║  ─────────────────────────────────────────────────────────────────────────────  ║
║  GOLD LAYER  (data_lake/gold/)   — Joined, feature-engineered, Star Schema      ║
║               │                                         │                        ║
║  ┌────────────▼─────────────────────────────────────────▼──────────────────┐    ║
║  │                      sales_weather  (HISTORICAL)                        │    ║
║  │   date │ city │ total_sales │ avg_temp │ precipitation │ is_rainy │     │    ║
║  │   temp_category │ sales_category │ rolling_7d_sales                    │    ║
║  ├─────────────────────────────────────────────────────────────────────────┤    ║
║  │                      sales_weather_live  (LIVE)                         │    ║
║  │   Same schema — today − 5 days → today only                            │    ║
║  ├─────────────────────────────────────────────────────────────────────────┤    ║
║  │  dim_date  │  dim_location  │  fact_sales_weather  (Star Schema)        │    ║
║  └─────────────────────────────────────────────────────────────────────────┘    ║
║                                         │                                        ║
║  ─────────────────────────────────────────────────────────────────────────────  ║
║  ANALYTICS LAYER                                                                 ║
║  ┌──────────────────────────────────────▼──────────────────────────────────┐    ║
║  │              python analysis/cli_app.py                                 │    ║
║  │   [H] Historical menu  │  [L] Live menu  │  [B] Side-by-side compare   │    ║
║  │   Charts saved to analysis/charts/                                      │    ║
║  └─────────────────────────────────────────────────────────────────────────┘    ║
║                                                                                  ║
║  ORCHESTRATION                                                                   ║
║  ┌─────────────────────────────────────────────────────────────────────────┐    ║
║  │  Apache Airflow (Docker)                                                 │    ║
║  │  dag_historical_pipeline  │  dag_live_pipeline  │  dag_dq_checks        │    ║
║  └─────────────────────────────────────────────────────────────────────────┘    ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

### Data Flow Summary

```
Open-Meteo API ──► raw/weather/open_meteo/ ──► bronze/weather/ ──► silver/weather/ ──┐
                                                                                       ├──► gold/sales_weather
UCI Retail CSV ──► raw/retail_sales/       ──► bronze/sales/   ──► silver/sales/   ──┘
                        │
                   CDC Watermark
                   (live only)
```

### Key Separation: Historical vs Live

| Aspect | Historical | Live |
|--------|-----------|------|
| Date range | 2022-01-01 → 2024-12-31 | Today − 5 days → today |
| Raw source (weather) | `raw/weather/open_meteo/` (shared folder, date-filtered) | Same folder, different date filter |
| Raw source (sales) | `raw/retail_sales/uci_online_retail/` | `raw/retail_sales/uci_online_retail_live/` |
| Bronze output | `bronze/weather/open_meteo` | `bronze/weather/open_meteo_live` |
| Silver output | `silver/weather/open_meteo` | `silver/weather/open_meteo_live` |
| Gold output | `gold/sales_weather` | `gold/sales_weather_live` |
| Trigger | Manual / `--all` | CDC watermark + `--live daily` |

---

## 2. Project Structure

```
capstone/
├── main.py                          # Unified pipeline entry point
│
├── configs/
│   └── api_config.yaml              # Cities, date ranges, API endpoints
│
├── ingestion/
│   ├── api_extractor.py             # Open-Meteo REST API client
│   ├── file_extractor.py            # CSV/Parquet file ingestor
│   └── base.py                      # BaseExtractor class
│
├── processing/
│   ├── bronze/
│   │   ├── weather_bronze.py        # Historical weather: raw JSON → Parquet
│   │   ├── weather_bronze_live.py   # Live weather: raw JSON → Parquet (today−5)
│   │   ├── sales_bronze.py          # Historical sales: raw Parquet → clean Parquet
│   │   └── sales_bronze_live.py     # Live sales: raw Parquet → clean Parquet (today−5)
│   ├── silver/
│   │   ├── weather_silver.py        # Historical weather: bronze → Delta
│   │   ├── weather_silver_live.py   # Live weather: bronze → Delta
│   │   ├── sales_silver.py          # Historical sales: bronze → aggregated Delta
│   │   └── sales_silver_live.py     # Live sales: bronze → aggregated Delta
│   └── gold/
│       ├── sales_weather_gold.py    # Historical: silver join → gold Delta
│       ├── sales_weather_gold_live.py # Live: silver join → gold Delta
│       ├── dim_date.py              # Date dimension table
│       ├── dim_location.py          # Location dimension table
│       └── fact_sales_weather.py    # Fact table (star schema)
│
├── scripts/
│   ├── cdc_manager.py               # CDC watermark engine
│   ├── generate_live_weather.py     # Simulate daily weather files
│   ├── generate_live_retail.py      # Simulate daily retail CSV files
│   ├── repair_raw_parquet.py        # One-time schema repair (INT64→INT32)
│   └── run_live_pipeline.py         # Live pipeline runner
│
├── loader/
│   └── spark_loader.py              # SparkSession factory with Delta support
│
├── analysis/
│   ├── cli_app.py                   # Interactive analytics CLI (H / L / B)
│   ├── insights.md                  # Key business findings
│   └── charts/                      # PNG chart outputs
│
├── airflow/
│   └── dags/
│       ├── _project_root.py         # Shared root resolver (Docker-safe)
│       ├── dag_historical_pipeline.py
│       ├── dag_live_pipeline.py
│       └── dag_dq_checks.py
│
├── data_lake/                        # All pipeline data (auto-generated)
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── manual-uploads/
│   └── retail_sales/uci_online_retail/
│       ├── OnlineRetail.csv          # UCI historical dataset
│       └── OnlineRetail_live_*.csv   # Simulated daily files
│
├── docker-compose.yml               # Airflow in Docker
└── requirements.txt
```

---

## 3. Data Sources

### Weather — Open-Meteo Archive API
- **URL:** `https://archive-api.open-meteo.com/v1/archive`
- **Cost:** Free, no API key required
- **Coverage:** 5 UK cities — London, Manchester, Birmingham, Leeds, Glasgow
- **Variables:** `temperature_2m_max`, `temperature_2m_min`, `precipitation_sum`
- **Historical range:** 2022-01-01 → 2024-12-31 (configured in `configs/api_config.yaml`)
- **Live range:** Today − 5 days → today (computed dynamically at runtime)

### Retail Sales — UCI Online Retail Dataset
- **Source:** `manual-uploads/retail_sales/uci_online_retail/OnlineRetail.csv`
- **Original dates:** December 2010 – December 2011
- **Date shift applied:** +12 years → 2022–2023 (via `add_months(InvoiceDate, 144)`)
- **Live simulation:** 500 synthetic records/day generated by `generate_live_retail.py`
- **City assignment:** Deterministic — `CustomerID % 5` maps each customer to one of the 5 cities

---

## 4. Medallion Layers

### Raw Layer — `data_lake/raw/`
Immutable landing zone. Files are never modified after writing.

| Path | Content | Format |
|------|---------|--------|
| `raw/weather/open_meteo/city=*/` | Daily weather per city (hist + live JSON co-located) | JSON |
| `raw/retail_sales/uci_online_retail/year=/month=/` | Historical retail parquet | Parquet |
| `raw/retail_sales/uci_online_retail_live/year=/month=/` | Live retail parquet | Parquet |
| `raw/live/cdc/watermark.json` | CDC watermark state | JSON |

### Bronze Layer — `data_lake/bronze/`
Cleaned and typed data. Still at transaction/row level. Partitioned by city.

| Path | Filter applied |
|------|---------------|
| `bronze/weather/open_meteo/` | `date <= 2024-12-31` |
| `bronze/weather/open_meteo_live/` | `date >= today-5 AND date <= today` |
| `bronze/sales/online_retail/` | `date <= 2024-12-31` |
| `bronze/sales/online_retail_live/` | `date >= today-5 AND date <= today` |

### Silver Layer — `data_lake/silver/`
Aggregated daily totals per city. Delta format. Ready for analytics joins.

| Path | Schema |
|------|--------|
| `silver/weather/open_meteo` | `date, city, avg_temp, precipitation` |
| `silver/weather/open_meteo_live` | Same schema, live window only |
| `silver/sales/online_retail` | `date, city, total_sales` |
| `silver/sales/online_retail_live` | Same schema, live window only |

### Gold Layer — `data_lake/gold/`
Joined and feature-engineered. Delta format. Analytics-ready.

| Path | Description |
|------|-------------|
| `gold/sales_weather` | Historical joined table with derived features |
| `gold/sales_weather_live` | Live joined table (today − 5 days) |
| `gold/dim_date` | Date dimension with year, month, week attributes |
| `gold/dim_location` | Location dimension with surrogate key |
| `gold/fact_sales_weather` | Star schema fact table |

**Derived features in gold:**

| Column | Formula |
|--------|---------|
| `is_rainy` | `1` if `precipitation > 0`, else `0` |
| `temp_category` | `Cold` (<10°C), `Moderate` (10–20°C), `Hot` (>20°C) |
| `sales_category` | `Low` (<£500), `Medium` (£500–£2000), `High` (>£2000) |
| `rolling_7d_sales` | 7-day rolling average of `total_sales` per city |

---

## 5. Pipeline Modes

### Historical Pipeline

```
python main.py --entity weather          # Step 1: Ingest weather JSON
python main.py --entity retail_sales     # Step 2: Ingest retail CSV
python main.py --entity weather_bronze   # Step 3: Raw → Bronze (weather)
python main.py --entity sales_bronze     # Step 4: Raw → Bronze (sales)
python main.py --entity weather_silver   # Step 5: Bronze → Silver (weather)
python main.py --entity sales_silver     # Step 6: Bronze → Silver (sales)
python main.py --entity gold             # Step 7: Silver → Gold (joined)
python main.py --entity dim_date         # Step 8: Build dim_date
python main.py --entity dim_location     # Step 9: Build dim_location
python main.py --entity fact             # Step 10: Build fact table

# Or run everything in one command:
python main.py --all
```

### Live Pipeline

```
python main.py --live backfill           # ONE-TIME: seed last 5 days
python main.py --live daily              # DAILY: add tomorrow's data
python main.py --live daily --date 2026-04-21   # Specific date
python main.py --live cdc                # CDC only (files already exist)
python main.py --live status             # Check watermark state
```

### CDC Watermark Logic

The CDC engine (`scripts/cdc_manager.py`) maintains `data_lake/raw/live/cdc/watermark.json`:

```json
{
  "weather":      "2026-04-20",
  "retail_sales": "2026-04-20"
}
```

On each `--live daily` run, only files with dates **after** the watermark are processed. This guarantees no duplicate processing and efficient incremental loads.

---

## 6. Analytics & Insights

Launch the interactive CLI:

```bash
python analysis/cli_app.py
```

**Menu options:**

| Key | Mode | Data |
|-----|------|------|
| `H` | Historical | 2022–2024 gold table |
| `L` | Live | Today − 5 days gold table |
| `B` | Both/Compare | Side-by-side comparison charts |

**Key findings from the historical data:**

- **Cold weather drives the highest sales** (~£5,990 avg vs £4,533 in hot weather)
- **Manchester is the top-performing city** (~£8,072 avg daily sales)
- **Rainy days outperform dry days** (~£6,081 vs £5,314) — bad weather increases indoor/online purchases
- **Friday is the best sales day** (~£7,459 avg); Monday is the weakest (~£3,182)
- **November peaks at £1.16M monthly revenue** — strong Q4 seasonality
- **Manchester + Cold + Rainy = highest sales conditions** (~£10,239 avg)

Charts are saved to `analysis/charts/` as PNG files.

---

## 7. Assumptions

### Data

1. **UCI date shift is intentional.** The UCI Online Retail dataset covers 2010–2011. All dates are shifted forward by exactly 12 years (144 months) to produce 2022–2023 data, aligning with the historical weather window. This is applied in `sales_bronze.py` via `add_months(InvoiceDate, 144)`.

2. **City assignment is deterministic.** No geographic data exists in the UCI dataset. Cities are assigned by `CustomerID % 5`, ensuring the same customer always maps to the same city across all transactions. This is a simulation assumption.

3. **Live retail data is simulated.** Real transaction feeds are not available. `generate_live_retail.py` creates 500 synthetic records per day by sampling from the historical UCI dataset and stamping them with today's date. The statistical distribution mirrors the real dataset.

4. **Weather and sales share the same raw weather folder.** Historical JSON files (`2022-01-01_2024-12-31`) and live JSON files (`2026-04-14_2026-04-18`) both land in `data_lake/raw/weather/open_meteo/city=X/`. Date filters in bronze and silver transformers strictly separate them — historical keeps `date <= 2024-12-31`, live keeps `date >= today-5 AND date <= today`.

5. **The historical end date of `2024-12-31` is the single source of truth.** It is read from `configs/api_config.yaml` (`ingestion.weather.date_range.end`) in every transformer, so changing it in one place automatically propagates to all pipeline stages.

6. **Live window is always exactly 6 days.** The live pipeline covers today and the 5 preceding days. This window slides forward daily as new data is generated. It does not accumulate indefinitely.

### Infrastructure

7. **Spark runs locally in single-node mode.** No YARN or Kubernetes cluster is assumed. The `SparkLoader` is configured for a local Mac/Linux machine with sufficient RAM (minimum 4 GB recommended for Spark driver).

8. **Delta Lake requires exact version pinning.** `pyspark==3.5.1` and `delta-spark==3.2.0` must be installed together. Other version combinations will fail due to Scala version incompatibilities.

9. **Python 3.9–3.11 is required.** PySpark 3.5.x does not support Python 3.12+.

10. **Java 11 or 17 must be installed.** Spark will not start without a compatible JVM. Java 21 may work but is untested.

11. **Airflow runs inside Docker.** The `docker-compose.yml` mounts the entire project at `/opt/airflow/capstone` and sets `CAPSTONE_ROOT` so all DAGs resolve relative paths correctly regardless of where Airflow stores its DAG files internally.

12. **All paths in pipeline code are relative to the project root.** Scripts must be run from the `capstone/` directory (where `main.py` lives), or the Airflow DAG must call `os.chdir(PROJECT_ROOT)` before invoking any pipeline module.

---

## 8. Setup Instructions

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.9 – 3.11 | 3.12+ not supported by PySpark |
| Java | 11 or 17 | Required for Spark |
| Git | Any | For cloning |
| Docker Desktop | 4.x+ | Only needed for Airflow |

### Step 1 — Clone / unzip the project

```bash
# If from zip:
unzip capstone.zip
cd capstone
```

### Step 2 — Verify Java

```bash
java -version
# Should show: openjdk version "17.x.x" or "11.x.x"
```

If Java is not installed:
```bash
# macOS
brew install openjdk@17

# Ubuntu/Debian
sudo apt install openjdk-17-jdk

# Set JAVA_HOME if needed
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # macOS
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Linux
```

### Step 3 — Create a virtual environment

```bash
python -m venv .venv

# Activate:
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows
```

### Step 4 — Install dependencies

```bash
# Core dependencies — version pins are mandatory
pip install pyspark==3.5.1 delta-spark==3.2.0

# Supporting libraries
pip install pandas matplotlib numpy pyarrow pyyaml requests
```

> **Important:** `pyspark` and `delta-spark` versions must match exactly. 3.5.1 / 3.2.0 is the only tested-and-working combination for this project.

### Step 5 — Create required directories

```bash
mkdir -p logs
mkdir -p analysis/charts
mkdir -p data_lake/raw/live/cdc
```

### Step 6 — Repair raw Parquet files (one-time)

The UCI retail Parquet files have an INT64/INT32 schema inconsistency that must be fixed before running the pipeline:

```bash
python scripts/repair_raw_parquet.py
```

Expected output:
```
Scanning 13 file(s) under data_lake/raw/retail_sales/
  [FIXED]  OnlineRetail_20260416T100408Z.parquet
           Quantity: int64 → int32
           InvoiceDate: datetime64 → str
  ...
Done. 13 file(s) rewritten.
```

---

## 9. Running the Pipelines

### Full Historical Pipeline (run once)

```bash
# Recommended: run all stages in sequence
python main.py --all
```

Or stage by stage if you want to monitor each step:

```bash
python main.py --entity weather          # ~30s — fetches from Open-Meteo API
python main.py --entity retail_sales     # ~5s  — reads local CSV
python main.py --entity weather_bronze   # ~20s
python main.py --entity sales_bronze     # ~30s
python main.py --entity weather_silver   # ~15s
python main.py --entity sales_silver     # ~20s
python main.py --entity gold             # ~25s
python main.py --entity dim_date         # ~10s
python main.py --entity dim_location     # ~5s
python main.py --entity fact             # ~15s
```

Total time: approximately 3–5 minutes on a modern laptop.

### Live Pipeline Setup (run once after historical)

```bash
python main.py --live backfill
```

This generates 5 days of simulated retail and weather data, runs CDC detection, and builds the full live medallion stack (bronze_live → silver_live → gold_live).

Verify it worked:
```bash
python main.py --live status
# Expected:
#   Pending weather files:  0
#   Pending retail files:   0
```

### Launch the Analytics CLI

```bash
python analysis/cli_app.py
```

Press `H` for historical analytics, `L` for live, `B` for side-by-side comparison. Press `A` inside any menu to generate all charts at once. Charts are saved to `analysis/charts/`.

---

## 10. Daily Live Operations

### Add a new day's data

```bash
# Add tomorrow (default)
python main.py --live daily

# Add a specific date
python main.py --live daily --date 2026-04-21
```

### If CDC watermark is already up to date

If `--live daily` reports "No new files found", manually reset the watermark and re-run:

```bash
# Check current watermark
python main.py --live status

# Reset watermark back one day (if needed)
python -c "
import json
wm = json.load(open('data_lake/raw/live/cdc/watermark.json'))
wm['weather'] = '2026-04-19'
wm['retail_sales'] = '2026-04-19'
json.dump(wm, open('data_lake/raw/live/cdc/watermark.json','w'), indent=2)
print('Watermark reset')
"

python main.py --live cdc
```

---

## 11. Airflow Orchestration

Three DAGs are included for automated scheduling:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `dag_historical_pipeline` | Manual trigger | Run the full 10-step historical pipeline |
| `dag_live_pipeline` | Daily at 01:00 UTC | Generate + CDC + rebuild live stack |
| `dag_dq_checks` | Manual trigger | Run data quality checks across all layers |

### Start Airflow with Docker

```bash
# From the capstone/ directory:
docker-compose up -d

# Watch startup logs:
docker-compose logs -f scheduler
```

Airflow UI available at: **http://localhost:8081**
Default credentials: `admin` / `admin`

### First-time Airflow setup

```bash
# Wait ~60 seconds for init to complete, then:
# 1. Open http://localhost:8081
# 2. Enable dag_historical_pipeline → click ▶ to trigger
# 3. Once complete, enable dag_live_pipeline
# 4. Run dag_dq_checks manually to validate all layers
```

### Trigger DAGs from CLI

```bash
# Trigger historical pipeline
docker exec -it <scheduler_container> airflow dags trigger dag_historical_pipeline

# Trigger live pipeline for a specific date
docker exec -it <scheduler_container> airflow dags trigger dag_live_pipeline \
  --conf '{"target_date": "2026-04-21"}'

# Trigger DQ checks for silver layer only
docker exec -it <scheduler_container> airflow dags trigger dag_dq_checks \
  --conf '{"layer": "silver"}'
```

### Stop Airflow

```bash
docker-compose down
```

---

## 12. Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `ClassNotFoundException: delta.DefaultSource` | Delta JAR not loaded — raw `SparkSession.builder` used instead of `SparkLoader` | Ensure all transformers use `SparkLoader()` not `SparkSession.builder` |
| `PurePath.relative_to() got unexpected keyword argument 'walk_up'` | Python 3.12 used instead of 3.9–3.11 | Switch Python version: `python3.11 -m venv .venv` |
| `delta-spark version conflict` | Wrong delta-spark version installed | `pip install pyspark==3.5.1 delta-spark==3.2.0` exactly |
| `INT64 / INT32 conflict` in bronze | Raw Parquet files not repaired | `python scripts/repair_raw_parquet.py` |
| `PATH_NOT_FOUND` on bronze/live path | Live pipeline never run | `python main.py --live backfill` |
| `No new files found` in CDC | Watermark already at today's date | Reset watermark JSON manually (see §10) |
| `JAVA_HOME not set` | Java not found | Install Java 11/17 and export `JAVA_HOME` |
| Spark runs out of memory | Too little driver RAM | `export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"` |
| Airflow DAG path errors in Docker | `PROJECT_ROOT` resolves to Airflow's home, not project | Ensure `CAPSTONE_ROOT` env var is set in `docker-compose.yml` |
| `dag_live_pipeline` shows phantom failed runs | `start_date` set to a past date with `catchup=False` | Set `start_date` to today in the DAG definition |

---

## Data Quality Checks

Run standalone:
```bash
python dq/data_quality.py                  # All layers
python dq/data_quality.py --layer silver   # Silver only
python dq/data_quality.py --strict         # Fail on any FAIL result
python dq/data_quality.py --output dq/report.json  # Save JSON report
```

**Checks implemented:**

| ID | Layer | Check |
|----|-------|-------|
| DQ-B01 | Bronze | Null % per column ≤ 5% |
| DQ-B02 | Bronze | Duplicate row count |
| DQ-B03 | Bronze | Row count > 100 |
| DQ-S01 | Silver | Zero nulls on critical columns (date, city, total_sales, avg_temp) |
| DQ-S02 | Silver | No negative total_sales |
| DQ-S03 | Silver | avg_temp within −40°C to 60°C |
| DQ-S04 | Silver | No duplicate (date, city) pairs |
| DQ-S05 | Silver | All 5 cities present |
| DQ-G01 | Gold | Null % on measure columns |
| DQ-G02 | Gold | `is_rainy` values only {0, 1} |
| DQ-G03 | Gold | `temp_category` ∈ {Cold, Moderate, Hot} |
| DQ-G04 | Gold | `sales_category` ∈ {Low, Medium, High} |
| DQ-G05 | Gold | `rolling_7d_sales` ≥ 0 |
| DQ-G06 | Gold | Referential integrity: fact → dim_date |
| DQ-G07 | Gold | Referential integrity: fact → dim_location |

---

*Built with PySpark 3.5.1 · Delta Lake 3.2.0 · Apache Airflow 2.8.1 · Open-Meteo API · UCI Online Retail Dataset*
