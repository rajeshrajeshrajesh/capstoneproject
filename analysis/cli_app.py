"""
analysis/cli_app.py  (v2 – full pipeline integration)

Two execution modes:

  [H] HISTORICAL  – runs raw → bronze → silver → gold then shows analytics
  [L] LIVE        – runs raw → bronze_live → silver_live → gold_live then analytics
  [B] BOTH        – runs both pipelines and produces side-by-side comparison charts

Usage:
    python analysis/cli_app.py                 # interactive
    python analysis/cli_app.py --mode H        # historical pipeline + analysis
    python analysis/cli_app.py --mode L        # live pipeline + analysis
    python analysis/cli_app.py --mode B        # both + comparison
    python analysis/cli_app.py --skip-pipeline # skip pipeline, use existing gold tables
"""

import sys
import argparse
from pathlib import Path
import logging

sys.path.append(str(Path(__file__).resolve().parents[1]))

from loader.spark_loader import SparkLoader
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("cli_app")

loader = SparkLoader()
spark  = loader.spark

HIST_GOLD_PATH = "data_lake/gold/sales_weather"
LIVE_GOLD_PATH = "data_lake/gold/sales_weather_live"
CHART_DIR      = Path("analysis/charts")
CHART_DIR.mkdir(parents=True, exist_ok=True)

PALETTE_HIST = "#4C72B0"
PALETTE_LIVE = "#DD8452"
PALETTE_RAIN = "#55A868"
CITY_COLORS  = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B2"]


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE RUNNERS
# ═══════════════════════════════════════════════════════════════════════

def run_historical_pipeline():
    """Run complete historical raw → bronze → silver → gold stack."""
    print("\n  ▶  Running HISTORICAL pipeline (raw → bronze → silver → gold)...")
    try:
        from ingestion.api_extractor  import APIExtractor
        from ingestion.file_extractor import FileExtractor
        from processing.bronze.sales_bronze    import SalesBronzeTransformer
        from processing.bronze.weather_bronze  import WeatherBronzeTransformer
        from processing.silver.sales_silver    import SalesSilverTransformer
        from processing.silver.weather_silver  import WeatherSilverTransformer
        from processing.gold.sales_weather_gold  import SalesWeatherGold
        from processing.gold.dim_date            import DimDate
        from processing.gold.dim_location        import DimLocation
        from processing.gold.fact_sales_weather  import FactSalesWeather
        from processing.gold.dim_product import DimProduct
        from processing.gold.fact_sales_weather_product import FactSalesWeatherProduct

        print("  [1/8] Weather ingestion...")
        APIExtractor(config_path="configs/api_config.yaml").extract(entity="weather")
        print("  [2/8] Retail ingestion...")
        FileExtractor(config_path="configs/api_config.yaml").extract(
            entity="retail_sales", source_name="uci_online_retail", output_format="parquet"
        )
        print("  [3/8] Weather bronze...")
        WeatherBronzeTransformer().extract()
        print("  [4/8] Sales bronze...")
        SalesBronzeTransformer().extract()
        print("  [5/8] Weather silver...")
        WeatherSilverTransformer().extract()
        print("  [6/8] Sales silver...")
        SalesSilverTransformer().extract()
        print("  [7/8] Dimensions...")
        DimDate().extract(); DimLocation().extract();DimProduct().extract()
        print("  [8/8] Gold...")
        SalesWeatherGold().extract(); FactSalesWeather().extract(); FactSalesWeatherProduct().extract()
        print("  ✔  Historical pipeline complete.\n")
        return True
    except Exception as e:
        logger.error(f"Historical pipeline failed: {e}")
        print(f"  ✗  Error: {e}")
        return False


def run_live_pipeline():
    """
    Run complete live pipeline:
    backfill 5 days → CDC → bronze_live → silver_live → gold_live
    """
    print("\n  ▶  Running LIVE pipeline (backfill → CDC → bronze → silver → gold live)...")
    try:
        from scripts.generate_live_weather import run_backfill as w_backfill
        from scripts.generate_live_retail  import run_backfill as r_backfill
        from scripts.cdc_manager import run_cdc_all
        from processing.bronze.sales_bronze_live    import SalesBronzeLiveTransformer
        from processing.bronze.weather_bronze_live  import WeatherBronzeLiveTransformer
        from processing.silver.sales_silver_live    import SalesSilverLiveTransformer
        from processing.silver.weather_silver_live  import WeatherSilverLiveTransformer
        from processing.gold.sales_weather_gold_live import SalesWeatherGoldLive

        print("  [1/5] Generate live weather files (last 5 days)...")
        w_backfill()
        print("  [2/5] Generate live retail files (last 5 days)...")
        r_backfill()
        print("  [3/5] CDC – stage new files + update watermarks...")
        run_cdc_all(dry_run=False)
        print("  [4/5] Bronze → Silver live...")
        WeatherBronzeLiveTransformer().extract()
        WeatherSilverLiveTransformer().extract()
        SalesBronzeLiveTransformer().extract()
        SalesSilverLiveTransformer().extract()
        print("  [5/5] Gold live...")
        SalesWeatherGoldLive().extract()
        print("  ✔  Live pipeline complete.\n")
        return True
    except Exception as e:
        logger.error(f"Live pipeline failed: {e}")
        print(f"  ✗  Error: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════
# DATASET LOADER
# ═══════════════════════════════════════════════════════════════════════

def load_dataset(mode: str):
    path  = HIST_GOLD_PATH if mode == "H" else LIVE_GOLD_PATH
    label = "Historical (2022–2024)" if mode == "H" else "Live (last 5 days + today)"
    try:
        df    = loader.read(path, format="delta")
        df.createOrReplaceTempView("sales_weather")
        count = df.count()
        print(f"\n  ✔  Loaded {label}  —  {count:,} rows")
        return True
    except Exception as e:
        print(f"\n  ✗  Could not load '{path}': {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════
# CHART HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _save(fig, name: str):
    path = CHART_DIR / f"{name}.png"
    fig.savefig(path, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"  [chart → {path}]")


def _bar(pdf, x_col, y_col, title, xlabel, ylabel, fname, rotation=0, color=PALETTE_HIST):
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(pdf[x_col].astype(str), pdf[y_col], color=color, edgecolor="white")
    ax.set_title(title, fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel); ax.set_ylabel(ylabel)
    plt.xticks(rotation=rotation, ha="right" if rotation else "center")
    plt.tight_layout()
    _save(fig, fname)


def _line(pdf, x_col, y_col, title, xlabel, ylabel, fname, color=PALETTE_HIST):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(pdf[x_col], pdf[y_col], marker="o", linewidth=2, color=color)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel); ax.set_ylabel(ylabel)
    ax.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    _save(fig, fname)


def _show_df(df):
    df.show(truncate=False)


# ═══════════════════════════════════════════════════════════════════════
# SHARED QUERIES (operate on whichever temp view is loaded)
# ═══════════════════════════════════════════════════════════════════════

def q_temp_vs_sales(tag="", color=PALETTE_HIST):
    print("\n─── Temperature vs Sales ───")
    result = spark.sql("""
        SELECT temp_category,
               ROUND(AVG(total_sales), 2) AS avg_sales,
               COUNT(*) AS days
        FROM sales_weather GROUP BY temp_category ORDER BY avg_sales DESC
    """)
    _show_df(result)
    _bar(result.toPandas(), "temp_category", "avg_sales",
         "Avg Sales by Temperature Category",
         "Temp Category", "Avg Daily Sales (£)", f"temp_vs_sales{tag}", color=color)


def q_monthly_trends(tag="", color=PALETTE_HIST):
    print("\n─── Monthly Revenue Trends ───")
    result = spark.sql("""
        SELECT YEAR(date) AS year, MONTH(date) AS month,
               ROUND(SUM(total_sales), 2) AS revenue
        FROM sales_weather GROUP BY year, month ORDER BY year, month
    """)
    _show_df(result)
    pdf = result.toPandas()
    pdf["period"] = pdf["year"].astype(str) + "-" + pdf["month"].astype(str).str.zfill(2)
    _line(pdf, "period", "revenue", "Monthly Revenue Trends",
          "Year-Month", "Revenue (£)", f"monthly_trends{tag}", color=color)


def q_city_performance(tag="", color=PALETTE_HIST):
    print("\n─── City-Level Sales Performance ───")
    result = spark.sql("""
        SELECT city, ROUND(AVG(total_sales), 2) AS avg_sales,
               ROUND(SUM(total_sales), 2) AS total_revenue, COUNT(*) AS days
        FROM sales_weather GROUP BY city ORDER BY avg_sales DESC
    """)
    _show_df(result)
    _bar(result.toPandas(), "city", "avg_sales",
         "Avg Daily Sales by City", "City", "Avg Daily Sales (£)",
         f"city_performance{tag}", rotation=20, color=color)


def q_rain_impact(tag="", color=PALETTE_HIST):
    print("\n─── Rain Impact on Sales ───")
    result = spark.sql("""
        SELECT CASE WHEN is_rainy=1 THEN 'Rainy' ELSE 'Dry' END AS weather,
               ROUND(AVG(total_sales), 2) AS avg_sales, COUNT(*) AS days
        FROM sales_weather GROUP BY is_rainy ORDER BY is_rainy
    """)
    _show_df(result)
    pdf = result.toPandas()
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.bar(pdf["weather"], pdf["avg_sales"],
           color=[color, PALETTE_RAIN], edgecolor="white", width=0.5)
    ax.set_title("Rain Impact on Sales", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("Avg Daily Sales (£)")
    for i, v in enumerate(pdf["avg_sales"]):
        ax.text(i, v + 5, f"£{v:,.0f}", ha="center", fontsize=11)
    plt.tight_layout()
    _save(fig, f"rain_impact{tag}")


def q_top_sales_days(tag="", color=PALETTE_HIST):
    print("\n─── Top 10 Peak Sales Days ───")
    result = spark.sql("""
        SELECT date, city, ROUND(total_sales,2) AS total_sales,
               ROUND(avg_temp,1) AS avg_temp, precipitation
        FROM sales_weather ORDER BY total_sales DESC LIMIT 10
    """)
    _show_df(result)
    pdf = result.toPandas()
    pdf["label"] = pdf["date"].astype(str) + "\n" + pdf["city"]
    _bar(pdf, "label", "total_sales", "Top 10 Peak Sales Days",
         "Date (City)", "Total Sales (£)", f"top_sales_days{tag}",
         rotation=30, color=color)


def q_weekly_trend(tag="", color=PALETTE_HIST):
    print("\n─── Weekly Sales Trend ───")
    result = spark.sql("""
        SELECT WEEKOFYEAR(date) AS week, ROUND(AVG(total_sales),2) AS avg_sales
        FROM sales_weather GROUP BY week ORDER BY week
    """)
    _show_df(result)
    _line(result.toPandas(), "week", "avg_sales", "Weekly Avg Sales",
          "Week of Year", "Avg Sales (£)", f"weekly_trend{tag}", color=color)


def q_day_of_week(tag="", color=PALETTE_HIST):
    print("\n─── Best Performing Day of Week ───")
    result = spark.sql("""
        SELECT CASE DAYOFWEEK(date)
               WHEN 1 THEN 'Sunday'   WHEN 2 THEN 'Monday'
               WHEN 3 THEN 'Tuesday'  WHEN 4 THEN 'Wednesday'
               WHEN 5 THEN 'Thursday' WHEN 6 THEN 'Friday'
               WHEN 7 THEN 'Saturday' END AS day_name,
               ROUND(AVG(total_sales),2) AS avg_sales
        FROM sales_weather GROUP BY day_name ORDER BY avg_sales DESC
    """)
    _show_df(result)
    _bar(result.toPandas(), "day_name", "avg_sales",
         "Avg Sales by Day of Week", "Day", "Avg Sales (£)",
         f"day_of_week{tag}", rotation=20, color=color)


def q_peak_conditions(tag=""):
    print("\n─── Top 10 Conditions (City × Temp × Rain) ───")
    spark.sql("""
        SELECT city, temp_category,
               CASE WHEN is_rainy=1 THEN 'Rainy' ELSE 'Dry' END AS weather,
               ROUND(AVG(total_sales),2) AS avg_sales, COUNT(*) AS days
        FROM sales_weather GROUP BY city, temp_category, is_rainy
        ORDER BY avg_sales DESC LIMIT 10
    """).show(truncate=False)


def q_sales_growth(tag="", color=PALETTE_HIST):
    print("\n─── Year-over-Year Sales Growth ───")
    result = spark.sql("""
        SELECT YEAR(date) AS year, ROUND(SUM(total_sales),2) AS annual_revenue,
               COUNT(DISTINCT date) AS trading_days
        FROM sales_weather GROUP BY year ORDER BY year
    """)
    _show_df(result)
    _bar(result.toPandas(), "year", "annual_revenue", "Annual Revenue",
         "Year", "Total Revenue (£)", f"sales_growth{tag}", color=color)
    
def q_topselling_products_in_rainy_days(tag="", color=PALETTE_HIST):
    print("\n─── Top Products on Rainy Days ───")

    df = loader.read("data_lake/gold/fact_sales_weather_product", format="delta")
    df.createOrReplaceTempView("fact_sales_weather_product")

    result = spark.sql("""
        SELECT 
            product_description,
            ROUND(SUM(total_price), 2) AS total_revenue,
            SUM(Quantity)              AS total_units
        FROM fact_sales_weather_product
        WHERE is_rainy = 1
        GROUP BY product_description
        ORDER BY total_revenue DESC
        LIMIT 15
    """)

    # Show table (consistent with others)
    _show_df(result)

    # Convert to pandas
    pdf = result.toPandas()

    # Use shared bar helper (horizontal style manually handled)
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(pdf["product_description"], pdf["total_revenue"],
            color=color, edgecolor="white")

    ax.set_title("Top 15 Products on Rainy Days", fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Total Revenue (£)")
    ax.set_ylabel("Product")

    ax.invert_yaxis()  # highest on top
    plt.tight_layout()

    _save(fig, f"top_products_rainy{tag}")



# ── LIVE-ONLY queries ─────────────────────────────────────────────────────

def q_live_daily_summary():
    print("\n─── Live: Daily Summary ───")
    spark.sql("""
        SELECT date, COUNT(DISTINCT city) AS cities,
               ROUND(SUM(total_sales),2) AS total_revenue,
               ROUND(AVG(avg_temp),1) AS avg_temp_c,
               SUM(is_rainy) AS rainy_cities
        FROM sales_weather GROUP BY date ORDER BY date DESC
    """).show(truncate=False)


def q_live_vs_rolling():
    print("\n─── Live: Daily Sales vs 7-Day Rolling Average ───")
    result = spark.sql("""
        SELECT date, city,
               ROUND(total_sales,2) AS sales,
               ROUND(rolling_7d_sales,2) AS rolling_avg,
               ROUND(total_sales - rolling_7d_sales,2) AS delta
        FROM sales_weather ORDER BY date DESC, city
    """)
    _show_df(result)
    pdf = result.toPandas()
    if pdf.empty: return
    fig, ax = plt.subplots(figsize=(11, 5))
    for i, (city, grp) in enumerate(pdf.groupby("city")):
        grp = grp.sort_values("date")
        xs  = grp["date"].astype(str).tolist()
        c   = CITY_COLORS[i % 5]
        ax.plot(xs, grp["sales"],       marker="o",  color=c, label=f"{city} sales", linewidth=2)
        ax.plot(xs, grp["rolling_avg"], linestyle="--", color=c, alpha=0.55, label=f"{city} 7d avg")
    ax.set_title("Live: Daily Sales vs 7-Day Rolling Avg", fontsize=13, fontweight="bold")
    ax.set_xlabel("Date"); ax.set_ylabel("Sales (£)")
    ax.legend(fontsize=7, ncol=2)
    plt.xticks(rotation=30)
    plt.tight_layout()
    _save(fig, "live_vs_rolling")


def q_live_weather_correlation():
    print("\n─── Live: Temperature vs Sales Scatter ───")
    result = spark.sql("""
        SELECT city, date, ROUND(avg_temp,1) AS avg_temp,
               ROUND(total_sales,2) AS sales, is_rainy
        FROM sales_weather ORDER BY date
    """)
    _show_df(result)
    pdf = result.toPandas()
    if pdf.empty: return
    fig, ax = plt.subplots(figsize=(8, 5))
    for i, (city, grp) in enumerate(pdf.groupby("city")):
        ax.scatter(grp["avg_temp"], grp["sales"],
                   c=CITY_COLORS[i % 5], label=city, alpha=0.85,
                   marker="^" if grp["is_rainy"].any() else "o", s=70)
    ax.set_title("Live: Temperature vs Sales", fontsize=13, fontweight="bold")
    ax.set_xlabel("Avg Temp (°C)"); ax.set_ylabel("Daily Sales (£)")
    ax.legend(fontsize=9)
    plt.tight_layout()
    _save(fig, "live_temp_scatter")


# ═══════════════════════════════════════════════════════════════════════
# COMBINED / COMPARISON CHARTS (NEW)
# ═══════════════════════════════════════════════════════════════════════

def q_combined_city_comparison(hist_df, live_df):
    """Side-by-side city avg sales: Historical vs Live."""
    print("\n─── Combined: City Performance ───")
    from pyspark.sql.functions import avg as _avg
    def _agg(df):
        return (df.groupBy("city").agg(_avg("total_sales").alias("avg"))
                  .toPandas().sort_values("city"))
    h = _agg(hist_df); l = _agg(live_df)
    m = pd.merge(h, l, on="city", suffixes=("_hist", "_live"))
    x = np.arange(len(m)); w = 0.35
    fig, ax = plt.subplots(figsize=(10, 6))
    b1 = ax.bar(x - w/2, m["avg_hist"], w, label="Historical", color=PALETTE_HIST, edgecolor="white")
    b2 = ax.bar(x + w/2, m["avg_live"], w, label="Live",       color=PALETTE_LIVE, edgecolor="white")
    ax.set_title("City Sales: Historical vs Live", fontsize=14, fontweight="bold", pad=12)
    ax.set_xticks(x); ax.set_xticklabels(m["city"], rotation=15)
    ax.set_ylabel("Avg Daily Sales (£)"); ax.legend()
    ax.bar_label(b1, fmt="£%.0f", fontsize=8, padding=3)
    ax.bar_label(b2, fmt="£%.0f", fontsize=8, padding=3)
    plt.tight_layout()
    _save(fig, "combined_city_comparison")


def q_combined_rain_impact(hist_df, live_df):
    """2-panel rain impact comparison."""
    print("\n─── Combined: Rain Impact ───")
    from pyspark.sql.functions import avg as _avg, col, when
    def _agg(df):
        return (df.withColumn("w", when(col("is_rainy")==1,"Rainy").otherwise("Dry"))
                  .groupBy("w").agg(_avg("total_sales").alias("avg"))
                  .toPandas().sort_values("w"))
    h = _agg(hist_df); l = _agg(live_df)
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    for ax, pdf, title, color in [(axes[0], h, "Historical", PALETTE_HIST),
                                   (axes[1], l, "Live", PALETTE_LIVE)]:
        ax.bar(pdf["w"], pdf["avg"], color=[color, PALETTE_RAIN], edgecolor="white", width=0.5)
        ax.set_title(f"Rain Impact — {title}", fontsize=13, fontweight="bold")
        ax.set_ylabel("Avg Daily Sales (£)")
        for i, v in enumerate(pdf["avg"]):
            ax.text(i, v + 5, f"£{v:,.0f}", ha="center", fontsize=10)
    plt.tight_layout()
    _save(fig, "combined_rain_impact")


def q_combined_dashboard(hist_df, live_df):
    """
    Full 2×2 dashboard:
      [0,0] City avg sales comparison
      [0,1] Temp category comparison
      [1,0] Rain impact comparison
      [1,1] Historical monthly revenue trend
    """
    print("\n─── Combined Dashboard (2×2) ───")
    from pyspark.sql.functions import avg as _avg, sum as _sum, col, when

    def city_avg(df):
        return df.groupBy("city").agg(_avg("total_sales").alias("avg")).toPandas().sort_values("city")
    def temp_avg(df):
        return (df.groupBy("temp_category").agg(_avg("total_sales").alias("avg"))
                  .toPandas().set_index("temp_category")
                  .reindex(["Cold","Moderate","Hot"]).fillna(0).reset_index())
    def rain_avg(df):
        return (df.withColumn("w", when(col("is_rainy")==1,"Rainy").otherwise("Dry"))
                  .groupBy("w").agg(_avg("total_sales").alias("avg"))
                  .toPandas().sort_values("w"))

    h_city = city_avg(hist_df); l_city = city_avg(live_df)
    h_temp = temp_avg(hist_df); l_temp = temp_avg(live_df)
    h_rain = rain_avg(hist_df); l_rain = rain_avg(live_df)

    fig = plt.figure(figsize=(14, 10))
    fig.suptitle("Global Retail & Weather Analytics — Historical vs Live",
                 fontsize=16, fontweight="bold", y=1.01)
    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)
    w  = 0.35

    # Panel A – city
    ax = fig.add_subplot(gs[0, 0])
    m  = pd.merge(h_city, l_city, on="city", suffixes=("_h","_l"))
    x  = np.arange(len(m))
    ax.bar(x - w/2, m["avg_h"], w, label="Historical", color=PALETTE_HIST)
    ax.bar(x + w/2, m["avg_l"], w, label="Live",       color=PALETTE_LIVE)
    ax.set_title("City: Avg Daily Sales", fontweight="bold")
    ax.set_xticks(x); ax.set_xticklabels(m["city"], rotation=20, fontsize=8)
    ax.set_ylabel("Avg Sales (£)"); ax.legend(fontsize=8)

    # Panel B – temp
    ax = fig.add_subplot(gs[0, 1])
    cats = ["Cold","Moderate","Hot"]
    hv = h_temp.set_index("temp_category")["avg"].reindex(cats).fillna(0)
    lv = l_temp.set_index("temp_category")["avg"].reindex(cats).fillna(0)
    x  = np.arange(len(cats))
    ax.bar(x - w/2, hv, w, label="Historical", color=PALETTE_HIST)
    ax.bar(x + w/2, lv, w, label="Live",       color=PALETTE_LIVE)
    ax.set_title("Temp Category: Avg Sales", fontweight="bold")
    ax.set_xticks(x); ax.set_xticklabels(cats, fontsize=9)
    ax.set_ylabel("Avg Sales (£)"); ax.legend(fontsize=8)

    # Panel C – rain
    ax = fig.add_subplot(gs[1, 0])
    rain_cats = ["Dry","Rainy"]
    hv = h_rain.set_index("w")["avg"].reindex(rain_cats).fillna(0)
    lv = l_rain.set_index("w")["avg"].reindex(rain_cats).fillna(0)
    x  = np.arange(len(rain_cats))
    ax.bar(x - w/2, hv, w, label="Historical", color=PALETTE_HIST)
    ax.bar(x + w/2, lv, w, label="Live",       color=PALETTE_LIVE)
    ax.set_title("Rain Impact: Avg Sales", fontweight="bold")
    ax.set_xticks(x); ax.set_xticklabels(rain_cats, fontsize=9)
    ax.set_ylabel("Avg Sales (£)"); ax.legend(fontsize=8)

    # Panel D – historical monthly trend
    ax = fig.add_subplot(gs[1, 1])
    monthly = (hist_df
               .groupBy(hist_df["date"].cast("date").substr(1,7).alias("ym"))
               .agg(_sum("total_sales").alias("rev"))
               .orderBy("ym").toPandas())
    ax.plot(monthly["ym"], monthly["rev"], color=PALETTE_HIST, marker=".", linewidth=1.5)
    ax.set_title("Historical Monthly Revenue", fontweight="bold")
    ax.set_xlabel("Month"); ax.set_ylabel("Revenue (£)")
    ax.tick_params(axis="x", rotation=45, labelsize=7)

    plt.tight_layout()
    _save(fig, "combined_dashboard")


def run_both_analysis():
    """Load both datasets and produce all individual + comparison charts."""
    print("\n  ▶  Loading both datasets for comparison...")
    try:
        hist_df = loader.read(HIST_GOLD_PATH, format="delta")
        print(f"  ✔  Historical: {hist_df.count():,} rows")
    except Exception as e:
        print(f"  ✗  Historical gold not found ({e}). Run option [H] first.")
        return
    try:
        live_df = loader.read(LIVE_GOLD_PATH, format="delta")
        print(f"  ✔  Live: {live_df.count():,} rows")
    except Exception as e:
        print(f"  ✗  Live gold not found ({e}). Run option [L] first.")
        return

    print("\n  ── Comparison charts ──")
    q_combined_city_comparison(hist_df, live_df)
    q_combined_rain_impact(hist_df, live_df)
    q_combined_dashboard(hist_df, live_df)

    print("\n  ── Historical charts ──")
    hist_df.createOrReplaceTempView("sales_weather")
    for fn in [q_temp_vs_sales, q_monthly_trends, q_city_performance,
               q_rain_impact, q_top_sales_days, q_weekly_trend,
               q_day_of_week, q_sales_growth]:
        fn("_hist", PALETTE_HIST)
    q_peak_conditions("_hist")

    print("\n  ── Live charts ──")
    live_df.createOrReplaceTempView("sales_weather")
    q_live_daily_summary()
    q_live_vs_rolling()
    q_live_weather_correlation()
    for fn in [q_rain_impact, q_city_performance, q_temp_vs_sales, q_top_sales_days]:
        fn("_live", PALETTE_LIVE)
    q_peak_conditions("_live")

    print(f"\n  ✔  All charts saved to {CHART_DIR}/")


# ═══════════════════════════════════════════════════════════════════════
# MENUS
# ═══════════════════════════════════════════════════════════════════════

HIST_MENU = """
╔══════════════════════════════════════════════════════════╗
        HISTORICAL DATA ANALYTICS  (2022 – 2024)         
══════════════════════════════════════════════════════════
  1.  Temperature vs Sales                               
  2.  Monthly Revenue Trends                             
  3.  City-Level Performance                             
  4.  Rain Impact on Sales                               
  5.  Top 10 Peak Sales Days                             
  6.  Weekly Sales Trend                                
  7.  Best Day of Week                                   
  8.  Peak Conditions (City × Temp × Rain)              
  9.  Year-over-Year Sales Growth
 10.  Top 15 Products on Rainy Days
  A.  Run ALL analyses + charts                          
  0.  ← Back                                             
══════════════════════════════════════════════════════════╝"""

LIVE_MENU = """
╔══════════════════════════════════════════════════════════╗
║     LIVE DATA ANALYTICS  (last 5 days + today)          ║
╠══════════════════════════════════════════════════════════╣
  1.  Daily Summary (revenue + weather per day)          
  2.  Sales vs 7-Day Rolling Average                     
  3.  Temperature vs Sales Scatter                       
  4.  Rain Impact on Sales                               
  5.  Top Peak Days                                      
  6.  City-Level Performance                             
  7.  Temperature Category vs Sales                      
  8.  Peak Conditions (City × Temp × Rain)              
  A.  Run ALL analyses + charts                          
  0.  ← Back                                             
╚══════════════════════════════════════════════════════════╝"""

DATASET_MENU = """
╔══════════════════════════════════════════════════════════╗
║   Global Retail & Weather Analytics Platform  v2        ║
╠══════════════════════════════════════════════════════════╣
                                                         
   H  →  Historical  (2022–2024)                          
          Runs full raw→bronze→silver→gold pipeline        
          then opens analytics + chart menu                                                                         
   L  →  Live  (last 5 days + today)                     
          Runs full live pipeline (CDC + medallion stack)  
          then opens analytics + chart menu               
                                                              
   Q  →  Quit                                             
╚══════════════════════════════════════════════════════════╝"""


def run_historical_menu():
    while True:
        print(HIST_MENU)
        c = input("  Choice: ").strip().upper()
        if   c=="1": q_temp_vs_sales("_hist", PALETTE_HIST)
        elif c=="2": q_monthly_trends("_hist", PALETTE_HIST)
        elif c=="3": q_city_performance("_hist", PALETTE_HIST)
        elif c=="4": q_rain_impact("_hist", PALETTE_HIST)
        elif c=="5": q_top_sales_days("_hist", PALETTE_HIST)
        elif c=="6": q_weekly_trend("_hist", PALETTE_HIST)
        elif c=="7": q_day_of_week("_hist", PALETTE_HIST)
        elif c=="8": q_peak_conditions("_hist")
        elif c=="9": q_sales_growth("_hist", PALETTE_HIST)
        elif c=="10": q_topselling_products_in_rainy_days("_hist", PALETTE_HIST)
        elif c=="A":
            for fn in [q_temp_vs_sales,q_monthly_trends,q_city_performance,
                       q_rain_impact,q_top_sales_days,q_weekly_trend,
                       q_day_of_week,q_sales_growth]:
                fn("_hist", PALETTE_HIST)
            q_peak_conditions("_hist")
            print(f"\n  ✔  All charts → {CHART_DIR}/")
        elif c=="0": break
        else: print("  Enter 0-9 or A.")


def run_live_menu():
    while True:
        print(LIVE_MENU)
        c = input("  Choice: ").strip().upper()
        if   c=="1": q_live_daily_summary()
        elif c=="2": q_live_vs_rolling()
        elif c=="3": q_live_weather_correlation()
        elif c=="4": q_rain_impact("_live", PALETTE_LIVE)
        elif c=="5": q_top_sales_days("_live", PALETTE_LIVE)
        elif c=="6": q_city_performance("_live", PALETTE_LIVE)
        elif c=="7": q_temp_vs_sales("_live", PALETTE_LIVE)
        elif c=="8": q_peak_conditions("_live")
        elif c=="A":
            q_live_daily_summary(); q_live_vs_rolling(); q_live_weather_correlation()
            for fn in [q_rain_impact,q_city_performance,q_temp_vs_sales,q_top_sales_days]:
                fn("_live", PALETTE_LIVE)
            q_peak_conditions("_live")
            print(f"\n  ✔  All charts → {CHART_DIR}/")
        elif c=="0": break
        else: print("  Enter 0-8 or A.")


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Global Retail & Weather Analytics CLI v2")
    parser.add_argument("--mode", choices=["H","L","B"],
                        help="Non-interactive: H=historical, L=live, B=both")
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="Skip pipeline; use existing gold tables")
    args = parser.parse_args()

    print("\n  Initialising Spark session…  (this takes ~20 s on first run)")

    if args.mode:
        if args.mode == "H":
            if not args.skip_pipeline: run_historical_pipeline()
            if load_dataset("H"): run_historical_menu()
        elif args.mode == "L":
            if not args.skip_pipeline: run_live_pipeline()
            if load_dataset("L"): run_live_menu()
        elif args.mode == "B":
            if not args.skip_pipeline:
                run_historical_pipeline()
                run_live_pipeline()
            run_both_analysis()
        return

    # interactive
    while True:
        print(DATASET_MENU)
        choice = input("  Select [H / L / B / Q]: ").strip().upper()
        if choice == "Q":
            print("\n  Bye!\n"); break
        elif choice == "H":
            ans = input("  Run full historical pipeline? [y/N]: ").strip().lower()
            if ans == "y": run_historical_pipeline()
            if load_dataset("H"): run_historical_menu()
        elif choice == "L":
            ans = input("  Run full live pipeline? [y/N]: ").strip().lower()
            if ans == "y": run_live_pipeline()
            if load_dataset("L"): run_live_menu()
        elif choice == "B":
            ans = input("  Run both pipelines? [y/N]: ").strip().lower()
            if ans == "y":
                run_historical_pipeline()
                run_live_pipeline()
            run_both_analysis()
        else:
            print("  Enter H, L, B, or Q.")


if __name__ == "__main__":
    main()
