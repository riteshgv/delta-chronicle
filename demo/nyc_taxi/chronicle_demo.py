"""
delta-chronicle Demo — The Payoff.

Story:
  BI alert fired: Vendor 1 revenue jumped ~99% overnight.
  3 lines of delta-chronicle find the root cause.
"""

import sys
sys.path.insert(0, "/app")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta_chronicle import ChronicleGraph, CausalityTracer, BlastRadiusAnalyzer

BASE     = "/app/demo/nyc_taxi"
BRONZE   = f"{BASE}/delta/bronze/taxi_trips"
SILVER   = f"{BASE}/delta/silver/trip_enriched"
GOLD_REV = f"{BASE}/delta/gold/driver_revenue"
GOLD_HEAT= f"{BASE}/delta/gold/zone_heatmap"

builder = (
    SparkSession.builder.appName("delta-chronicle-demo")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .master("local[*]")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 55)
print("  delta-chronicle — Live Demo")
print("=" * 55)

# ── 1. Register the DAG ─────────────────────────────────────
print("\n📋 Step 1: Register lakehouse DAG")
cg = (
    ChronicleGraph(spark)
    .register("bronze.taxi_trips", path=BRONZE,
              primary_key="trip_id",
              description="Raw dispatch system ingestion")
    .register("silver.trip_enriched", path=SILVER,
              primary_key="trip_id",
              upstream=["bronze.taxi_trips"],
              description="Validated and enriched trips")
    .register("gold.driver_revenue", path=GOLD_REV,
              primary_key="vendor_id",
              upstream=["silver.trip_enriched"],
              description="Daily revenue per vendor")
    .register("gold.zone_heatmap", path=GOLD_HEAT,
              primary_key="pickup_zone_id",
              upstream=["silver.trip_enriched"],
              description="Pickup counts per zone")
)
print(cg.summary())

# ── 2. Trace the root cause ──────────────────────────────────
print("\n🔍 Step 2: Trace causality (symptom: vendor_id=1 revenue spike)")
result = CausalityTracer(cg).trace(
    table="gold.driver_revenue",
    filter_expr="vendor_id = 1"
)
result.show_chain()

# ── 3. Blast radius ──────────────────────────────────────────
print("\n💥 Step 3: Blast radius — what else is tainted?")
impact = BlastRadiusAnalyzer(cg).analyze("bronze.taxi_trips", version=1)
impact.show()

# ── 4. Time travel proof ─────────────────────────────────────
print("\n🕐 Step 4: Time travel comparison — before vs after")
v0 = (spark.read.format("delta").option("versionAsOf", 0).load(GOLD_REV)
      .groupBy("vendor_id").agg(F.sum("total_revenue").alias("rev_v0")))
v1 = (spark.read.format("delta").option("versionAsOf", 1).load(GOLD_REV)
      .groupBy("vendor_id").agg(F.sum("total_revenue").alias("rev_v1")))

(v0.join(v1, "vendor_id")
   .selectExpr("vendor_id",
               "round(rev_v0,2) as before_anomaly",
               "round(rev_v1,2) as after_anomaly",
               "round((rev_v1-rev_v0)/rev_v0*100,1) as pct_change")
   .orderBy("vendor_id")
   .show())

print("=" * 55)
print("  ✅ Root cause: bronze.taxi_trips v1")
print("  ✅ Blast radius: silver + 2 gold tables tainted")
print("  ✅ Revenue delta confirmed via time travel")
print("=" * 55)
spark.stop()