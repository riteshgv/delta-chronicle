"""
Injects a realistic data corruption into the bronze layer.

Scenario:
  Vendor 1's fare amounts are multiplied by 10x on Jan 15, 2023
  (simulating a decimal point bug in an upstream dispatch system).

This creates bronze version 1, which flows through to
silver version 1 and gold version 1 — all with wrong revenue.

delta-chronicle's job: find this automatically.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

BASE      = "/app/demo/nyc_taxi"
BRONZE    = f"{BASE}/delta/bronze/taxi_trips"
SILVER    = f"{BASE}/delta/silver/trip_enriched"
GOLD_REV  = f"{BASE}/delta/gold/driver_revenue"
GOLD_HEAT = f"{BASE}/delta/gold/zone_heatmap"

sys.path.insert(0, "/app")


def get_spark():
    builder = (
        SparkSession.builder.appName("inject-anomaly")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[*]")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def show_revenue(spark, label):
    print(f"\n  📊 Revenue {label}:")
    (spark.read.format("delta").load(GOLD_REV)
     .groupBy("vendor_id")
     .agg(F.round(F.sum("total_revenue"), 2).alias("total_revenue"))
     .orderBy("vendor_id")
     .show())


if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 55)
    print("  🔴 delta-chronicle — Anomaly Injection")
    print("=" * 55)

    show_revenue(spark, "BEFORE (gold v0)")

    # ── Inject corruption into bronze ──────────────────────────
    print("\n  💉 Corrupting bronze: vendor_id=1 fares × 10 on Jan 15...")
    bronze_df = spark.read.format("delta").load(BRONZE)

    corrupt = (
        bronze_df
        .filter(F.col("vendor_id") == 1)
        .filter(F.to_date("pickup_datetime") == "2023-01-15")
        .withColumn("fare_amount",  F.col("fare_amount")  * 10)
        .withColumn("total_amount", F.col("total_amount") * 10)
        .withColumn("_ingested_at", F.current_timestamp())
    )
    corrupt = corrupt.dropDuplicates(["trip_id"])
    row_count = corrupt.count()

    (DeltaTable.forPath(spark, BRONZE).alias("t")
     .merge(corrupt.alias("c"), "t.trip_id = c.trip_id")
     .whenMatchedUpdate(set={
         "fare_amount":  "c.fare_amount",
         "total_amount": "c.total_amount",
         "_ingested_at": "c._ingested_at"
     })
     .execute())

    bronze_v = DeltaTable.forPath(spark, BRONZE).history().first()["version"]
    print(f"  ✅ Corrupted {row_count:,} rows → bronze now at version {bronze_v}")

    # ── Reprocess silver + gold ────────────────────────────────
    print("\n  🔄 Reprocessing silver and gold from corrupt bronze...")
    from demo.nyc_taxi.pipeline import (
        transform_silver, aggregate_gold_revenue, aggregate_gold_heatmap
    )
    transform_silver(spark)
    aggregate_gold_revenue(spark)
    aggregate_gold_heatmap(spark)

    show_revenue(spark, "AFTER (gold v1) — notice vendor 1 spike ⬆️")

    # ── Show version history ───────────────────────────────────
    print("\n  🕐 Delta version history:")
    for name, path in [("bronze", BRONZE), ("silver", SILVER),
                        ("gold.revenue", GOLD_REV)]:
        v = DeltaTable.forPath(spark, path).history().first()["version"]
        print(f"    {name:<20} is now at version {v}")

    print("\n" + "=" * 55)
    print("  ✅ Crime scene ready. Run chronicle_demo.py next.")
    print("=" * 55)
    spark.stop()