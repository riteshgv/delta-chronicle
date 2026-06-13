"""
CDF Exploration Script.
Run this, read every output section carefully.
This is the data structure the CausalityTracer algorithm uses on Day 8.
"""
import sys
sys.path.insert(0, "/app")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

BASE     = "/app/demo/nyc_taxi/delta"
BRONZE   = f"{BASE}/bronze/taxi_trips"
SILVER   = f"{BASE}/silver/trip_enriched"
GOLD_REV = f"{BASE}/gold/driver_revenue"

builder = (
    SparkSession.builder.appName("explore-cdf")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 55)
print("  CDF Exploration — NYC Taxi Pipeline")
print("=" * 55)

# ── Section 1: Schema of CDF ──────────────────────────────────
print("\n1. CDF schema (what columns CDF adds):")
bronze_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 1)
    .load(BRONZE)
)
cdf_cols = [f.name for f in bronze_cdf.schema.fields]
data_cols = [c for c in cdf_cols if not c.startswith("_")]
meta_cols  = [c for c in cdf_cols if c.startswith("_")]
print(f"  Data columns  : {data_cols}")
print(f"  CDF columns   : {meta_cols}")

# ── Section 2: Change types breakdown ────────────────────────
print("\n2. Bronze CDF change types (v0 to v1):")
print("   v0=original write, v1=corrupt MERGE batch")
bronze_cdf.groupBy("_change_type").count().orderBy("_change_type").show()
# You'll see: update_preimage (original values) + update_postimage (corrupt values)

# ── Section 3: The corrupt rows ──────────────────────────────
print("\n3. Corrupt rows in bronze CDF (vendor_id=1, Jan 15, postimage):")
(bronze_cdf
 .filter(F.col("_change_type") == "update_postimage")
 .filter(F.col("vendor_id") == 1)
 .filter(F.to_date("pickup_datetime") == "2023-01-15")
 .select("trip_id", "vendor_id", "fare_amount",
         "_change_type", "_commit_version", "_commit_timestamp")
 .limit(5)
 .show(truncate=False))

# ── Section 4: Before vs after same trip ─────────────────────
print("\n4. Before vs after for same trip_id (preimage vs postimage):")
from pyspark.sql.window import Window
sample_trip = (bronze_cdf
               .filter(F.col("vendor_id") == 1)
               .filter(F.to_date("pickup_datetime") == "2023-01-15")
               .select("trip_id").first()[0])

(bronze_cdf
 .filter(F.col("trip_id") == sample_trip)
 .select("trip_id", "fare_amount", "_change_type", "_commit_version")
 .show())

# ── Section 5: Silver CDF ────────────────────────────────────
print("\n5. Silver CDF change types (v0 to v1):")
silver_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 1)
    .load(SILVER)
)
silver_cdf.groupBy("_change_type").count().show()

# ── Section 6: Gold CDF ──────────────────────────────────────
print("\n6. Gold CDF change types (v0 to v1):")
gold_cdf = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 1)
    .load(GOLD_REV)
)
gold_cdf.groupBy("_change_type").count().show()

# ── Section 7: THE KEY - Timestamp ordering ───────────────────
print("\n7. COMMIT TIMESTAMPS ACROSS ALL TABLES (this is the key insight):")
print("   Bronze must have committed BEFORE silver BEFORE gold")
print("   This ordering PROVES causal direction.\n")

for name, path in [("bronze", BRONZE), ("silver", SILVER), ("gold", GOLD_REV)]:
    cdf = (spark.read.format("delta")
           .option("readChangeFeed", "true")
           .option("startingVersion", 1)
           .option("endingVersion", 1)
           .load(path))
    row = cdf.select(
        F.min("_commit_timestamp").alias("committed_at")
    ).first()
    print(f"  {name:<15} v1 committed at: {row['committed_at']}")

print("\n  KEY INSIGHT:")
print("  bronze_ts < silver_ts < gold_ts")
print("  The CausalityTracer uses this ordering to build the causal chain.")
print("  If bronze committed first, it CAUSED the downstream changes.")

# ── Section 8: Version history comparison ─────────────────────
print("\n8. DESCRIBE HISTORY for all tables:")
from delta.tables import DeltaTable
for name, path in [("bronze", BRONZE), ("silver", SILVER), ("gold", GOLD_REV)]:
    print(f"\n  {name}:")
    (DeltaTable.forPath(spark, path)
     .history()
     .select("version", "timestamp", "operation", "operationMetrics")
     .show(5, truncate=False))

print("=" * 55)
print("  CDF exploration complete.")
print("  You now understand the data structure for the Day 8 algorithm.")
print("=" * 55)
spark.stop()