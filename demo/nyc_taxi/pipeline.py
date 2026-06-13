"""
NYC Taxi Bronze → Silver → Gold Pipeline.
Demo pipeline for delta-chronicle — runs entirely inside Docker.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

# ── Paths (Docker-internal) ────────────────────────────────────
BASE       = "/app/demo/nyc_taxi"
RAW_DATA   = f"{BASE}/yellow_tripdata_2023-01.parquet"
BRONZE     = f"{BASE}/delta/bronze/taxi_trips"
SILVER     = f"{BASE}/delta/silver/trip_enriched"
GOLD_REV   = f"{BASE}/delta/gold/driver_revenue"
GOLD_HEAT  = f"{BASE}/delta/gold/zone_heatmap"


def get_spark():
    builder = (
        SparkSession.builder
        .appName("delta-chronicle-taxi-pipeline")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[*]")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest_bronze(spark):
    print("\n📥 [BRONZE] Ingesting raw taxi data...")
    df = spark.read.parquet(RAW_DATA)
    bronze = (
        df.select(
            F.col("VendorID").cast("integer").alias("vendor_id"),
            F.col("tpep_pickup_datetime").alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            F.col("passenger_count").cast("integer"),
            F.col("trip_distance").cast("double"),
            F.col("PULocationID").cast("integer").alias("pickup_zone_id"),
            F.col("DOLocationID").cast("integer").alias("dropoff_zone_id"),
            F.col("fare_amount").cast("double"),
            F.col("tip_amount").cast("double"),
            F.col("total_amount").cast("double"),
        )
        .withColumn("trip_id",
            F.concat(
                F.col("vendor_id").cast("string"), F.lit("_"),
                F.unix_timestamp("pickup_datetime").cast("string")
            )
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (bronze.write.format("delta").mode("overwrite")
     .option("delta.enableChangeDataFeed", "true")
     .save(BRONZE))
    count = bronze.count()
    print(f"  ✅ {count:,} rows → {BRONZE}")
    return count


def transform_silver(spark):
    print("\n🔧 [SILVER] Cleaning and validating...")
    bronze = spark.read.format("delta").load(BRONZE)
    silver = (
        bronze
        .filter(F.col("fare_amount").between(0.01, 499.99))
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("vendor_id").isNotNull())
        .filter(F.col("pickup_datetime") >= "2023-01-01")
        .filter(F.col("pickup_datetime") <  "2023-02-01")
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("trip_duration_mins",
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 60)
        .withColumn("fare_per_mile",
            F.when(F.col("trip_distance") > 0,
                   F.round(F.col("fare_amount") / F.col("trip_distance"), 4))
            .otherwise(None))
        .withColumn("_processed_at", F.current_timestamp())
    )
    (silver.write.format("delta").mode("overwrite")
     .option("delta.enableChangeDataFeed", "true")
     .save(SILVER))
    count = silver.count()
    print(f"  ✅ {count:,} rows → {SILVER}")
    return count


def aggregate_gold_revenue(spark):
    print("\n🏆 [GOLD] Aggregating driver revenue...")
    silver = spark.read.format("delta").load(SILVER)
    revenue = (
        silver.groupBy("vendor_id", "pickup_date")
        .agg(
            F.count("trip_id").alias("total_trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_distance"), 4).alias("avg_distance"),
        )
        .withColumn("_computed_at", F.current_timestamp())
    )
    (revenue.write.format("delta").mode("overwrite")
     .option("delta.enableChangeDataFeed", "true")
     .save(GOLD_REV))
    count = revenue.count()
    print(f"  ✅ {count:,} rows → {GOLD_REV}")
    return count


def aggregate_gold_heatmap(spark):
    print("\n🗺️  [GOLD] Aggregating zone heatmap...")
    silver = spark.read.format("delta").load(SILVER)
    heatmap = (
        silver.groupBy("pickup_zone_id", "pickup_date")
        .agg(
            F.count("trip_id").alias("pickup_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.sum("total_amount"), 2).alias("zone_revenue"),
        )
        .withColumn("_computed_at", F.current_timestamp())
    )
    (heatmap.write.format("delta").mode("overwrite")
     .option("delta.enableChangeDataFeed", "true")
     .save(GOLD_HEAT))
    count = heatmap.count()
    print(f"  ✅ {count:,} rows → {GOLD_HEAT}")
    return count


if __name__ == "__main__":
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 55)
    print("  delta-chronicle Demo — NYC Taxi Pipeline")
    print("=" * 55)

    b = ingest_bronze(spark)
    s = transform_silver(spark)
    r = aggregate_gold_revenue(spark)
    h = aggregate_gold_heatmap(spark)

    print("\n" + "=" * 55)
    print("  ✅ Pipeline complete!")
    print(f"  Bronze  : {b:>10,} rows")
    print(f"  Silver  : {s:>10,} rows  ({100*s//b}% passed validation)")
    print(f"  Revenue : {r:>10,} rows")
    print(f"  Heatmap : {h:>10,} rows")
    print("=" * 55)
    spark.stop()