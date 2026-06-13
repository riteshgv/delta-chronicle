"""
Shared fixtures for all integration tests.
One Spark session per test run (session scope = fast).
Fresh Delta tables per test (function scope = isolated).
"""
import pytest
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    """
    Single Spark session reused across all integration tests.
    Session scope means Spark starts once and stays running.
    This saves ~2 min vs starting per test.
    """
    builder = (
        SparkSession.builder
        .appName("delta-chronicle-integration-tests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .master("local[2]")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="function")
def simple_delta_table(spark, tmp_path):
    """
    A minimal Delta table with 3 rows and CDF enabled.
    Returns (spark, path_string).
    Fresh directory every test so no cross-test pollution.
    """
    path = str(tmp_path / "simple_table")
    data = [(1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Charlie", 150.0)]
    df = spark.createDataFrame(data, ["id", "name", "amount"])
    (df.write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .mode("overwrite")
     .save(path))
    return spark, path


@pytest.fixture(scope="function")
def three_layer_delta(spark, tmp_path):
    """
    Full bronze/silver/gold Delta pipeline in a tmp directory.
    Returns dict with spark and paths for all 3 tables.

    Schema:
      bronze: trip_id, vendor_id, fare_amount, pickup_date
      silver: same + fare_category column
      gold:   vendor_id, total_fare, trip_count
    """
    from pyspark.sql import functions as F

    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    gold_path   = str(tmp_path / "gold")

    # Bronze — raw data
    bronze_data = [
        (1, "vendor_A", 10.0, "2023-01-15"),
        (2, "vendor_A", 20.0, "2023-01-15"),
        (3, "vendor_B", 15.0, "2023-01-15"),
        (4, "vendor_B",  5.0, "2023-01-15"),
    ]
    bronze_df = spark.createDataFrame(
        bronze_data, ["trip_id", "vendor_id", "fare_amount", "pickup_date"]
    )
    (bronze_df.write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .mode("overwrite").save(bronze_path))

    # Silver — validated + enriched
    silver_df = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col("fare_amount") > 0)
        .withColumn("fare_category",
            F.when(F.col("fare_amount") > 12, "high").otherwise("low"))
    )
    (silver_df.write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .mode("overwrite").save(silver_path))

    # Gold — aggregated
    gold_df = (
        spark.read.format("delta").load(silver_path)
        .groupBy("vendor_id")
        .agg(
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.count("trip_id").alias("trip_count")
        )
    )
    (gold_df.write.format("delta")
     .option("delta.enableChangeDataFeed", "true")
     .mode("overwrite").save(gold_path))

    return {
        "spark":       spark,
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "gold_path":   gold_path,
    }


@pytest.fixture(scope="function")
def corrupted_three_layer(three_layer_delta):
    """
    Takes three_layer_delta and injects a corruption into bronze.
    vendor_A trip_id=1 fare goes from 10.0 to 999.0.
    Returns same dict with paths -- tables are now at version 1.
    """
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark       = three_layer_delta["spark"]
    bronze_path = three_layer_delta["bronze_path"]
    silver_path = three_layer_delta["silver_path"]
    gold_path   = three_layer_delta["gold_path"]

    # Corrupt bronze v1
    corrupt = spark.createDataFrame(
        [(1, "vendor_A", 999.0, "2023-01-15")],
        ["trip_id", "vendor_id", "fare_amount", "pickup_date"]
    )
    (DeltaTable.forPath(spark, bronze_path).alias("t")
     .merge(corrupt.alias("c"), "t.trip_id = c.trip_id")
     .whenMatchedUpdateAll()
     .execute())

    # Reprocess silver v1
    from pyspark.sql import functions as F
    silver_df = (
        spark.read.format("delta").load(bronze_path)
        .filter(F.col("fare_amount") > 0)
        .withColumn("fare_category",
            F.when(F.col("fare_amount") > 12, "high").otherwise("low"))
    )
    (silver_df.write.format("delta")
     .mode("overwrite").save(silver_path))

    # Reprocess gold v1
    gold_df = (
        spark.read.format("delta").load(silver_path)
        .groupBy("vendor_id")
        .agg(
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.count("trip_id").alias("trip_count")
        )
    )
    (gold_df.write.format("delta")
     .mode("overwrite").save(gold_path))

    return three_layer_delta