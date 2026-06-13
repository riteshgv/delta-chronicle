"""Internal Delta Lake utility helpers."""
import logging
logger = logging.getLogger(__name__)


def get_table_history(spark, path: str) -> list:
    """Return Delta table history as list of dicts."""
    try:
        from delta.tables import DeltaTable
        dt = DeltaTable.forPath(spark, path)
        return [row.asDict() for row in dt.history().collect()]
    except Exception as e:
        logger.warning(f"Could not read history for {path}: {e}")
        return []


def get_table_at_version(spark, path: str, version: int):
    """Read a Delta table at a specific version."""
    return (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .load(path)
    )


def get_table_changes(spark, path: str, start_version: int, end_version: int):
    """
    Read Change Data Feed between two versions.
    Requires: delta.enableChangeDataFeed = true on the table.
    """
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .option("endingVersion", end_version)
        .load(path)
    )


def enable_cdf_on_table(spark, path: str):
    """Enable Change Data Feed on an existing Delta table."""
    spark.sql(f"""
        ALTER TABLE delta.`{path}`
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    logger.info(f"CDF enabled on: {path}")