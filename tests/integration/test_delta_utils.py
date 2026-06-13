"""Integration tests for _delta_utils helpers."""
import pytest
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from delta_chronicle.core._delta_utils import (
    get_table_history,
    get_table_at_version,
    get_table_changes,
)


class TestGetTableHistory:

    def test_returns_list_of_dicts(self, simple_delta_table):
        spark, path = simple_delta_table
        history = get_table_history(spark, path)
        assert isinstance(history, list)
        assert len(history) >= 1
        assert isinstance(history[0], dict)

    def test_history_has_required_keys(self, simple_delta_table):
        spark, path = simple_delta_table
        history = get_table_history(spark, path)
        h = history[0]
        assert "version" in h
        assert "timestamp" in h
        assert "operation" in h

    def test_initial_version_is_zero(self, simple_delta_table):
        spark, path = simple_delta_table
        history = get_table_history(spark, path)
        versions = [h["version"] for h in history]
        assert 0 in versions

    def test_invalid_path_returns_empty(self, spark):
        history = get_table_history(spark, "/tmp/does_not_exist_xyz")
        assert history == []


class TestGetTableAtVersion:

    def test_version_zero_has_original_data(self, simple_delta_table):
        spark, path = simple_delta_table
        df = get_table_at_version(spark, path, 0)
        assert df.count() == 3

    def test_version_zero_has_correct_schema(self, simple_delta_table):
        spark, path = simple_delta_table
        df = get_table_at_version(spark, path, 0)
        assert "id" in df.columns
        assert "name" in df.columns
        assert "amount" in df.columns


class TestGetTableChanges:

    def test_cdf_readable_after_update(self, simple_delta_table):
        spark, path = simple_delta_table

        # Write a second version
        update = spark.createDataFrame(
            [(1, "Alice", 999.0)], ["id", "name", "amount"]
        )
        (DeltaTable.forPath(spark, path).alias("t")
         .merge(update.alias("u"), "t.id = u.id")
         .whenMatchedUpdateAll()
         .execute())

        cdf = get_table_changes(spark, path, 1, 1)
        assert cdf.count() > 0

    def test_cdf_has_commit_timestamp(self, simple_delta_table):
        spark, path = simple_delta_table

        update = spark.createDataFrame(
            [(2, "Bob", 888.0)], ["id", "name", "amount"]
        )
        (DeltaTable.forPath(spark, path).alias("t")
         .merge(update.alias("u"), "t.id = u.id")
         .whenMatchedUpdateAll()
         .execute())

        cdf = get_table_changes(spark, path, 1, 1)
        assert "_commit_timestamp" in cdf.columns

    def test_cdf_change_types_include_postimage(self, simple_delta_table):
        spark, path = simple_delta_table

        update = spark.createDataFrame(
            [(3, "Charlie", 777.0)], ["id", "name", "amount"]
        )
        (DeltaTable.forPath(spark, path).alias("t")
         .merge(update.alias("u"), "t.id = u.id")
         .whenMatchedUpdateAll()
         .execute())

        cdf = get_table_changes(spark, path, 1, 1)
        change_types = [r[0] for r in
                        cdf.select("_change_type").distinct().collect()]
        assert "update_postimage" in change_types