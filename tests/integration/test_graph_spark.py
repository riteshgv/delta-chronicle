"""Integration tests for ChronicleGraph with real Delta tables."""
import pytest
from delta_chronicle.core.graph import ChronicleGraph


class TestChronicleGraphWithDelta:

    def test_register_real_paths(self, three_layer_delta):
        spark = three_layer_delta["spark"]
        cg = (
            ChronicleGraph(spark)
            .register("bronze.trips",
                      path=three_layer_delta["bronze_path"],
                      primary_key="trip_id")
            .register("silver.enriched",
                      path=three_layer_delta["silver_path"],
                      primary_key="trip_id",
                      upstream=["bronze.trips"])
            .register("gold.revenue",
                      path=three_layer_delta["gold_path"],
                      primary_key="vendor_id",
                      upstream=["silver.enriched"])
        )
        assert len(cg) == 3
        assert cg.validate() == []

    def test_time_travel_version_zero(self, three_layer_delta):
        spark = three_layer_delta["spark"]
        df = (spark.read.format("delta")
              .option("versionAsOf", 0)
              .load(three_layer_delta["bronze_path"]))
        assert df.count() == 4

    def test_delta_history_readable(self, three_layer_delta):
        spark = three_layer_delta["spark"]
        from delta_chronicle.core._delta_utils import get_table_history
        history = get_table_history(spark, three_layer_delta["bronze_path"])
        assert len(history) >= 1
        assert history[0]["version"] == 0

    def test_blast_radius_with_real_row_counts(self, three_layer_delta):
        spark = three_layer_delta["spark"]
        from delta_chronicle.core.blast_radius import BlastRadiusAnalyzer
        cg = (
            ChronicleGraph(spark)
            .register("bronze.trips",
                      path=three_layer_delta["bronze_path"],
                      primary_key="trip_id")
            .register("silver.enriched",
                      path=three_layer_delta["silver_path"],
                      primary_key="trip_id",
                      upstream=["bronze.trips"])
            .register("gold.revenue",
                      path=three_layer_delta["gold_path"],
                      primary_key="vendor_id",
                      upstream=["silver.enriched"])
        )
        result = BlastRadiusAnalyzer(cg).analyze("bronze.trips", version=0)
        assert len(result.tainted_tables) == 2
        silver = next(t for t in result.tainted_tables
                      if "silver" in t.table_name)
        assert silver.estimated_rows == 4
        gold = next(t for t in result.tainted_tables
                    if "gold" in t.table_name)
        assert gold.estimated_rows == 2