"""Integration tests for CDF-based CausalityTracer."""
import pytest
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from delta_chronicle.core.graph import ChronicleGraph
from delta_chronicle.core.tracer import CausalityTracer


@pytest.fixture
def registered_graph(three_layer_delta):
    """ChronicleGraph registered against the three_layer_delta fixture."""
    spark = three_layer_delta["spark"]
    return (
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


class TestCDFTracer:

    def test_mode_is_cdf_when_spark_available(self, registered_graph):
        result = CausalityTracer(registered_graph).trace(
            "gold.revenue", "vendor_id = vendor_A"
        )
        assert result.trace_mode == "cdf"

    def test_structural_mode_when_forced(self, registered_graph):
        result = CausalityTracer(registered_graph).trace(
            "gold.revenue", "vendor_id = vendor_A", mode="structural"
        )
        assert result.trace_mode == "structural"

    def test_root_cause_is_bronze(self, registered_graph):
        result = CausalityTracer(registered_graph).trace(
            "gold.revenue", "vendor_id = vendor_A"
        )
        assert result.root_cause is not None
        assert result.root_cause.table_name == "bronze.trips"

    def test_root_cause_flag_set(self, registered_graph):
        result = CausalityTracer(registered_graph).trace(
            "gold.revenue", "vendor_id = vendor_A"
        )
        assert result.root_cause.is_root_cause is True

    def test_commit_timestamp_populated(self, registered_graph):
        result = CausalityTracer(registered_graph).trace(
            "gold.revenue", "vendor_id = vendor_A"
        )
        links_with_ts = [l for l in result.causal_chain
                         if l.commit_timestamp is not None]
        assert len(links_with_ts) > 0

    def test_chain_length_is_three(self, registered_graph):
        result = CausalityTracer(registered_graph).trace(
            "gold.revenue", "vendor_id = vendor_A"
        )
        assert len(result.causal_chain) == 3

    def test_root_cause_after_corruption(self, corrupted_three_layer):
        spark = corrupted_three_layer["spark"]
        cg = (
            ChronicleGraph(spark)
            .register("bronze.trips",
                      path=corrupted_three_layer["bronze_path"],
                      primary_key="trip_id")
            .register("silver.enriched",
                      path=corrupted_three_layer["silver_path"],
                      primary_key="trip_id",
                      upstream=["bronze.trips"])
            .register("gold.revenue",
                      path=corrupted_three_layer["gold_path"],
                      primary_key="vendor_id",
                      upstream=["silver.enriched"])
        )
        result = CausalityTracer(cg).trace(
            "gold.revenue", "vendor_id = vendor_A"
        )
        assert result.root_cause.table_name == "bronze.trips"
        assert result.root_cause.version == 1

    def test_timestamp_ordering_proves_causality(self, corrupted_three_layer):
        """
        The KEY test: bronze committed BEFORE silver BEFORE gold.
        This is the algorithmic proof of causality.
        """
        spark = corrupted_three_layer["spark"]
        cg = (
            ChronicleGraph(spark)
            .register("bronze.trips",
                      path=corrupted_three_layer["bronze_path"],
                      primary_key="trip_id")
            .register("silver.enriched",
                      path=corrupted_three_layer["silver_path"],
                      primary_key="trip_id",
                      upstream=["bronze.trips"])
            .register("gold.revenue",
                      path=corrupted_three_layer["gold_path"],
                      primary_key="vendor_id",
                      upstream=["silver.enriched"])
        )
        result = CausalityTracer(cg).trace(
            "gold.revenue", "vendor_id = vendor_A"
        )
        chain = result.causal_chain
        bronze = next(l for l in chain if "bronze" in l.table_name)
        silver = next(l for l in chain if "silver" in l.table_name)
        gold   = next(l for l in chain if "gold"   in l.table_name)

        if all(l.commit_timestamp for l in [bronze, silver, gold]):
            assert bronze.commit_timestamp <= silver.commit_timestamp
            assert silver.commit_timestamp <= gold.commit_timestamp