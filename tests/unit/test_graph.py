"""Unit tests for ChronicleGraph — zero Spark required."""
import pytest
from delta_chronicle.core.graph import ChronicleGraph, TableNode


@pytest.fixture
def taxi_graph():
    cg = ChronicleGraph(spark=None)
    (cg
     .register("bronze.taxi_trips",
               path="/tmp/bronze/taxi_trips",
               primary_key="trip_id",
               description="Raw taxi ingestion")
     .register("silver.trip_enriched",
               path="/tmp/silver/trip_enriched",
               primary_key="trip_id",
               upstream=["bronze.taxi_trips"],
               description="Cleaned trips")
     .register("gold.driver_revenue",
               path="/tmp/gold/driver_revenue",
               primary_key="vendor_id",
               upstream=["silver.trip_enriched"],
               description="Daily revenue per vendor")
     .register("gold.zone_heatmap",
               path="/tmp/gold/zone_heatmap",
               primary_key="zone_id",
               upstream=["silver.trip_enriched"],
               description="Zone pickup counts")
    )
    return cg


class TestRegistration:
    def test_register_single_table(self):
        cg = ChronicleGraph(spark=None)
        cg.register("bronze.test", path="/tmp/t", primary_key="id")
        assert "bronze.test" in cg.get_all_nodes()

    def test_fluent_chaining_returns_self(self):
        cg = ChronicleGraph(spark=None)
        result = cg.register("bronze.a", path="/tmp/a", primary_key="id")
        assert result is cg

    def test_four_tables_registered(self, taxi_graph):
        assert len(taxi_graph) == 4

    def test_unknown_table_raises_value_error(self, taxi_graph):
        with pytest.raises(ValueError, match="not registered"):
            taxi_graph.get_node("gold.does_not_exist")


class TestLayerDetection:
    def test_bronze_layer_detected(self, taxi_graph):
        assert taxi_graph.get_node("bronze.taxi_trips").layer == "bronze"

    def test_silver_layer_detected(self, taxi_graph):
        assert taxi_graph.get_node("silver.trip_enriched").layer == "silver"

    def test_gold_layer_detected(self, taxi_graph):
        assert taxi_graph.get_node("gold.driver_revenue").layer == "gold"

    def test_unknown_layer_for_non_standard_name(self):
        cg = ChronicleGraph(spark=None)
        cg.register("my_custom_table", path="/tmp/x", primary_key="id")
        assert cg.get_node("my_custom_table").layer == "unknown"


class TestDAGTraversal:
    def test_get_downstream_from_bronze(self, taxi_graph):
        downstream = taxi_graph.get_downstream("bronze.taxi_trips")
        assert "silver.trip_enriched" in downstream

    def test_get_downstream_from_silver_has_two_gold(self, taxi_graph):
        downstream = taxi_graph.get_downstream("silver.trip_enriched")
        assert "gold.driver_revenue" in downstream
        assert "gold.zone_heatmap" in downstream

    def test_leaf_table_has_no_downstream(self, taxi_graph):
        downstream = taxi_graph.get_downstream("gold.driver_revenue")
        assert downstream == []

    def test_get_upstream(self, taxi_graph):
        upstream = taxi_graph.get_upstream("silver.trip_enriched")
        assert "bronze.taxi_trips" in upstream

    def test_root_table_has_no_upstream(self, taxi_graph):
        bronze = taxi_graph.get_node("bronze.taxi_trips")
        assert bronze.upstream == []

    def test_get_root_tables(self, taxi_graph):
        roots = taxi_graph.get_root_tables()
        assert roots == ["bronze.taxi_trips"]

    def test_get_leaf_tables(self, taxi_graph):
        leaves = taxi_graph.get_leaf_tables()
        assert "gold.driver_revenue" in leaves
        assert "gold.zone_heatmap" in leaves


class TestValidation:
    def test_empty_graph_has_warning(self):
        cg = ChronicleGraph(spark=None)
        warnings = cg.validate()
        assert len(warnings) > 0

    def test_valid_graph_has_no_warnings(self, taxi_graph):
        warnings = taxi_graph.validate()
        assert warnings == []

    def test_summary_contains_all_layers(self, taxi_graph):
        summary = taxi_graph.summary()
        assert "BRONZE" in summary
        assert "SILVER" in summary
        assert "GOLD" in summary