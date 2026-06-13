"""Unit tests for BlastRadiusAnalyzer."""
import pytest
from delta_chronicle.core.graph import ChronicleGraph
from delta_chronicle.core.blast_radius import BlastRadiusAnalyzer


@pytest.fixture
def fanout_graph():
    """Bronze → Silver → Gold1 + Gold2 (fan-out pattern)."""
    cg = ChronicleGraph(spark=None)
    (cg
     .register("bronze.trips", path="/tmp/b", primary_key="trip_id")
     .register("silver.enriched", path="/tmp/s",
               primary_key="trip_id", upstream=["bronze.trips"])
     .register("gold.revenue", path="/tmp/g1",
               primary_key="vendor_id", upstream=["silver.enriched"])
     .register("gold.heatmap", path="/tmp/g2",
               primary_key="zone_id", upstream=["silver.enriched"])
    )
    return cg


class TestBlastRadius:
    def test_bronze_taints_all_downstream(self, fanout_graph):
        result = BlastRadiusAnalyzer(fanout_graph).analyze("bronze.trips", 3)
        names = [t.table_name for t in result.tainted_tables]
        assert "silver.enriched" in names
        assert "gold.revenue" in names
        assert "gold.heatmap" in names

    def test_silver_corruption_skips_bronze(self, fanout_graph):
        result = BlastRadiusAnalyzer(fanout_graph).analyze("silver.enriched", 5)
        names = [t.table_name for t in result.tainted_tables]
        assert "bronze.trips" not in names
        assert "gold.revenue" in names

    def test_leaf_table_has_empty_blast_radius(self, fanout_graph):
        result = BlastRadiusAnalyzer(fanout_graph).analyze("gold.revenue", 2)
        assert result.tainted_tables == []

    def test_distance_from_bronze_to_silver_is_1(self, fanout_graph):
        result = BlastRadiusAnalyzer(fanout_graph).analyze("bronze.trips", 3)
        silver = next(t for t in result.tainted_tables
                      if t.table_name == "silver.enriched")
        assert silver.distance == 1

    def test_distance_from_bronze_to_gold_is_2(self, fanout_graph):
        result = BlastRadiusAnalyzer(fanout_graph).analyze("bronze.trips", 3)
        gold = next(t for t in result.tainted_tables
                    if t.table_name == "gold.revenue")
        assert gold.distance == 2

    def test_source_table_not_in_tainted(self, fanout_graph):
        result = BlastRadiusAnalyzer(fanout_graph).analyze("bronze.trips", 3)
        names = [t.table_name for t in result.tainted_tables]
        assert "bronze.trips" not in names

    def test_result_stores_source_info(self, fanout_graph):
        result = BlastRadiusAnalyzer(fanout_graph).analyze("bronze.trips", 7)
        assert result.source_table == "bronze.trips"
        assert result.source_version == 7