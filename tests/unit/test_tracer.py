"""Unit tests for CausalityTracer — structural mode (no Spark)."""
import pytest
from delta_chronicle.core.graph import ChronicleGraph
from delta_chronicle.core.tracer import CausalityTracer


@pytest.fixture
def simple_graph():
    cg = ChronicleGraph(spark=None)
    (cg
     .register("bronze.trips", path="/tmp/b", primary_key="trip_id")
     .register("silver.enriched", path="/tmp/s",
               primary_key="trip_id", upstream=["bronze.trips"])
     .register("gold.revenue", path="/tmp/g",
               primary_key="vendor_id", upstream=["silver.enriched"])
    )
    return cg


class TestCausalityTracer:
    def test_trace_returns_result(self, simple_graph):
        result = CausalityTracer(simple_graph).trace(
            "gold.revenue", "vendor_id = 1"
        )
        assert result is not None

    def test_causal_chain_not_empty(self, simple_graph):
        result = CausalityTracer(simple_graph).trace(
            "gold.revenue", "vendor_id = 1"
        )
        assert len(result.causal_chain) > 0

    def test_root_cause_is_bronze(self, simple_graph):
        result = CausalityTracer(simple_graph).trace(
            "gold.revenue", "vendor_id = 1"
        )
        assert result.root_cause is not None
        assert result.root_cause.table_name == "bronze.trips"

    def test_trace_mode_is_structural(self, simple_graph):
        result = CausalityTracer(simple_graph).trace(
            "gold.revenue", "vendor_id = 1"
        )
        assert result.trace_mode == "structural"

    def test_symptom_table_stored(self, simple_graph):
        result = CausalityTracer(simple_graph).trace(
            "gold.revenue", "vendor_id = 1"
        )
        assert result.symptom_table == "gold.revenue"

    def test_filter_stored(self, simple_graph):
        result = CausalityTracer(simple_graph).trace(
            "gold.revenue", "vendor_id = 1"
        )
        assert result.symptom_filter == "vendor_id = 1"