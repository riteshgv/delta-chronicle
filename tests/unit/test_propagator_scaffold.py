
"""Unit tests for ForgetPropagator scaffold — no Spark needed."""
import pytest
from unittest.mock import MagicMock
from delta_chronicle.core.graph import ChronicleGraph
from delta_chronicle.gdpr.propagator import ForgetPropagator


@pytest.fixture
def mock_graph():
    cg = (
        ChronicleGraph(spark=MagicMock())
        .register("bronze.trips",    path="/tmp/b", primary_key="vendor_id")
        .register("silver.enriched", path="/tmp/s", primary_key="vendor_id",
                  upstream=["bronze.trips"])
        .register("gold.revenue",    path="/tmp/g", primary_key="vendor_id",
                  upstream=["silver.enriched"])
    )
    return cg


class TestTopologicalOrder:
    def test_bronze_before_silver(self, mock_graph):
        order = ForgetPropagator(mock_graph)._topological_order()
        assert order.index("bronze.trips") < order.index("silver.enriched")

    def test_silver_before_gold(self, mock_graph):
        order = ForgetPropagator(mock_graph)._topological_order()
        assert order.index("silver.enriched") < order.index("gold.revenue")

    def test_all_tables_present(self, mock_graph):
        order = ForgetPropagator(mock_graph)._topological_order()
        assert len(order) == 3

    def test_root_is_first(self, mock_graph):
        order = ForgetPropagator(mock_graph)._topological_order()
        assert order[0] == "bronze.trips"

    def test_leaf_is_last(self, mock_graph):
        order = ForgetPropagator(mock_graph)._topological_order()
        assert order[-1] == "gold.revenue"


class TestForgetPropagatorInit:
    def test_raises_without_spark(self):
        cg = ChronicleGraph(spark=None)
        cg.register("bronze.t", path="/tmp/t", primary_key="id")
        with pytest.raises(ValueError, match="requires a Spark session"):
            ForgetPropagator(cg)

    def test_initializes_with_spark(self, mock_graph):
        fp = ForgetPropagator(mock_graph)
        assert fp.graph is mock_graph