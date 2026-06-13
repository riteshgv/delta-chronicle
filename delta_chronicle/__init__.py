"""
delta-chronicle: Cross-table temporal causality engine for Delta Lake.

Trace data corruption backwards through your lakehouse DAG to find
the root cause table, version, job, and row that started it all.

Usage:
    from delta_chronicle import ChronicleGraph, CausalityTracer, BlastRadiusAnalyzer

    cg = ChronicleGraph(spark)
    cg.register("bronze.trips", path="...", primary_key="trip_id")
    cg.register("gold.revenue", path="...", primary_key="vendor_id",
                upstream=["bronze.trips"])

    result = CausalityTracer(cg).trace("gold.revenue", "vendor_id = 1")
    result.show_chain()
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__license__ = "Apache-2.0"

from delta_chronicle.core.graph import ChronicleGraph
from delta_chronicle.core.tracer import CausalityTracer
from delta_chronicle.core.blast_radius import BlastRadiusAnalyzer

__all__ = ["ChronicleGraph", "CausalityTracer", "BlastRadiusAnalyzer"]