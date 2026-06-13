"""
CausalityTracer: Walk backwards through the DAG to find root cause.

Week 1: Structural trace (uses DESCRIBE HISTORY + DAG topology)
Week 3: Full CDF algorithm (cross-joins CDF streams by timestamp)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, TYPE_CHECKING
import logging

if TYPE_CHECKING:
    from delta_chronicle.core.graph import ChronicleGraph, TableNode

logger = logging.getLogger(__name__)


@dataclass
class CausalLink:
    """One step in the causal chain."""

    table_name: str
    layer: str
    version: int
    timestamp: str
    operation: str
    affected_rows: int
    job_description: str
    is_root_cause: bool = False
    is_symptom: bool = False


@dataclass
class TraceResult:
    """Full result of a causality trace."""

    symptom_table: str
    symptom_filter: str
    causal_chain: List[CausalLink] = field(default_factory=list)
    root_cause: Optional[CausalLink] = None
    trace_mode: str = "structural"  # "structural" | "cdf" (Week 3)

    def show_chain(self):
        print("\n" + "=" * 55)
        print("  🔍 delta-chronicle — Causality Trace")
        print("=" * 55)
        print(f"  Symptom : {self.symptom_filter}")
        print(f"  In table: {self.symptom_table}")
        print(f"  Mode    : {self.trace_mode}")
        if self.trace_mode == "structural":
            print("  Note    : Structural trace (Week 1 stub).")
            print("            Full CDF row-level algo arrives Week 3.")
        print()

        for i, link in enumerate(reversed(self.causal_chain)):
            if link.is_root_cause:
                prefix = "🔴 [ROOT CAUSE]"
            elif link.is_symptom:
                prefix = "🟡 [SYMPTOM]   "
            else:
                prefix = f"   [HOP {i}]     "

            print(f"  {prefix}  {link.table_name}  [{link.layer.upper()}]")
            print(f"             Version   : {link.version}")
            print(f"             Timestamp : {link.timestamp}")
            print(f"             Operation : {link.operation}")
            print(f"             Rows      : {link.affected_rows}")
            print()

        if self.root_cause:
            print(
                f"  ✅ Root cause: {self.root_cause.table_name} "
                f"at version {self.root_cause.version}"
            )
        else:
            print("  ⚠️  Root cause undetermined — expand suspect_window")
        print("=" * 55)


class CausalityTracer:
    """
    Traces backwards through a ChronicleGraph DAG to identify
    the root cause of a data anomaly.

    Week 1 implementation: structural trace
    - Reads DESCRIBE HISTORY for each table
    - Walks upstream by DAG topology
    - Correctly identifies root as the table with no upstream
    - Does NOT yet correlate rows across tables (Week 3)
    """

    def __init__(self, graph: "ChronicleGraph"):
        self.graph = graph

    def trace(
        self, table: str, filter_expr: str, suspect_window: Optional[tuple] = None
    ) -> TraceResult:
        """
        Trace causality from a symptomatic downstream table back to source.

        Args:
            table: Downstream table showing the anomaly
            filter_expr: Filter identifying the bad rows e.g. "vendor_id = 1"
            suspect_window: Optional (start_ts, end_ts) to narrow search

        Returns:
            TraceResult with causal chain from root to symptom
        """
        logger.info(f"Starting trace: {table} | filter: {filter_expr}")
        node = self.graph.get_node(table)

        chain: List[CausalLink] = []
        self._collect_chain(node, chain, is_symptom_table=True)

        root = next((c for c in chain if c.is_root_cause), None)

        return TraceResult(
            symptom_table=table,
            symptom_filter=filter_expr,
            causal_chain=chain,
            root_cause=root,
            trace_mode="structural",
        )

    def _collect_chain(
        self, node: "TableNode", chain: List[CausalLink], is_symptom_table: bool = False
    ):
        """Recursive upstream collection."""
        link = self._build_link(node)
        link.is_symptom = is_symptom_table
        link.is_root_cause = len(node.upstream) == 0
        chain.append(link)

        for upstream_name in node.upstream:
            upstream_node = self.graph.get_node(upstream_name)
            self._collect_chain(upstream_node, chain, is_symptom_table=False)

    def _build_link(self, node: "TableNode") -> CausalLink:
        """Build a CausalLink from a TableNode, reading Delta history if Spark available."""
        version, timestamp, operation, rows, job = 0, "unknown", "WRITE", 0, "unknown"

        if self.graph.spark:
            try:
                from delta_chronicle.core._delta_utils import get_table_history

                history = get_table_history(self.graph.spark, node.path)
                if history:
                    latest = history[0]
                    version = latest.get("version", 0)
                    timestamp = str(latest.get("timestamp", "unknown"))
                    operation = latest.get("operation", "WRITE")
                    metrics = latest.get("operationMetrics") or {}
                    rows = int(metrics.get("numOutputRows", 0))
                    params = latest.get("operationParameters") or {}
                    job = params.get("description", "No description")
            except Exception as e:
                logger.warning(f"Could not read history for {node.name}: {e}")

        return CausalLink(
            table_name=node.name,
            layer=node.layer,
            version=version,
            timestamp=timestamp,
            operation=operation,
            affected_rows=rows,
            job_description=job,
        )
