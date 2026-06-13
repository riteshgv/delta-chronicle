"""
CausalityTracer: Cross-table temporal causality via Delta CDF.

Two modes:
  structural - graph topology only (Week 1 fallback, no Spark needed)
  cdf        - real CDF timestamp correlation (Week 2, needs Spark)
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
    commit_timestamp: Optional[str] = None
    is_root_cause: bool = False
    is_symptom: bool = False


@dataclass
class TraceResult:
    """Full result of a causality trace."""
    symptom_table: str
    symptom_filter: str
    causal_chain: List[CausalLink] = field(default_factory=list)
    root_cause: Optional[CausalLink] = None
    trace_mode: str = "structural"

    def show_chain(self):
        print("\n" + "=" * 58)
        print("  delta-chronicle  Causality Trace")
        print("=" * 58)
        print(f"  Symptom : {self.symptom_filter}")
        print(f"  Table   : {self.symptom_table}")
        print(f"  Mode    : {self.trace_mode}")
        if self.trace_mode == "structural":
            print("  Note    : structural mode (no Spark). Week 1 fallback.")
        print()

        for link in reversed(self.causal_chain):
            if link.is_root_cause:
                tag = "ROOT CAUSE"
            elif link.is_symptom:
                tag = "SYMPTOM   "
            else:
                tag = "HOP       "

            print(f"  [{tag}]  {link.table_name}  [{link.layer.upper()}]")
            print(f"               Version   : {link.version}")
            print(f"               Timestamp : {link.timestamp}")
            print(f"               Operation : {link.operation}")
            print(f"               Rows      : {link.affected_rows}")
            if link.commit_timestamp:
                print(f"               CDF ts    : {link.commit_timestamp}")
            print()

        if self.root_cause:
            print(f"  Root cause : {self.root_cause.table_name} "
                  f"v{self.root_cause.version}")
            if self.root_cause.commit_timestamp:
                print(f"  Committed  : {self.root_cause.commit_timestamp}")
        else:
            print("  Root cause : undetermined")
        print("=" * 58)


class CausalityTracer:
    """
    Traces backwards through a ChronicleGraph to find root cause.

    Usage:
        tracer = CausalityTracer(cg)
        result = tracer.trace("gold.revenue", "vendor_id = 1")
        result.show_chain()
    """

    def __init__(self, graph: "ChronicleGraph"):
        self.graph = graph

    def trace(
        self,
        table: str,
        filter_expr: str,
        suspect_window: Optional[tuple] = None,
        mode: str = "auto"
    ) -> TraceResult:
        """
        Trace causality from a symptomatic downstream table.

        Args:
            table:          The table showing the anomaly
            filter_expr:    Filter identifying bad rows e.g. "vendor_id = 1"
            suspect_window: Optional (start_ts, end_ts) to narrow CDF window
            mode:           "auto" | "cdf" | "structural"
                            auto = cdf if Spark available, else structural

        Returns:
            TraceResult with full causal chain
        """
        use_cdf = (
            mode == "cdf" or
            (mode == "auto" and self.graph.spark is not None)
        )

        if use_cdf:
            try:
                return self._trace_cdf(table, filter_expr, suspect_window)
            except Exception as e:
                logger.warning(
                    f"CDF trace failed: {e}. Falling back to structural."
                )

        return self._trace_structural(table, filter_expr)

    # ── CDF Algorithm ───────────────────────────────────────────────────────

    def _trace_cdf(
        self,
        table: str,
        filter_expr: str,
        suspect_window: Optional[tuple]
    ) -> TraceResult:
        """
        Real CDF-based trace using commit timestamp correlation.
        """
        node = self.graph.get_node(table)
        chain: List[CausalLink] = []

        # Anchor timestamp = when the symptom table last committed
        anchor_ts = self._get_latest_commit_ts(node)
        if anchor_ts is None:
            logger.warning(
                f"Could not get commit timestamp for {table}. "
                "Falling back to structural."
            )
            return self._trace_structural(table, filter_expr)

        self._cdf_walk_upstream(
            node=node,
            anchor_ts=anchor_ts,
            chain=chain,
            is_symptom=True,
            suspect_window=suspect_window
        )

        root = next((c for c in chain if c.is_root_cause), None)
        return TraceResult(
            symptom_table=table,
            symptom_filter=filter_expr,
            causal_chain=chain,
            root_cause=root,
            trace_mode="cdf"
        )

    def _cdf_walk_upstream(
        self,
        node,
        anchor_ts,
        chain: List[CausalLink],
        is_symptom: bool,
        suspect_window: Optional[tuple]
    ):
        """
        Recursively walk upstream, correlating CDF timestamps.

        For each table:
        1. Read its DESCRIBE HISTORY
        2. Find the version whose commit_timestamp <= anchor_ts
           (this is the version that fed the downstream table)
        3. Read CDF for that version to get exact commit_timestamp
        4. Use that as the anchor_ts for the next upstream table
        """
        from delta_chronicle.core._delta_utils import get_table_history

        history = get_table_history(self.graph.spark, node.path)
        if not history:
            logger.warning(f"No history found for {node.name}")
            return

        # Find the version that committed BEFORE or AT anchor_ts
        matched = None
        anchor_str = str(anchor_ts)

        for h in history:
            h_ts = h.get("timestamp")
            if h_ts is not None and str(h_ts) <= anchor_str:
                matched = h
                break

        # If no match found (all versions after anchor), use earliest
        if matched is None:
            matched = history[-1]
            logger.warning(
                f"No version of {node.name} found before {anchor_ts}. "
                f"Using earliest version {matched['version']}."
            )

        version   = matched.get("version", 0)
        timestamp = str(matched.get("timestamp", "unknown"))
        operation = matched.get("operation", "WRITE")
        metrics   = matched.get("operationMetrics") or {}
        rows      = int(metrics.get("numOutputRows", 0))
        params    = matched.get("operationParameters") or {}
        job_desc  = params.get("description", "")

        # Get actual CDF commit timestamp for this version
        commit_ts = self._get_cdf_commit_ts_for_version(node, version)

        link = CausalLink(
            table_name=node.name,
            layer=node.layer,
            version=version,
            timestamp=timestamp,
            operation=operation,
            affected_rows=rows,
            job_description=job_desc,
            commit_timestamp=str(commit_ts) if commit_ts else timestamp,
            is_root_cause=(len(node.upstream) == 0),
            is_symptom=is_symptom
        )
        chain.append(link)

        # Use this version's commit_ts as anchor for the next upstream
        next_anchor = commit_ts if commit_ts else anchor_ts

        for upstream_name in node.upstream:
            upstream_node = self.graph.get_node(upstream_name)
            self._cdf_walk_upstream(
                node=upstream_node,
                anchor_ts=next_anchor,
                chain=chain,
                is_symptom=False,
                suspect_window=suspect_window
            )

    def _get_latest_commit_ts(self, node):
        """
        Get the most recent commit timestamp for a table.
        Uses DESCRIBE HISTORY first row.
        """
        try:
            from delta.tables import DeltaTable
            row = DeltaTable.forPath(
                self.graph.spark, node.path
            ).history().first()
            return row["timestamp"] if row else None
        except Exception as e:
            logger.warning(f"Cannot get latest ts for {node.name}: {e}")
            return None

    def _get_cdf_commit_ts_for_version(self, node, version: int):
        """
        Read CDF for a specific version and return its _commit_timestamp.
        This is the EXACT timestamp Delta wrote those rows.
        """
        try:
            cdf = (
                self.graph.spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", version)
                .option("endingVersion", version)
                .load(node.path)
            )
            row = cdf.select("_commit_timestamp").first()
            return row[0] if row else None
        except Exception as e:
            logger.warning(
                f"CDF read failed for {node.name} v{version}: {e}"
            )
            return None

    # ── Structural fallback ─────────────────────────────────────────────────

    def _trace_structural(self, table: str, filter_expr: str) -> TraceResult:
        """
        Week 1 structural trace. Used as fallback when CDF fails.
        Finds root cause by graph topology only.
        """
        node = self.graph.get_node(table)
        chain: List[CausalLink] = []
        self._structural_walk(node, chain, is_symptom=True)
        root = next((c for c in chain if c.is_root_cause), None)
        return TraceResult(
            symptom_table=table,
            symptom_filter=filter_expr,
            causal_chain=chain,
            root_cause=root,
            trace_mode="structural"
        )

    def _structural_walk(self, node, chain: List[CausalLink], is_symptom: bool):
        version, ts, op, rows = 0, "unknown", "WRITE", 0
        if self.graph.spark:
            try:
                from delta_chronicle.core._delta_utils import get_table_history
                history = get_table_history(self.graph.spark, node.path)
                if history:
                    h       = history[0]
                    version = h.get("version", 0)
                    ts      = str(h.get("timestamp", "unknown"))
                    op      = h.get("operation", "WRITE")
                    metrics = h.get("operationMetrics") or {}
                    rows    = int(metrics.get("numOutputRows", 0))
            except Exception as e:
                logger.warning(f"History read failed for {node.name}: {e}")

        chain.append(CausalLink(
            table_name=node.name, layer=node.layer,
            version=version, timestamp=ts, operation=op,
            affected_rows=rows, job_description="",
            is_root_cause=(len(node.upstream) == 0),
            is_symptom=is_symptom
        ))
        for up in node.upstream:
            self._structural_walk(self.graph.get_node(up), chain, False)