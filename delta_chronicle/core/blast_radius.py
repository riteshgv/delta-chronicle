"""
BlastRadiusAnalyzer: Forward impact analysis.

Given a corrupt upstream table, find all downstream tables
that are now tainted and need recomputation.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from delta_chronicle.core.graph import ChronicleGraph


@dataclass
class TaintedTable:
    table_name: str
    distance: int
    layer: str
    estimated_rows: int = -1
    needs_recompute: bool = True


@dataclass
class BlastRadiusResult:
    source_table: str
    source_version: int
    tainted_tables: List[TaintedTable] = field(default_factory=list)

    def show(self):
        print("\n" + "=" * 55)
        print("  💥 delta-chronicle — Blast Radius Analysis")
        print("=" * 55)
        print(f"  Source  : {self.source_table}")
        print(f"  Version : {self.source_version}")
        print()

        if not self.tainted_tables:
            print("  ✅ No downstream tables affected.")
            print("=" * 55)
            return

        for t in sorted(self.tainted_tables, key=lambda x: x.distance):
            indent = "  " + ("→ " * t.distance)
            rows_str = (
                f"{t.estimated_rows:,} rows"
                if t.estimated_rows >= 0
                else "rows: pending"
            )
            recompute = "⚠️  needs recompute" if t.needs_recompute else "✅ clean"
            print(f"{indent}[{t.layer.upper()}] {t.table_name}")
            print(f"{indent}    {rows_str}  |  {recompute}")
            print()

        print(f"  Total tainted : {len(self.tainted_tables)} tables")
        print(f"  Recompute     : {sum(1 for t in self.tainted_tables if t.needs_recompute)} tables")
        print("=" * 55)


class BlastRadiusAnalyzer:
    """Walks the DAG forward from a corrupt source to find all tainted tables."""

    def __init__(self, graph: "ChronicleGraph"):
        self.graph = graph

    def analyze(self, source_table: str, version: int) -> BlastRadiusResult:
        """
        Find all downstream tables tainted by a corrupt version.

        Args:
            source_table: Table containing the corrupt data
            version: The corrupt version number

        Returns:
            BlastRadiusResult listing all affected downstream tables
        """
        tainted: List[TaintedTable] = []
        self._walk_downstream(source_table, distance=1, tainted=tainted)

        return BlastRadiusResult(
            source_table=source_table,
            source_version=version,
            tainted_tables=tainted
        )

    def _walk_downstream(self, table_name: str, distance: int, tainted: list):
        downstream = self.graph.get_downstream(table_name)
        for ds_name in downstream:
            node = self.graph.get_node(ds_name)
            rows = self._get_row_count(node)
            tainted.append(TaintedTable(
                table_name=ds_name,
                distance=distance,
                layer=node.layer,
                estimated_rows=rows
            ))
            self._walk_downstream(ds_name, distance + 1, tainted)

    def _get_row_count(self, node) -> int:
        if self.graph.spark:
            try:
                df = self.graph.spark.read.format("delta").load(node.path)
                return df.count()
            except Exception:
                pass
        return -1