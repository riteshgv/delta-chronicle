"""
ChronicleGraph: Register and manage your Delta Lake table DAG.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import logging

logger = logging.getLogger(__name__)


@dataclass
class TableNode:
    """Represents one Delta table in the lakehouse DAG."""
    name: str
    path: str
    primary_key: str
    upstream: List[str] = field(default_factory=list)
    layer: str = "unknown"
    description: str = ""

    def __post_init__(self):
        if self.layer == "unknown":
            name_lower = self.name.lower()
            if "bronze" in name_lower:
                self.layer = "bronze"
            elif "silver" in name_lower:
                self.layer = "silver"
            elif "gold" in name_lower:
                self.layer = "gold"


class ChronicleGraph:
    """
    DAG registry for delta-chronicle.

    Register all Delta tables and their upstream dependencies.
    Supports fluent chaining: cg.register(...).register(...).register(...)
    """

    def __init__(self, spark=None):
        self.spark = spark
        self._nodes: Dict[str, TableNode] = {}
        logger.info("ChronicleGraph initialized")

    def register(
        self,
        name: str,
        path: str,
        primary_key: str,
        upstream: Optional[List[str]] = None,
        description: str = ""
    ) -> "ChronicleGraph":
        """Register a Delta table. Returns self for fluent chaining."""
        if upstream is None:
            upstream = []

        for up in upstream:
            if up not in self._nodes:
                logger.warning(
                    f"Upstream '{up}' not yet registered. "
                    "Register tables in order: bronze → silver → gold"
                )

        node = TableNode(
            name=name,
            path=path,
            primary_key=primary_key,
            upstream=upstream,
            description=description
        )
        self._nodes[name] = node
        logger.info(f"Registered: {name} (layer={node.layer})")
        return self

    def get_node(self, name: str) -> TableNode:
        if name not in self._nodes:
            raise ValueError(
                f"Table '{name}' not registered. "
                f"Available: {list(self._nodes.keys())}"
            )
        return self._nodes[name]

    def get_all_nodes(self) -> Dict[str, TableNode]:
        return dict(self._nodes)

    def get_downstream(self, name: str) -> List[str]:
        """All tables that list this table as upstream."""
        return [n.name for n in self._nodes.values() if name in n.upstream]

    def get_upstream(self, name: str) -> List[str]:
        return self._nodes[name].upstream if name in self._nodes else []

    def get_root_tables(self) -> List[str]:
        """Tables with no upstream — the sources of your DAG."""
        return [n.name for n in self._nodes.values() if not n.upstream]

    def get_leaf_tables(self) -> List[str]:
        """Tables with no downstream — the sinks of your DAG."""
        return [
            n.name for n in self._nodes.values()
            if not self.get_downstream(n.name)
        ]

    def validate(self) -> List[str]:
        """
        Check the graph for issues.
        Returns a list of warning strings (empty = no issues).
        """
        warnings = []
        if not self._nodes:
            warnings.append("No tables registered")
            return warnings

        if not self.get_root_tables():
            warnings.append("No root tables found — possible cycle in DAG")

        for name, node in self._nodes.items():
            for up in node.upstream:
                if up not in self._nodes:
                    warnings.append(
                        f"Table '{name}' references upstream '{up}' "
                        "which is not registered"
                    )
        return warnings

    def summary(self) -> str:
        """Human-readable summary of the registered DAG."""
        lines = ["\n📊 Chronicle Graph Summary", "=" * 45]
        for layer in ["bronze", "silver", "gold", "unknown"]:
            nodes = [n for n in self._nodes.values() if n.layer == layer]
            if nodes:
                lines.append(f"\n  [{layer.upper()}]")
                for n in nodes:
                    upstream_str = (
                        f"  ← {', '.join(n.upstream)}" if n.upstream else ""
                    )
                    desc = f"  # {n.description}" if n.description else ""
                    lines.append(f"    {n.name}{upstream_str}{desc}")

        lines.append(f"\n  Total tables : {len(self._nodes)}")
        lines.append(f"  Root tables  : {self.get_root_tables()}")
        lines.append(f"  Leaf tables  : {self.get_leaf_tables()}")
        warnings = self.validate()
        if warnings:
            lines.append(f"\n  ⚠️  Warnings:")
            for w in warnings:
                lines.append(f"    - {w}")
        return "\n".join(lines)

    def __repr__(self):
        return (
            f"ChronicleGraph("
            f"{len(self._nodes)} tables, "
            f"roots={self.get_root_tables()})"
        )

    def __len__(self):
        return len(self._nodes)