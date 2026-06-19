
"""
ForgetPropagator: Cascading GDPR DELETE across a Delta Lake DAG.
Day 11: scaffold + topological sort + stub _delete_from_table.
Day 12: real DELETE implementation replaces the stub.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional
import uuid
import logging

from delta_chronicle.gdpr.audit import ForgetAuditReport, ForgetRecord

if TYPE_CHECKING:
    from delta_chronicle.core.graph import ChronicleGraph

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ForgetPropagator:
    """
    Propagates GDPR DELETE requests across all tables in a ChronicleGraph.
    Processes tables in topological order: roots first, leaves last.
    This ensures bronze is cleaned before silver before gold.
    """

    def __init__(self, graph: "ChronicleGraph"):
        if graph.spark is None:
            raise ValueError(
                "ForgetPropagator requires a Spark session. "
                "Pass spark=spark when creating ChronicleGraph."
            )
        self.graph = graph

    def forget(
        self,
        primary_key_value,
        primary_key_column: Optional[str] = None,
    ) -> ForgetAuditReport:
        """
        Delete all rows matching primary_key_value from every
        registered table in topological order.

        Args:
            primary_key_value:  Value to delete e.g. 1 or "vendor_A"
            primary_key_column: Column to filter on. If None, uses
                                each table's registered primary_key.

        Returns:
            ForgetAuditReport with full per-table audit trail
        """
        request_id   = str(uuid.uuid4())[:8]
        requested_at = _now()

        logger.info(
            f"[{request_id}] forget() started: "
            f"{primary_key_column}={primary_key_value}"
        )

        ordered = self._topological_order()
        logger.info(f"[{request_id}] Table order: {ordered}")

        report = ForgetAuditReport(
            request_id=request_id,
            primary_key_column=primary_key_column or "primary_key",
            primary_key_value=str(primary_key_value),
            requested_at=requested_at,
            completed_at=None,
            records=[],
            success=False,
        )

        all_ok = True
        for table_name in ordered:
            node   = self.graph.get_node(table_name)
            pk_col = primary_key_column or node.primary_key

            record = self._delete_from_table(
                node=node,
                pk_col=pk_col,
                pk_val=primary_key_value,
                request_id=request_id,
            )
            report.records.append(record)

            if not record.success:
                all_ok = False
                logger.error(
                    f"[{request_id}] Failed on {table_name}: "
                    f"{record.error_message}"
                )

        report.completed_at = _now()
        report.success      = all_ok

        logger.info(
            f"[{request_id}] forget() done. "
            f"success={all_ok} deleted={report.total_rows_deleted} rows"
        )
        return report

    def _delete_from_table(
        self, node, pk_col: str, pk_val, request_id: str
    ) -> ForgetRecord:
        """
        STUB — Day 12 replaces this with real Delta DELETE.
        """
        started_at = _now()
        return ForgetRecord(
            table_name=node.name,
            table_path=node.path,
            layer=node.layer,
            primary_key_column=pk_col,
            primary_key_value=str(pk_val),
            rows_before=-1,
            rows_deleted=-1,
            rows_after=-1,
            delta_version_before=-1,
            delta_version_after=-1,
            started_at=started_at,
            completed_at=_now(),
            success=False,
            error_message="Not implemented yet — coming Day 12",
        )

    def _topological_order(self) -> list:
        """
        Kahn's algorithm — BFS topological sort.
        Returns tables with roots first (bronze) and leaves last (gold).
        """
        from collections import deque
        nodes     = self.graph.get_all_nodes()
        in_degree = {name: 0 for name in nodes}

        for name, node in nodes.items():
            for up in node.upstream:
                if up in in_degree:
                    in_degree[name] += 1

        queue  = deque([n for n, d in in_degree.items() if d == 0])
        result = []

        while queue:
            current = queue.popleft()
            result.append(current)
            for downstream in self.graph.get_downstream(current):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(result) != len(nodes):
            logger.warning("Topological sort incomplete — possible cycle.")

        return result