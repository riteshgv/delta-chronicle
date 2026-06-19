
"""
Audit report data model for GDPR forget() operations.

Every forget() call produces a ForgetAuditReport containing:
  - One ForgetRecord per table processed
  - Timestamp of each delete
  - Row counts before and after
  - Delta version before and after
  - JSON export for compliance documentation
"""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import List, Optional
import json


@dataclass
class ForgetRecord:
    """Audit record for one table's delete operation."""
    table_name: str
    table_path: str
    layer: str
    primary_key_column: str
    primary_key_value: str
    rows_before: int
    rows_deleted: int
    rows_after: int
    delta_version_before: int
    delta_version_after: int
    started_at: str
    completed_at: str
    success: bool
    error_message: Optional[str] = None

    @property
    def duration_seconds(self) -> float:
        from datetime import datetime
        fmt = "%Y-%m-%dT%H:%M:%S.%f"
        try:
            t0 = datetime.strptime(self.started_at[:26],  fmt)
            t1 = datetime.strptime(self.completed_at[:26], fmt)
            return (t1 - t0).total_seconds()
        except Exception:
            return -1.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["duration_seconds"] = self.duration_seconds
        return d


@dataclass
class ForgetAuditReport:
    """Full audit report for one forget() call."""
    request_id: str
    primary_key_column: str
    primary_key_value: str
    requested_at: str
    completed_at: Optional[str]
    records: List[ForgetRecord] = field(default_factory=list)
    success: bool = False

    @property
    def total_tables(self) -> int:
        return len(self.records)

    @property
    def total_rows_deleted(self) -> int:
        return sum(r.rows_deleted for r in self.records if r.rows_deleted > 0)

    @property
    def failed_tables(self) -> List[str]:
        return [r.table_name for r in self.records if not r.success]

    @property
    def succeeded_tables(self) -> List[str]:
        return [r.table_name for r in self.records if r.success]

    def show(self):
        print("\n" + "=" * 58)
        print("  delta-chronicle  GDPR Forget Report")
        print("=" * 58)
        print(f"  Request ID   : {self.request_id}")
        print(f"  Subject      : {self.primary_key_column} = {self.primary_key_value}")
        print(f"  Requested at : {self.requested_at}")
        print(f"  Completed at : {self.completed_at or 'IN PROGRESS'}")
        print(f"  Overall      : {'SUCCESS' if self.success else 'FAILED'}")
        print()
        for r in self.records:
            status = "OK  " if r.success else "FAIL"
            print(f"  [{status}]  {r.table_name}  [{r.layer.upper()}]")
            print(f"           Rows deleted  : {r.rows_deleted}")
            print(f"           Before/After  : {r.rows_before} -> {r.rows_after}")
            print(f"           Delta version : v{r.delta_version_before} -> v{r.delta_version_after}")
            print(f"           Duration      : {r.duration_seconds:.2f}s")
            if r.error_message:
                print(f"           Error         : {r.error_message}")
            print()
        print(f"  Total tables  : {self.total_tables}")
        print(f"  Total deleted : {self.total_rows_deleted} rows")
        if self.failed_tables:
            print(f"  FAILED        : {self.failed_tables}")
        print("=" * 58)

    def to_dict(self) -> dict:
        return {
            "request_id":         self.request_id,
            "primary_key_column": self.primary_key_column,
            "primary_key_value":  self.primary_key_value,
            "requested_at":       self.requested_at,
            "completed_at":       self.completed_at,
            "success":            self.success,
            "summary": {
                "total_tables":       self.total_tables,
                "total_rows_deleted": self.total_rows_deleted,
                "succeeded_tables":   self.succeeded_tables,
                "failed_tables":      self.failed_tables,
            },
            "records": [r.to_dict() for r in self.records],
        }

    def to_json(self, path: Optional[str] = None, indent: int = 2) -> str:
        """Export as JSON string. Optionally write to file."""
        json_str = json.dumps(self.to_dict(), indent=indent, default=str)
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(json_str)
        return json_str

    @classmethod
    def from_json(cls, json_str: str) -> "ForgetAuditReport":
        """Reconstruct from JSON string."""
        data    = json.loads(json_str)
        records = [ForgetRecord(**r) for r in data.get("records", [])]
        return cls(
            request_id=data["request_id"],
            primary_key_column=data["primary_key_column"],
            primary_key_value=data["primary_key_value"],
            requested_at=data["requested_at"],
            completed_at=data.get("completed_at"),
            records=records,
            success=data.get("success", False),
        )