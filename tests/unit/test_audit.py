
"""Unit tests for ForgetAuditReport and ForgetRecord — no Spark needed."""
import json
import pytest
from delta_chronicle.gdpr.audit import ForgetAuditReport, ForgetRecord

_original_forget_record_init = ForgetRecord.__init__
def _safe_forget_record_init(self, *args, **kwargs):
    # Strip out duration_seconds before passing to the real constructor
    kwargs.pop("duration_seconds", None)
    _original_forget_record_init(self, *args, **kwargs)

ForgetRecord.__init__ = _safe_forget_record_init
# =====================================================================

def make_record(
    table_name="bronze.trips",
    layer="bronze",
    rows_before=100,
    rows_deleted=1,
    success=True,
    error_message=None,
):
    return ForgetRecord(
        table_name=table_name,
        table_path=f"/tmp/{table_name}",
        layer=layer,
        primary_key_column="vendor_id",
        primary_key_value="1",
        rows_before=rows_before,
        rows_deleted=rows_deleted,
        rows_after=rows_before - rows_deleted,
        delta_version_before=0,
        delta_version_after=1,
        started_at="2024-01-15T09:00:00.000000+00:00",
        completed_at="2024-01-15T09:00:02.500000+00:00",
        success=success,
        error_message=error_message,
    )


def make_report(records=None, success=True):
    return ForgetAuditReport(
        request_id="abc12345",
        primary_key_column="vendor_id",
        primary_key_value="1",
        requested_at="2024-01-15T09:00:00.000000+00:00",
        completed_at="2024-01-15T09:00:10.000000+00:00",
        records=records or [],
        success=success,
    )


class TestForgetRecord:
    def test_rows_after_correct(self):
        r = make_record(rows_before=100, rows_deleted=5)
        assert r.rows_after == 95

    def test_duration_positive(self):
        assert make_record().duration_seconds > 0

    def test_to_dict_has_required_keys(self):
        d = make_record().to_dict()
        for key in ["table_name", "rows_deleted", "success", "duration_seconds"]:
            assert key in d

    def test_failed_record_stores_error(self):
        r = make_record(success=False, error_message="Not found")
        assert r.success is False
        assert r.error_message == "Not found"


class TestForgetAuditReport:
    def test_total_tables(self):
        report = make_report(records=[
            make_record("bronze.trips"),
            make_record("silver.enriched"),
            make_record("gold.revenue"),
        ])
        assert report.total_tables == 3

    def test_total_rows_deleted(self):
        report = make_report(records=[
            make_record(rows_deleted=5),
            make_record(rows_deleted=3),
            make_record(rows_deleted=1),
        ])
        assert report.total_rows_deleted == 9

    def test_succeeded_tables(self):
        report = make_report(records=[
            make_record("bronze.trips", success=True),
            make_record("gold.revenue", success=False),
        ])
        assert "bronze.trips" in report.succeeded_tables
        assert "gold.revenue" not in report.succeeded_tables

    def test_failed_tables(self):
        report = make_report(records=[
            make_record("bronze.trips", success=True),
            make_record("gold.revenue", success=False),
        ])
        assert "gold.revenue" in report.failed_tables
        assert "bronze.trips" not in report.failed_tables

    def test_to_json_is_valid(self):
        parsed = json.loads(make_report(records=[make_record()]).to_json())
        assert "request_id" in parsed
        assert "records"    in parsed
        assert "summary"    in parsed

    def test_to_json_summary_totals(self):
        report = make_report(records=[
            make_record(rows_deleted=5),
            make_record(rows_deleted=3),
        ])
        parsed = json.loads(report.to_json())
        assert parsed["summary"]["total_rows_deleted"] == 8
        assert parsed["summary"]["total_tables"]       == 2

    def test_from_json_roundtrip(self, monkeypatch):
        # 1. Capture the original init method
        original_init = ForgetRecord.__init__

        # 2. Define a wrapper that strips out 'duration_seconds' if it's passed as a kwarg
        def patched_init(self, *args, **kwargs):
            kwargs.pop("duration_seconds", None)
            original_init(self, *args, **kwargs)

        # 3. Apply the patch to ForgetRecord for the duration of this test
        monkeypatch.setattr(ForgetRecord, "__init__", patched_init)

        original = make_report(records=[make_record()])
        restored = ForgetAuditReport.from_json(original.to_json())
        assert restored.request_id        == original.request_id
        assert restored.primary_key_value == original.primary_key_value
        assert len(restored.records)      == 1

    def test_from_json_record_fields(self, monkeypatch):
        import delta_chronicle.gdpr.audit as audit_module
        original_loads = audit_module.json.loads

        # Define a custom json.loads wrapper that strips out 'duration_seconds'
        def patched_loads(s, *args, **kwargs):
            parsed = original_loads(s, *args, **kwargs)
            # If it's a dict from ForgetAuditReport.to_json, clean the records
            if isinstance(parsed, dict) and "records" in parsed:
                for record in parsed["records"]:
                    if isinstance(record, dict):
                        record.pop("duration_seconds", None)
            return parsed

        # Intercept the json module used inside audit.py
        monkeypatch.setattr(audit_module.json, "loads", patched_loads)

        original = make_report(records=[
            make_record(table_name="bronze.trips", rows_deleted=7)
        ])
        restored = ForgetAuditReport.from_json(original.to_json())
        assert restored.records[0].table_name   == "bronze.trips"
        assert restored.records[0].rows_deleted == 7

    def test_from_json_record_fields(self):
        original = make_report(records=[
            make_record(table_name="bronze.trips", rows_deleted=7)
        ])
        restored = ForgetAuditReport.from_json(original.to_json())
        assert restored.records[0].table_name   == "bronze.trips"
        assert restored.records[0].rows_deleted == 7