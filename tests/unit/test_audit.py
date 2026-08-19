from datetime import UTC, datetime

from radar.observability.audit import IngestionAudit


def test_failed_audit_sanitizes_and_limits_error() -> None:
    audit = IngestionAudit.failed(
        run_id="run-1",
        source_name="orders",
        target="Tables/bronze_orders",
        started_at=datetime(2026, 8, 18, tzinfo=UTC),
        source_file_hash="a" * 64,
        error=RuntimeError("x" * 5000),
    )

    assert audit.status == "FAILED"
    assert audit.error_message is not None
    assert audit.error_message.startswith("RuntimeError:")
    assert len(audit.error_message) == 4000
