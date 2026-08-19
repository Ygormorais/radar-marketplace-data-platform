from datetime import UTC, datetime, timedelta

from radar.observability.health import AlertSeverity, HealthSnapshot, evaluate_health

NOW = datetime(2026, 8, 18, 12, tzinfo=UTC)


def test_healthy_snapshot_has_no_alerts() -> None:
    snapshot = HealthSnapshot(NOW, NOW - timedelta(minutes=5), 0, 0.005, 0)
    assert evaluate_health(snapshot) == []


def test_health_rules_classify_operational_failures() -> None:
    snapshot = HealthSnapshot(NOW, NOW - timedelta(minutes=40), 2, 0.08, 1)
    alerts = evaluate_health(snapshot)
    assert {alert.alert_code for alert in alerts} == {
        "STREAM_HEARTBEAT_STALE",
        "STREAM_QUERY_FAILED",
        "QUARANTINE_RATE_HIGH",
        "QUALITY_GATE_FAILURE",
    }
    assert sum(alert.severity == AlertSeverity.CRITICAL for alert in alerts) == 3


def test_missing_heartbeat_is_critical() -> None:
    alerts = evaluate_health(HealthSnapshot(NOW, None, 0, 0.0, 0))
    assert alerts[0].alert_code == "STREAM_HEARTBEAT_STALE"
    assert alerts[0].observed_value == float("inf")
