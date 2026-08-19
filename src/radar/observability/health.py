"""Regras determinísticas para SLOs e alertas operacionais do Radar."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class AlertSeverity(StrEnum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class HealthSnapshot:
    observed_at: datetime
    latest_stream_heartbeat: datetime | None
    streaming_failures: int
    quarantine_rate: float
    failed_quality_checks: int


@dataclass(frozen=True)
class OperationalAlert:
    alert_code: str
    severity: AlertSeverity
    message: str
    observed_value: float
    threshold: float


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def evaluate_health(
    snapshot: HealthSnapshot,
    *,
    max_heartbeat_age_minutes: int = 20,
    max_quarantine_rate: float = 0.02,
) -> list[OperationalAlert]:
    """Avalia SLOs sem dependência de Spark, permitindo testes e reuso."""
    alerts: list[OperationalAlert] = []
    if snapshot.latest_stream_heartbeat is None:
        heartbeat_age = float("inf")
    else:
        delta = _as_utc(snapshot.observed_at) - _as_utc(snapshot.latest_stream_heartbeat)
        heartbeat_age = max(delta.total_seconds() / 60, 0.0)
    if heartbeat_age > max_heartbeat_age_minutes:
        alerts.append(
            OperationalAlert(
                "STREAM_HEARTBEAT_STALE",
                AlertSeverity.CRITICAL,
                "Nenhum heartbeat de streaming dentro do SLO.",
                heartbeat_age,
                float(max_heartbeat_age_minutes),
            )
        )
    if snapshot.streaming_failures > 0:
        alerts.append(
            OperationalAlert(
                "STREAM_QUERY_FAILED",
                AlertSeverity.CRITICAL,
                "Uma ou mais queries de streaming falharam na janela.",
                float(snapshot.streaming_failures),
                0.0,
            )
        )
    if snapshot.quarantine_rate > max_quarantine_rate:
        alerts.append(
            OperationalAlert(
                "QUARANTINE_RATE_HIGH",
                AlertSeverity.WARNING,
                "Taxa de eventos em quarentena acima do limite.",
                snapshot.quarantine_rate,
                max_quarantine_rate,
            )
        )
    if snapshot.failed_quality_checks > 0:
        alerts.append(
            OperationalAlert(
                "QUALITY_GATE_FAILURE",
                AlertSeverity.CRITICAL,
                "Há testes críticos de qualidade com falha.",
                float(snapshot.failed_quality_checks),
                0.0,
            )
        )
    return alerts
