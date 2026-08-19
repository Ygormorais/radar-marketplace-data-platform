from datetime import UTC, datetime

import pytest

from radar.quality.expectations import (
    Expectation,
    QualityResult,
    Rule,
    Severity,
    enforce_quality_gate,
    failing_errors,
)


def _result(*, passed: bool, severity: Severity = Severity.ERROR) -> QualityResult:
    return QualityResult(
        run_id="run",
        dataset="orders",
        expectation="unique_order",
        rule="unique",
        severity=severity.value,
        evaluated_rows=10,
        failed_rows=0 if passed else 1,
        failure_rate=0 if passed else 0.1,
        max_failure_rate=0,
        passed=passed,
        evaluated_at=datetime(2026, 8, 18, tzinfo=UTC),
    )


def test_expectation_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="accepted_values"):
        Expectation("status", "status", Rule.ACCEPTED_VALUES)
    with pytest.raises(ValueError, match="BETWEEN"):
        Expectation("amount", "amount", Rule.BETWEEN)
    with pytest.raises(ValueError, match="intervalo"):
        Expectation("id", "id", Rule.NOT_NULL, max_failure_rate=1.1)


def test_gate_fails_only_for_error_severity() -> None:
    warning = _result(passed=False, severity=Severity.WARN)
    error = _result(passed=False)

    assert failing_errors([warning]) == []
    enforce_quality_gate([warning, _result(passed=True)])
    with pytest.raises(RuntimeError, match=r"orders\.unique_order"):
        enforce_quality_gate([warning, error])
