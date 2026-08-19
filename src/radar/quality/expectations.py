"""Framework enxuto de qualidade Spark com resultados interoperáveis."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class Rule(StrEnum):
    NOT_NULL = "not_null"
    ACCEPTED_VALUES = "accepted_values"
    BETWEEN = "between"
    UNIQUE = "unique"


class Severity(StrEnum):
    ERROR = "ERROR"
    WARN = "WARN"


@dataclass(frozen=True)
class Expectation:
    name: str
    column: str
    rule: Rule
    severity: Severity = Severity.ERROR
    max_failure_rate: float = 0.0
    accepted_values: tuple[Any, ...] = ()
    minimum: float | None = None
    maximum: float | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.max_failure_rate <= 1:
            raise ValueError("max_failure_rate deve estar no intervalo [0, 1]")
        if self.rule == Rule.ACCEPTED_VALUES and not self.accepted_values:
            raise ValueError("accepted_values é obrigatório para esta regra")
        if self.rule == Rule.BETWEEN and self.minimum is None and self.maximum is None:
            raise ValueError("BETWEEN exige minimum e/ou maximum")


@dataclass(frozen=True)
class QualityResult:
    run_id: str
    dataset: str
    expectation: str
    rule: str
    severity: str
    evaluated_rows: int
    failed_rows: int
    failure_rate: float
    max_failure_rate: float
    passed: bool
    evaluated_at: datetime


def _failure_condition(expectation: Expectation) -> Any:
    from pyspark.sql import functions as F

    column = F.col(expectation.column)
    if expectation.rule == Rule.NOT_NULL:
        return column.isNull()
    if expectation.rule == Rule.ACCEPTED_VALUES:
        return column.isNull() | ~column.isin(*expectation.accepted_values)
    if expectation.rule == Rule.BETWEEN:
        condition = column.isNull()
        if expectation.minimum is not None:
            condition = condition | (column < F.lit(expectation.minimum))
        if expectation.maximum is not None:
            condition = condition | (column > F.lit(expectation.maximum))
        return condition
    raise ValueError(f"Regra requer avaliação especializada: {expectation.rule}")


def evaluate_expectations(
    dataframe: Any,
    expectations: list[Expectation],
    *,
    run_id: str,
    dataset: str,
) -> list[QualityResult]:
    missing = sorted({item.column for item in expectations} - set(dataframe.columns))
    if missing:
        raise ValueError(f"Colunas de qualidade ausentes em {dataset}: {missing}")
    cached = dataframe.cache()
    total = int(cached.count())
    evaluated_at = datetime.now(UTC)
    results: list[QualityResult] = []
    for expectation in expectations:
        if expectation.rule == Rule.UNIQUE:
            failed = int(
                cached.groupBy(expectation.column)
                .count()
                .filter("count > 1")
                .selectExpr("coalesce(sum(count), 0) AS failed")
                .first()["failed"]
            )
        else:
            failed = int(cached.filter(_failure_condition(expectation)).count())
        rate = failed / total if total else 0.0
        results.append(
            QualityResult(
                run_id=run_id,
                dataset=dataset,
                expectation=expectation.name,
                rule=expectation.rule.value,
                severity=expectation.severity.value,
                evaluated_rows=total,
                failed_rows=failed,
                failure_rate=rate,
                max_failure_rate=expectation.max_failure_rate,
                passed=rate <= expectation.max_failure_rate,
                evaluated_at=evaluated_at,
            )
        )
    cached.unpersist()
    return results


def evaluate_referential_integrity(
    child: Any,
    parent: Any,
    *,
    child_key: str,
    parent_key: str,
    run_id: str,
    dataset: str,
    expectation_name: str,
    max_failure_rate: float = 0.0,
) -> QualityResult:
    total = int(child.count())
    failed = int(
        child.select(child_key)
        .filter(f"{child_key} IS NOT NULL")
        .join(
            parent.select(parent_key).dropDuplicates(),
            child[child_key] == parent[parent_key],
            "left_anti",
        )
        .count()
    )
    rate = failed / total if total else 0.0
    return QualityResult(
        run_id=run_id,
        dataset=dataset,
        expectation=expectation_name,
        rule="referential_integrity",
        severity=Severity.ERROR.value,
        evaluated_rows=total,
        failed_rows=failed,
        failure_rate=rate,
        max_failure_rate=max_failure_rate,
        passed=rate <= max_failure_rate,
        evaluated_at=datetime.now(UTC),
    )


def failing_errors(results: list[QualityResult]) -> list[QualityResult]:
    return [result for result in results if not result.passed and result.severity == Severity.ERROR]


def persist_results(spark: Any, results: list[QualityResult], target_path: str) -> None:
    if not results:
        return
    payload = [asdict(result) for result in results]
    spark.createDataFrame(payload).write.format("delta").mode("append").save(target_path)


def enforce_quality_gate(results: list[QualityResult]) -> None:
    failures = failing_errors(results)
    if failures:
        details = "; ".join(
            f"{result.dataset}.{result.expectation}={result.failure_rate:.4%}"
            for result in failures
        )
        raise RuntimeError(f"Quality gate reprovado: {details}")
