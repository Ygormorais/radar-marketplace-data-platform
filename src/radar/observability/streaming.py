"""Telemetria de Structured Streaming persistida como dados operacionais."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Literal


@dataclass(frozen=True)
class StreamingOutcome:
    run_id: str
    query_name: str
    query_id: str
    status: Literal["SUCCEEDED", "FAILED"]
    recorded_at: datetime
    batch_id: int | None
    input_rows: int
    input_rows_per_second: float
    processed_rows_per_second: float
    error_message: str | None = None


def outcome_from_query(query: Any, *, run_id: str) -> StreamingOutcome:
    progress = query.lastProgress or {}
    exception = query.exception()
    return StreamingOutcome(
        run_id=run_id,
        query_name=query.name,
        query_id=str(query.id),
        status="FAILED" if exception else "SUCCEEDED",
        recorded_at=datetime.now(UTC),
        batch_id=progress.get("batchId"),
        input_rows=int(progress.get("numInputRows", 0)),
        input_rows_per_second=float(progress.get("inputRowsPerSecond", 0.0)),
        processed_rows_per_second=float(progress.get("processedRowsPerSecond", 0.0)),
        error_message=str(exception)[:4000] if exception else None,
    )


def append_streaming_outcomes(
    spark: Any, outcomes: list[StreamingOutcome], audit_path: str
) -> None:
    rows = [asdict(outcome) for outcome in outcomes]
    spark.createDataFrame(rows).write.format("delta").mode("append").save(audit_path)


def await_and_record_streams(
    spark: Any,
    *,
    queries: tuple[Any, ...],
    run_id: str,
    audit_path: str,
) -> str:
    """Aguarda todas as queries, registra resultado e propaga falhas ao pipeline."""
    raised_error: Exception | None = None
    for query in queries:
        try:
            query.awaitTermination()
        except Exception as error:  # Spark expõe o detalhe definitivo em query.exception().
            raised_error = error
            for other_query in queries:
                if getattr(other_query, "isActive", False):
                    other_query.stop()
            break
    outcomes = [outcome_from_query(query, run_id=run_id) for query in queries]
    append_streaming_outcomes(spark, outcomes, audit_path)
    failed = [outcome for outcome in outcomes if outcome.status == "FAILED"]
    if failed or raised_error:
        names = ", ".join(outcome.query_name for outcome in failed)
        raise RuntimeError(
            f"Falha nas queries de streaming: {names or 'não identificada'}"
        ) from raised_error
    return json.dumps(
        {
            "run_id": run_id,
            "status": "SUCCEEDED",
            "queries": [outcome.query_name for outcome in outcomes],
            "input_rows": sum(outcome.input_rows for outcome in outcomes),
        }
    )
