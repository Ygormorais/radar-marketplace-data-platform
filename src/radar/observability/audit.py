"""Registro append-only de execuções Spark em uma tabela Delta."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Literal


@dataclass(frozen=True)
class IngestionAudit:
    run_id: str
    source_name: str
    target: str
    status: Literal["SUCCEEDED", "FAILED"]
    started_at: datetime
    ended_at: datetime
    rows_read: int
    rows_written: int
    rows_quarantined: int
    source_file_hash: str
    error_message: str | None = None

    @classmethod
    def failed(
        cls,
        *,
        run_id: str,
        source_name: str,
        target: str,
        started_at: datetime,
        source_file_hash: str,
        error: Exception,
    ) -> IngestionAudit:
        return cls(
            run_id=run_id,
            source_name=source_name,
            target=target,
            status="FAILED",
            started_at=started_at,
            ended_at=datetime.now(UTC),
            rows_read=0,
            rows_written=0,
            rows_quarantined=0,
            source_file_hash=source_file_hash,
            error_message=f"{type(error).__name__}: {error}"[:4000],
        )


def append_audit(spark: Any, audit: IngestionAudit, audit_path: str) -> None:
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    payload = asdict(audit)
    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("source_name", StringType(), False),
            StructField("target", StringType(), False),
            StructField("status", StringType(), False),
            StructField("started_at", TimestampType(), False),
            StructField("ended_at", TimestampType(), False),
            StructField("rows_read", LongType(), False),
            StructField("rows_written", LongType(), False),
            StructField("rows_quarantined", LongType(), False),
            StructField("source_file_hash", StringType(), False),
            StructField("error_message", StringType(), True),
        ]
    )
    spark.createDataFrame([payload], schema=schema).write.format("delta").mode("append").save(
        audit_path
    )
