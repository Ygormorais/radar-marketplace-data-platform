from __future__ import annotations

import json
import os
import platform
from importlib.util import find_spec
from pathlib import Path

import pytest

from radar.bronze.batch import ingest_batch
from radar.bronze.clickstream import parse_clickstream
from radar.bronze.streaming import parse_delivery_stream
from radar.contracts.sources import get_source_contract
from radar.generators.spark_events import generate_delivery_events_spark

SPARK_UNAVAILABLE = find_spec("pyspark") is None or find_spec("delta") is None
WINDOWS_HADOOP_UNAVAILABLE = platform.system() == "Windows" and not os.getenv("HADOOP_HOME")
pytestmark = pytest.mark.skipif(
    SPARK_UNAVAILABLE or WINDOWS_HADOOP_UNAVAILABLE,
    reason="runtime Spark/Delta local indisponível; integração executa no CI Linux",
)


@pytest.mark.integration
def test_batch_ingestion_is_idempotent_and_quarantines(spark: object, tmp_path: Path) -> None:
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
        "order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date\n"
        "order-1,customer-1,delivered,2026-01-01 10:00:00,2026-01-01 10:05:00,"
        "2026-01-02 10:00:00,2026-01-03 10:00:00,2026-01-04 10:00:00\n"
        ",customer-2,created,2026-01-01 11:00:00,,,,2026-01-05 10:00:00\n",
        encoding="utf-8",
    )
    arguments = {
        "spark": spark,
        "source_path": str(source),
        "contract": get_source_contract("orders"),
        "source_file_hash": "a" * 64,
        "run_id": "integration-run",
        "target_path": str(tmp_path / "bronze_orders"),
        "quarantine_path": str(tmp_path / "quarantine"),
        "audit_path": str(tmp_path / "audit"),
    }

    first = ingest_batch(**arguments)
    second = ingest_batch(**arguments)

    assert first.rows_read == 2
    assert first.rows_written == 1
    assert first.rows_quarantined == 1
    assert second.rows_written == 0
    assert spark.read.format("delta").load(arguments["target_path"]).count() == 1
    assert spark.read.format("delta").load(arguments["quarantine_path"]).count() == 1
    assert spark.read.format("delta").load(arguments["audit_path"]).count() == 2


@pytest.mark.integration
def test_spark_generator_contract_and_cardinality(spark: object) -> None:
    generated = generate_delivery_events_spark(spark, order_count=10, seed=42, partitions=2)

    assert generated.count() == 70
    assert generated.select("event_id").distinct().count() == 70
    first = generated.first().asDict(recursive=True)
    assert first["schema_version"] == "1.0.0"
    assert len(first["event_id"]) == 36
    assert first["attributes"]["synthetic"] == "true"


@pytest.mark.integration
def test_stream_parser_classifies_invalid_status(spark: object) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    valid = (
        generate_delivery_events_spark(spark, order_count=1, seed=1).first().asDict(recursive=True)
    )
    valid["occurred_at"] = valid["occurred_at"].isoformat() + "Z"
    valid["produced_at"] = valid["produced_at"].isoformat() + "Z"
    invalid = dict(valid, status="teleported")
    schema = StructType(
        [
            StructField("value", StringType(), False),
            StructField("topic", StringType(), False),
            StructField("partition", LongType(), False),
            StructField("offset", LongType(), False),
            StructField("timestamp", TimestampType(), True),
        ]
    )
    frame = spark.createDataFrame(
        [(json.dumps(invalid), "events", 0, 1, None)], schema=schema
    ).withColumn("timestamp", F.current_timestamp())

    parsed = parse_delivery_stream(frame).first()
    assert parsed["_quarantine_reason"] == "INVALID_STATUS"


@pytest.mark.integration
def test_clickstream_parser_rejects_unknown_type(spark: object) -> None:
    from pyspark.sql import functions as F

    payload = json.dumps(
        {
            "schema_version": "1.0.0",
            "event_id": "6761d480-8fc7-5a7d-a133-89df788e5f7d",
            "user_id": "u1",
            "event_type": "teleport",
            "occurred_at": "2026-01-01T00:00:00Z",
        }
    )
    frame = spark.createDataFrame(
        [(payload, "clickstream", 0, 1)], "value string, topic string, partition long, offset long"
    ).withColumn("timestamp", F.current_timestamp())

    parsed = parse_clickstream(frame).first()
    assert parsed["_quarantine_reason"] == "INVALID_EVENT_TYPE"
