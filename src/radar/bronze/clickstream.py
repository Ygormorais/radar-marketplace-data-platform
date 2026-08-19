"""Parsing e persistência Bronze do clickstream versionado."""

from __future__ import annotations

from typing import Any

from radar.bronze.spark_schemas import clickstream_event_struct_type
from radar.bronze.streaming import TriggerMode, configure_stream_trigger, upsert_event_microbatch
from radar.silver.sessionization import CLICKSTREAM_EVENT_TYPES


def parse_clickstream(kafka_stream: Any) -> Any:
    from pyspark.sql import functions as F

    parsed = kafka_stream.select(
        F.col("value").cast("string").alias("_raw_payload"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    ).withColumn(
        "event",
        F.from_json(
            "_raw_payload",
            clickstream_event_struct_type(),
            {"mode": "PERMISSIVE", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX"},
        ),
    )
    return parsed.withColumn(
        "_quarantine_reason",
        F.when(F.col("event").isNull(), F.lit("INVALID_JSON"))
        .when(
            F.col("event.event_id").isNull()
            | F.col("event.user_id").isNull()
            | F.col("event.occurred_at").isNull(),
            F.lit("MISSING_REQUIRED_FIELD"),
        )
        .when(F.col("event.schema_version") != "1.0.0", F.lit("UNSUPPORTED_SCHEMA_VERSION"))
        .when(
            ~F.col("event.event_type").isin(*CLICKSTREAM_EVENT_TYPES),
            F.lit("INVALID_EVENT_TYPE"),
        ),
    )


def start_clickstream(
    parsed_stream: Any,
    *,
    target_path: str,
    quarantine_path: str,
    checkpoint_root: str,
    watermark_delay: str = "2 hours",
    trigger_interval: str = "30 seconds",
    trigger_mode: TriggerMode = "continuous",
) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    valid = (
        parsed_stream.filter(F.col("_quarantine_reason").isNull())
        .select(
            "event.*",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date("_ingested_at"))
        .withWatermark("occurred_at", watermark_delay)
        .dropDuplicates(["event_id"])
    )
    invalid = (
        parsed_stream.filter(F.col("_quarantine_reason").isNotNull())
        .drop("event")
        .withColumn("_source_name", F.lit("clickstream_events"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date("_ingested_at"))
    )
    valid_writer = (
        valid.writeStream.foreachBatch(
            lambda batch, batch_id: upsert_event_microbatch(batch, batch_id, target_path)
        )
        .option("checkpointLocation", f"{checkpoint_root}/clickstream_valid")
        .queryName("radar_bronze_clickstream")
    )
    valid_query = configure_stream_trigger(
        valid_writer, trigger_mode=trigger_mode, trigger_interval=trigger_interval
    ).start()
    invalid_writer = (
        invalid.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("_source_name", "_ingestion_date")
        .option("checkpointLocation", f"{checkpoint_root}/clickstream_quarantine")
        .queryName("radar_quarantine_clickstream")
    )
    invalid_query = configure_stream_trigger(
        invalid_writer, trigger_mode=trigger_mode, trigger_interval=trigger_interval
    ).start(quarantine_path)
    return valid_query, invalid_query
