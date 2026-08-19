"""Streaming Bronze para eventos Kafka/Event Hubs com semântica exactly-once lógica."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from radar.bronze.spark_schemas import delivery_event_struct_type

TriggerMode = Literal["available_now", "continuous"]

VALID_STATUSES = (
    "created",
    "approved",
    "invoiced",
    "shipped",
    "in_transit",
    "out_for_delivery",
    "delivered",
    "exception",
)


def read_kafka_stream(
    spark: Any,
    *,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
    extra_options: Mapping[str, str] | None = None,
) -> Any:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "true")
    )
    for key, value in (extra_options or {}).items():
        reader = reader.option(key, value)
    return reader.load()


def parse_delivery_stream(kafka_stream: Any) -> Any:
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
            delivery_event_struct_type(),
            {"mode": "PERMISSIVE", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX"},
        ),
    )
    required_missing = (
        F.col("event.event_id").isNull()
        | F.col("event.order_id").isNull()
        | F.col("event.occurred_at").isNull()
        | F.col("event.produced_at").isNull()
        | F.col("event.sequence_number").isNull()
    )
    return parsed.withColumn(
        "_quarantine_reason",
        F.when(F.col("event").isNull(), F.lit("INVALID_JSON"))
        .when(required_missing, F.lit("MISSING_REQUIRED_FIELD"))
        .when(F.col("event.schema_version") != "1.0.0", F.lit("UNSUPPORTED_SCHEMA_VERSION"))
        .when(~F.col("event.status").isin(*VALID_STATUSES), F.lit("INVALID_STATUS"))
        .when(~F.col("event.location_state").rlike("^[A-Z]{2}$"), F.lit("INVALID_STATE")),
    )


def valid_delivery_events(parsed_stream: Any, watermark_delay: str) -> Any:
    from pyspark.sql import functions as F

    return (
        parsed_stream.filter(F.col("_quarantine_reason").isNull())
        .select("event.*", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date("_ingested_at"))
        .withWatermark("occurred_at", watermark_delay)
        .dropDuplicates(["event_id"])
    )


def quarantine_delivery_events(parsed_stream: Any) -> Any:
    from pyspark.sql import functions as F

    return (
        parsed_stream.filter(F.col("_quarantine_reason").isNotNull())
        .drop("event")
        .withColumn("_source_name", F.lit("delivery_events"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.to_date("_ingested_at"))
    )


def upsert_event_microbatch(batch: Any, batch_id: int, target_path: str) -> None:
    from delta.tables import DeltaTable

    if batch.isEmpty():
        return
    staged = batch.withColumn("_micro_batch_id", batch_id)
    if not DeltaTable.isDeltaTable(batch.sparkSession, target_path):
        staged.write.format("delta").mode("append").partitionBy("_ingestion_date").save(target_path)
        return
    target = DeltaTable.forPath(batch.sparkSession, target_path)
    target.alias("target").merge(
        staged.alias("source"), "target.event_id = source.event_id"
    ).whenNotMatchedInsertAll().execute()


def configure_stream_trigger(
    writer: Any,
    *,
    trigger_mode: TriggerMode,
    trigger_interval: str,
) -> Any:
    """Configura execução contínua ou microbatch finito para orquestração."""
    if trigger_mode == "available_now":
        return writer.trigger(availableNow=True)
    if trigger_mode == "continuous":
        return writer.trigger(processingTime=trigger_interval)
    raise ValueError(f"trigger_mode inválido: {trigger_mode}")


def start_bronze_streams(
    parsed_stream: Any,
    *,
    target_path: str,
    quarantine_path: str,
    checkpoint_root: str,
    watermark_delay: str,
    trigger_interval: str = "30 seconds",
    trigger_mode: TriggerMode = "continuous",
) -> tuple[Any, Any]:
    valid_writer = (
        valid_delivery_events(parsed_stream, watermark_delay)
        .writeStream.foreachBatch(
            lambda batch, batch_id: upsert_event_microbatch(batch, batch_id, target_path)
        )
        .option("checkpointLocation", f"{checkpoint_root}/delivery_events_valid")
        .queryName("radar_bronze_delivery_events")
    )
    valid_query = configure_stream_trigger(
        valid_writer, trigger_mode=trigger_mode, trigger_interval=trigger_interval
    ).start()
    quarantine_writer = (
        quarantine_delivery_events(parsed_stream)
        .writeStream.format("delta")
        .outputMode("append")
        .partitionBy("_source_name", "_ingestion_date")
        .option("checkpointLocation", f"{checkpoint_root}/delivery_events_quarantine")
        .queryName("radar_quarantine_delivery_events")
    )
    quarantine_query = configure_stream_trigger(
        quarantine_writer, trigger_mode=trigger_mode, trigger_interval=trigger_interval
    ).start(quarantine_path)
    return valid_query, quarantine_query
