"""Geração distribuída de eventos logísticos compatíveis com o contrato v1."""

from __future__ import annotations

from typing import Any

from radar.generators.delivery_events import DEFAULT_CARRIERS, DEFAULT_STATES, HAPPY_PATH


def generate_delivery_events_spark(
    spark: Any,
    *,
    order_count: int,
    seed: int,
    base_time: str = "2026-01-01T00:00:00Z",
    partitions: int | None = None,
) -> Any:
    """Gera `order_count * 7` eventos sem coletar IDs no driver."""

    if order_count <= 0:
        raise ValueError("order_count deve ser positivo")
    from pyspark.sql import functions as F

    statuses = F.array(*[F.lit(status.value) for status in HAPPY_PATH])
    orders = spark.range(1, order_count + 1, numPartitions=partitions).select(
        F.format_string("synthetic_order_%012d", F.col("id")).alias("order_id"),
        F.col("id").alias("order_number"),
    )
    events = orders.select("*", F.posexplode(statuses).alias("status_index", "status")).withColumn(
        "sequence_number", F.col("status_index") + 1
    )
    stable_hash = F.abs(F.xxhash64("order_id", F.lit(seed)))
    offset_seconds = (F.col("order_number") * 3 + F.col("sequence_number") * 360) * 60
    occurred_at = (F.to_timestamp(F.lit(base_time)).cast("long") + offset_seconds).cast("timestamp")
    event_hash = F.sha2(F.concat_ws(":", F.lit("radar"), "order_id", "sequence_number"), 256)
    event_uuid = F.concat(
        F.substring(event_hash, 1, 8),
        F.lit("-"),
        F.substring(event_hash, 9, 4),
        F.lit("-5"),
        F.substring(event_hash, 14, 3),
        F.lit("-a"),
        F.substring(event_hash, 18, 3),
        F.lit("-"),
        F.substring(event_hash, 21, 12),
    )
    return events.select(
        F.lit("1.0.0").alias("schema_version"),
        event_uuid.alias("event_id"),
        "order_id",
        "status",
        occurred_at.alias("occurred_at"),
        (occurred_at + F.expr("INTERVAL 5 MINUTES")).alias("produced_at"),
        F.element_at(
            F.array(*[F.lit(value) for value in DEFAULT_STATES]),
            (stable_hash % len(DEFAULT_STATES) + 1).cast("int"),
        ).alias("location_state"),
        F.element_at(
            F.array(*[F.lit(value) for value in DEFAULT_CARRIERS]),
            (stable_hash % len(DEFAULT_CARRIERS) + 1).cast("int"),
        ).alias("carrier_code"),
        "sequence_number",
        F.create_map(
            F.lit("synthetic"), F.lit("true"), F.lit("generator_seed"), F.lit(str(seed))
        ).alias("attributes"),
    )
