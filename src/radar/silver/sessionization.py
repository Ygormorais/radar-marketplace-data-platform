"""Sessionização determinística de clickstream por event time."""

from __future__ import annotations

from typing import Any

CLICKSTREAM_EVENT_TYPES = ("page_view", "product_view", "add_to_cart", "checkout", "purchase")


def sessionize_clickstream(events: Any, *, inactivity_minutes: int = 30) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if inactivity_minutes <= 0:
        raise ValueError("inactivity_minutes deve ser positivo")
    required = {"event_id", "user_id", "event_type", "occurred_at"}
    missing = sorted(required - set(events.columns))
    if missing:
        raise ValueError(f"Colunas clickstream ausentes: {missing}")

    working = events
    for optional_column in ("product_id", "device_type", "traffic_source"):
        if optional_column not in working.columns:
            working = working.withColumn(optional_column, F.lit(None).cast("string"))

    ordering = Window.partitionBy("user_id").orderBy("occurred_at", "event_id")
    cumulative = ordering.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    previous_at = F.lag("occurred_at").over(ordering)
    gap_seconds = F.col("occurred_at").cast("long") - previous_at.cast("long")
    is_new_session = previous_at.isNull() | (gap_seconds > inactivity_minutes * 60)
    sequenced = (
        working.filter(F.col("event_type").isin(*CLICKSTREAM_EVENT_TYPES))
        .withColumn("previous_event_at", previous_at)
        .withColumn("session_sequence", F.sum(is_new_session.cast("int")).over(cumulative))
        .withColumn(
            "session_id",
            F.sha2(F.concat_ws(":", "user_id", F.col("session_sequence").cast("string")), 256),
        )
    )
    return sequenced.groupBy("session_id", "user_id", "session_sequence").agg(
        F.min("occurred_at").alias("session_started_at"),
        F.max("occurred_at").alias("session_ended_at"),
        (F.max("occurred_at").cast("long") - F.min("occurred_at").cast("long")).alias(
            "session_duration_seconds"
        ),
        F.count(F.lit(1)).alias("event_count"),
        F.countDistinct("product_id").alias("distinct_product_count"),
        F.max((F.col("event_type") == "product_view").cast("int"))
        .cast("boolean")
        .alias("has_product_view"),
        F.max((F.col("event_type") == "add_to_cart").cast("int"))
        .cast("boolean")
        .alias("has_add_to_cart"),
        F.max((F.col("event_type") == "checkout").cast("int"))
        .cast("boolean")
        .alias("has_checkout"),
        F.max((F.col("event_type") == "purchase").cast("int"))
        .cast("boolean")
        .alias("has_purchase"),
        F.min_by("device_type", F.struct("occurred_at", "event_id")).alias("device_type"),
        F.min_by("traffic_source", F.struct("occurred_at", "event_id")).alias("traffic_source"),
    )
