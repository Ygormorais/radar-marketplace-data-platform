"""Reconciliações financeiras e logísticas na granularidade correta."""

from __future__ import annotations

from typing import Any

from radar.silver.business_rules import ALLOWED_DELIVERY_TRANSITIONS, DELIVERY_STATUS_ORDER


def build_financial_reconciliation(
    orders: Any,
    order_items: Any,
    payments: Any,
    *,
    tolerance: float = 0.01,
) -> Any:
    from pyspark.sql import functions as F

    if tolerance < 0:
        raise ValueError("tolerance não pode ser negativa")
    item_totals = order_items.groupBy("order_id").agg(
        F.sum("item_amount").cast("decimal(18,2)").alias("item_amount"),
        F.sum("freight_amount").cast("decimal(18,2)").alias("freight_amount"),
        F.sum("gross_amount").cast("decimal(18,2)").alias("expected_payment_amount"),
        F.count(F.lit(1)).alias("item_count"),
        F.countDistinct("seller_id").alias("seller_count"),
    )
    payment_totals = payments.groupBy("order_id").agg(
        F.sum("payment_amount").cast("decimal(18,2)").alias("actual_payment_amount"),
        F.count(F.lit(1)).alias("payment_record_count"),
        F.countDistinct("payment_type").alias("payment_method_count"),
    )
    expected = F.coalesce(F.col("expected_payment_amount"), F.lit(0)).cast("decimal(18,2)")
    actual = F.coalesce(F.col("actual_payment_amount"), F.lit(0)).cast("decimal(18,2)")
    difference = (actual - expected).cast("decimal(18,2)")
    return (
        orders.select("order_id", "order_status", "purchased_at")
        .join(item_totals, "order_id", "left")
        .join(payment_totals, "order_id", "left")
        .withColumn("expected_payment_amount", expected)
        .withColumn("actual_payment_amount", actual)
        .withColumn("payment_difference", difference)
        .withColumn("is_payment_reconciled", F.abs(difference) <= F.lit(tolerance))
        .withColumn(
            "reconciliation_status",
            F.when(F.col("item_count").isNull(), F.lit("ORDER_WITHOUT_ITEMS"))
            .when(F.col("payment_record_count").isNull(), F.lit("ORDER_WITHOUT_PAYMENT"))
            .when(F.abs(difference) <= F.lit(tolerance), F.lit("RECONCILED"))
            .when(difference > 0, F.lit("OVERPAID"))
            .otherwise(F.lit("UNDERPAID")),
        )
    )


def classify_delivery_transitions(events: Any) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("order_id").orderBy(
        F.col("occurred_at"), F.col("sequence_number"), F.col("produced_at")
    )
    previous = F.lag("status").over(window)
    allowed_pairs = F.array(
        *[
            F.struct(F.lit(left).alias("previous"), F.lit(right).alias("current"))
            for left, right in sorted(ALLOWED_DELIVERY_TRANSITIONS)
        ]
    )
    transition = F.struct(previous.alias("previous"), F.col("status").alias("current"))
    return (
        events.withColumn("previous_status", previous)
        .withColumn(
            "is_valid_transition",
            F.when(F.col("previous_status").isNull(), F.col("status") == "created")
            .when(F.col("previous_status") == F.col("status"), F.lit(True))
            .otherwise(F.array_contains(allowed_pairs, transition)),
        )
        .withColumn(
            "status_rank",
            F.create_map(
                *[
                    expression
                    for status, rank in DELIVERY_STATUS_ORDER.items()
                    for expression in (F.lit(status), F.lit(rank))
                ]
            )[F.col("status")],
        )
    )


def build_delivery_snapshot(orders: Any, delivery_events: Any) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    classified = classify_delivery_transitions(delivery_events)
    latest_window = Window.partitionBy("order_id").orderBy(
        F.col("occurred_at").desc(), F.col("sequence_number").desc(), F.col("produced_at").desc()
    )
    latest = (
        classified.withColumn("_latest_rank", F.row_number().over(latest_window))
        .filter(F.col("_latest_rank") == 1)
        .drop("_latest_rank")
        .select(
            "order_id",
            F.col("status").alias("latest_delivery_status"),
            F.col("occurred_at").alias("latest_event_at"),
            F.col("produced_at").alias("latest_event_received_at"),
            "location_state",
            "carrier_code",
        )
    )
    transition_quality = classified.groupBy("order_id").agg(
        F.sum((~F.col("is_valid_transition")).cast("int")).alias("invalid_transition_count"),
        F.count(F.lit(1)).alias("delivery_event_count"),
    )
    return (
        orders.select(
            "order_id",
            "order_status",
            "purchased_at",
            "estimated_delivery_at",
            "delivered_at",
            "delivery_delay_days",
            "is_delivered_on_time",
        )
        .join(latest, "order_id", "left")
        .join(transition_quality, "order_id", "left")
        .withColumn(
            "is_at_risk",
            F.col("delivered_at").isNull()
            & (F.current_timestamp() > F.col("estimated_delivery_at") - F.expr("INTERVAL 1 DAY")),
        )
        .withColumn(
            "event_arrival_lag_minutes",
            (F.col("latest_event_received_at").cast("long") - F.col("latest_event_at").cast("long"))
            / 60,
        )
    )
