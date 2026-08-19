from __future__ import annotations

import os
import platform
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from importlib.util import find_spec
from pathlib import Path

import pytest

from radar.quality.expectations import (
    Expectation,
    Rule,
    evaluate_expectations,
    evaluate_referential_integrity,
)
from radar.silver.merge import apply_scd2
from radar.silver.reconciliation import build_financial_reconciliation
from radar.silver.sessionization import sessionize_clickstream
from radar.silver.transforms import (
    transform_order_items,
    transform_orders,
    transform_payments,
)

SPARK_UNAVAILABLE = find_spec("pyspark") is None or find_spec("delta") is None
WINDOWS_HADOOP_UNAVAILABLE = platform.system() == "Windows" and not os.getenv("HADOOP_HOME")
pytestmark = pytest.mark.skipif(
    SPARK_UNAVAILABLE or WINDOWS_HADOOP_UNAVAILABLE,
    reason="runtime Spark/Delta local indisponível; integração executa no CI Linux",
)


@pytest.mark.integration
def test_financial_and_order_transformations(spark: object) -> None:
    purchased = datetime(2026, 1, 1, tzinfo=UTC)
    orders_raw = spark.createDataFrame(
        [
            (
                "order-1",
                "customer-1",
                "DELIVERED",
                purchased,
                purchased + timedelta(minutes=10),
                purchased + timedelta(days=1),
                purchased + timedelta(days=3),
                purchased + timedelta(days=2),
            )
        ],
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp timestamp, order_approved_at timestamp, "
        "order_delivered_carrier_date timestamp, order_delivered_customer_date timestamp, "
        "order_estimated_delivery_date timestamp",
    )
    items_raw = spark.createDataFrame(
        [("order-1", 1, "product-1", "seller-1", purchased, Decimal("90"), Decimal("10"))],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "shipping_limit_date timestamp, price decimal(18,2), freight_value decimal(18,2)",
    )
    payments_raw = spark.createDataFrame(
        [("order-1", 1, "credit_card", 1, Decimal("100"))],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value decimal(18,2)",
    )

    orders = transform_orders(orders_raw)
    items = transform_order_items(items_raw)
    payments = transform_payments(payments_raw)
    reconciled = build_financial_reconciliation(orders, items, payments).first()

    assert orders.first()["delivery_delay_days"] == 1
    assert reconciled["is_payment_reconciled"] is True
    assert reconciled["reconciliation_status"] == "RECONCILED"


@pytest.mark.integration
def test_sessionization_splits_on_inactivity(spark: object) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    events = spark.createDataFrame(
        [
            ("e1", "u1", "product_view", start, "p1", "mobile", "organic"),
            ("e2", "u1", "add_to_cart", start + timedelta(minutes=5), "p1", "mobile", "organic"),
            ("e3", "u1", "purchase", start + timedelta(minutes=40), "p1", "mobile", "organic"),
        ],
        "event_id string, user_id string, event_type string, occurred_at timestamp, "
        "product_id string, device_type string, traffic_source string",
    )

    sessions = (
        sessionize_clickstream(events, inactivity_minutes=30).orderBy("session_sequence").collect()
    )

    assert len(sessions) == 2
    assert sessions[0]["event_count"] == 2
    assert sessions[0]["has_add_to_cart"] is True
    assert sessions[1]["has_purchase"] is True


@pytest.mark.integration
def test_quality_expectations_and_referential_integrity(spark: object) -> None:
    child = spark.createDataFrame([("a", "p1"), ("b", "missing")], "id string, product_id string")
    parent = spark.createDataFrame([("p1",)], "product_id string")
    results = evaluate_expectations(
        child,
        [
            Expectation("id_not_null", "id", Rule.NOT_NULL),
            Expectation("id_unique", "id", Rule.UNIQUE),
        ],
        run_id="run",
        dataset="child",
    )
    reference = evaluate_referential_integrity(
        child,
        parent,
        child_key="product_id",
        parent_key="product_id",
        run_id="run",
        dataset="child",
        expectation_name="product_fk",
    )

    assert all(result.passed for result in results)
    assert reference.failed_rows == 1
    assert reference.failure_rate == 0.5


@pytest.mark.integration
def test_scd2_expires_current_and_isolates_late_arrival(spark: object, tmp_path: Path) -> None:
    target = str(tmp_path / "seller_history")
    effective = datetime(2026, 1, 1, tzinfo=UTC)
    initial = spark.createDataFrame(
        [("seller-1", "SP", effective)], "seller_id string, state string, effective_at timestamp"
    )
    apply_scd2(
        spark,
        initial,
        target_path=target,
        business_keys=["seller_id"],
        tracked_columns=["state"],
        effective_at_column="effective_at",
        run_id="run-1",
    )
    changed = spark.createDataFrame(
        [("seller-1", "RJ", effective + timedelta(days=1))],
        "seller_id string, state string, effective_at timestamp",
    )
    apply_scd2(
        spark,
        changed,
        target_path=target,
        business_keys=["seller_id"],
        tracked_columns=["state"],
        effective_at_column="effective_at",
        run_id="run-2",
    )
    history = spark.read.format("delta").load(target)
    assert history.count() == 2
    assert history.filter("is_current = true").first()["state"] == "RJ"

    late = spark.createDataFrame(
        [("seller-1", "MG", effective - timedelta(days=1))],
        "seller_id string, state string, effective_at timestamp",
    )
    result = apply_scd2(
        spark,
        late,
        target_path=target,
        business_keys=["seller_id"],
        tracked_columns=["state"],
        effective_at_column="effective_at",
        run_id="run-3",
    )
    assert result.late_arrivals.count() == 1
    assert spark.read.format("delta").load(target).count() == 2
