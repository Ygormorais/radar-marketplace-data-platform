"""Regras canônicas reutilizadas por Spark, testes e documentação."""

from __future__ import annotations

from decimal import Decimal
from enum import StrEnum


class OlistOrderStatus(StrEnum):
    CREATED = "created"
    APPROVED = "approved"
    INVOICED = "invoiced"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    UNAVAILABLE = "unavailable"
    CANCELED = "canceled"


ORDER_STATUS_VALUES = tuple(status.value for status in OlistOrderStatus)

DELIVERY_STATUS_ORDER = {
    "created": 10,
    "approved": 20,
    "invoiced": 30,
    "shipped": 40,
    "in_transit": 50,
    "out_for_delivery": 60,
    "delivered": 70,
    "exception": 90,
}

ALLOWED_DELIVERY_TRANSITIONS = frozenset(
    {
        ("created", "approved"),
        ("approved", "invoiced"),
        ("invoiced", "shipped"),
        ("shipped", "in_transit"),
        ("in_transit", "out_for_delivery"),
        ("out_for_delivery", "delivered"),
        ("shipped", "exception"),
        ("in_transit", "exception"),
        ("out_for_delivery", "exception"),
        ("exception", "in_transit"),
        ("exception", "out_for_delivery"),
    }
)


def is_valid_delivery_transition(previous: str | None, current: str) -> bool:
    if previous is None:
        return current == "created"
    if previous == current:
        return True
    return (previous, current) in ALLOWED_DELIVERY_TRANSITIONS


def is_payment_reconciled(
    order_total: Decimal,
    payment_total: Decimal,
    tolerance: Decimal = Decimal("0.01"),
) -> bool:
    if tolerance < 0:
        raise ValueError("tolerance não pode ser negativa")
    return abs(order_total - payment_total) <= tolerance
