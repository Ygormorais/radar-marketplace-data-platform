from decimal import Decimal

import pytest

from radar.silver.business_rules import (
    is_payment_reconciled,
    is_valid_delivery_transition,
)


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        (None, "created"),
        ("created", "approved"),
        ("in_transit", "exception"),
        ("shipped", "shipped"),
    ],
)
def test_valid_delivery_transitions(previous: str | None, current: str) -> None:
    assert is_valid_delivery_transition(previous, current)


@pytest.mark.parametrize(
    ("previous", "current"),
    [(None, "delivered"), ("created", "delivered"), ("delivered", "in_transit")],
)
def test_invalid_delivery_transitions(previous: str | None, current: str) -> None:
    assert not is_valid_delivery_transition(previous, current)


def test_payment_reconciliation_uses_decimal_tolerance() -> None:
    assert is_payment_reconciled(Decimal("100.00"), Decimal("100.01"))
    assert not is_payment_reconciled(Decimal("100.00"), Decimal("100.02"))
    with pytest.raises(ValueError, match="negativa"):
        is_payment_reconciled(Decimal("1"), Decimal("1"), Decimal("-0.01"))
