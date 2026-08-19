import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from radar.contracts.events import DeliveryEvent, DeliveryStatus
from radar.generators.delivery_events import (
    HAPPY_PATH,
    generate_delivery_events,
    read_order_ids,
    synthetic_order_ids,
    write_jsonl,
)

BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def test_generator_is_deterministic_and_preserves_lifecycle() -> None:
    kwargs = {"seed": 7, "base_time": BASE_TIME, "duplicate_rate": 0, "late_event_rate": 0}
    first = list(generate_delivery_events(["order-a"], **kwargs))
    second = list(generate_delivery_events(["order-a"], **kwargs))

    assert [event.model_dump_json() for event in first] == [
        event.model_dump_json() for event in second
    ]
    assert [event.status for event in first] == list(HAPPY_PATH)
    assert [event.sequence_number for event in first] == list(range(1, len(HAPPY_PATH) + 1))
    assert all(event.attributes["synthetic"] is True for event in first)


def test_duplicate_rate_can_emit_exact_duplicate() -> None:
    events = list(
        generate_delivery_events(
            ["order-a"], seed=1, base_time=BASE_TIME, duplicate_rate=0.99, late_event_rate=0
        )
    )
    ids = [event.event_id for event in events]
    assert len(ids) > len(set(ids))


def test_write_jsonl_emits_valid_contracts(tmp_path: Path) -> None:
    target = tmp_path / "events.jsonl"
    events = generate_delivery_events(["order-a"], seed=2, base_time=BASE_TIME)

    count = write_jsonl(events, target)
    lines = target.read_text(encoding="utf-8").splitlines()

    assert count == len(HAPPY_PATH)
    assert len(lines) == count
    assert DeliveryEvent.model_validate_json(lines[-1]).status == DeliveryStatus.DELIVERED
    assert json.loads(lines[0])["schema_version"] == "1.0.0"


def test_read_order_ids_and_reject_invalid_csv(tmp_path: Path) -> None:
    valid = tmp_path / "orders.csv"
    valid.write_text("order_id,value\nabc,1\n,2\ndef,3\n", encoding="utf-8")
    assert read_order_ids(valid) == ["abc", "def"]

    invalid = tmp_path / "invalid.csv"
    invalid.write_text("customer_id\na\n", encoding="utf-8")
    with pytest.raises(ValueError, match="order_id"):
        read_order_ids(invalid)


def test_rejects_naive_base_time_and_invalid_counts() -> None:
    with pytest.raises(ValueError, match="timezone"):
        naive_time = datetime(2026, 1, 1)  # noqa: DTZ001 - cenário inválido sob teste
        list(generate_delivery_events(["x"], seed=1, base_time=naive_time))
    with pytest.raises(ValueError, match="positivo"):
        synthetic_order_ids(0)


def test_event_contract_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        naive_time = datetime(2026, 1, 1)  # noqa: DTZ001 - cenário inválido sob teste
        DeliveryEvent(
            event_id="00000000-0000-0000-0000-000000000000",
            order_id="x",
            status="created",
            occurred_at=naive_time,
            produced_at=BASE_TIME,
            location_state="SP",
            carrier_code="carrier",
            sequence_number=1,
        )
