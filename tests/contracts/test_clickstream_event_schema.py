import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker


@pytest.mark.contract
def test_clickstream_contract_accepts_purchase_event() -> None:
    schema = json.loads(Path("contracts/events/clickstream_event.v1.schema.json").read_text())
    event = {
        "schema_version": "1.0.0",
        "event_id": "6761d480-8fc7-5a7d-a133-89df788e5f7d",
        "user_id": "customer-1",
        "anonymous_id": None,
        "session_source_id": "browser-session-1",
        "event_type": "purchase",
        "occurred_at": "2026-08-18T12:00:00Z",
        "product_id": "product-1",
        "order_id": "order-1",
        "device_type": "mobile",
        "traffic_source": "organic",
    }

    Draft202012Validator(schema, format_checker=FormatChecker()).validate(event)


@pytest.mark.contract
def test_clickstream_contract_rejects_unknown_event_type() -> None:
    schema = json.loads(Path("contracts/events/clickstream_event.v1.schema.json").read_text())
    event = {
        "schema_version": "1.0.0",
        "event_id": "6761d480-8fc7-5a7d-a133-89df788e5f7d",
        "user_id": "customer-1",
        "event_type": "teleport",
        "occurred_at": "2026-08-18T12:00:00Z",
    }

    assert list(Draft202012Validator(schema).iter_errors(event))
