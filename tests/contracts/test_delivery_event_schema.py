import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from radar.generators.delivery_events import generate_delivery_events


@pytest.mark.contract
def test_generated_event_satisfies_published_json_schema() -> None:
    schema_path = Path("contracts/events/delivery_event.v1.schema.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    event = next(
        generate_delivery_events(
            ["order-contract"], seed=99, base_time=datetime(2026, 1, 1, tzinfo=UTC)
        )
    )

    validator.validate(event.model_dump(mode="json"))


@pytest.mark.contract
def test_schema_rejects_unknown_field() -> None:
    schema = json.loads(Path("contracts/events/delivery_event.v1.schema.json").read_text())
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    event = next(
        generate_delivery_events(["x"], seed=1, base_time=datetime(2026, 1, 1, tzinfo=UTC))
    ).model_dump(mode="json")
    event["silent_schema_drift"] = True

    errors = list(validator.iter_errors(event))
    assert any("Additional properties" in error.message for error in errors)
