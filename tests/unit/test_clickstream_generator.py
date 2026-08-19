from datetime import UTC, datetime
from pathlib import Path

import pytest

from radar.contracts.events import ClickstreamEvent
from radar.generators.clickstream_events import (
    generate_clickstream_events,
    write_clickstream_jsonl,
)


def test_clickstream_generation_is_deterministic_and_funnel_ordered() -> None:
    parameters = {"user_count": 10, "seed": 42, "base_time": datetime(2026, 1, 1, tzinfo=UTC)}
    first = list(generate_clickstream_events(**parameters))
    second = list(generate_clickstream_events(**parameters))

    assert [event.model_dump_json() for event in first] == [
        event.model_dump_json() for event in second
    ]
    assert len(first) >= 20
    by_user: dict[str, list[str]] = {}
    for event in first:
        by_user.setdefault(event.user_id, []).append(event.event_type.value)
    assert all(events[:2] == ["page_view", "product_view"] for events in by_user.values())


def test_clickstream_writer_emits_valid_models(tmp_path: Path) -> None:
    events = generate_clickstream_events(
        user_count=2, seed=1, base_time=datetime(2026, 1, 1, tzinfo=UTC)
    )
    target = tmp_path / "clickstream.jsonl"

    count = write_clickstream_jsonl(events, target)

    lines = target.read_text(encoding="utf-8").splitlines()
    assert count == len(lines)
    assert ClickstreamEvent.model_validate_json(lines[0]).event_type.value == "page_view"


def test_clickstream_generator_rejects_invalid_parameters() -> None:
    with pytest.raises(ValueError, match="positivo"):
        list(
            generate_clickstream_events(
                user_count=0, seed=1, base_time=datetime(2026, 1, 1, tzinfo=UTC)
            )
        )
    naive = datetime(2026, 1, 1)  # noqa: DTZ001 - cenário inválido sob teste
    with pytest.raises(ValueError, match="timezone"):
        list(generate_clickstream_events(user_count=1, seed=1, base_time=naive))
