from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from radar.bronze.streaming import configure_stream_trigger
from radar.observability.streaming import outcome_from_query


class FakeWriter:
    def __init__(self) -> None:
        self.options: dict[str, object] = {}

    def trigger(self, **kwargs: object) -> "FakeWriter":
        self.options = kwargs
        return self


def test_configure_available_now_trigger() -> None:
    writer = FakeWriter()
    assert (
        configure_stream_trigger(
            writer, trigger_mode="available_now", trigger_interval="30 seconds"
        )
        is writer
    )
    assert writer.options == {"availableNow": True}


def test_configure_continuous_trigger() -> None:
    writer = FakeWriter()
    configure_stream_trigger(writer, trigger_mode="continuous", trigger_interval="45 seconds")
    assert writer.options == {"processingTime": "45 seconds"}


def test_rejects_unknown_trigger() -> None:
    with pytest.raises(ValueError, match="trigger_mode inválido"):
        configure_stream_trigger(
            FakeWriter(),
            trigger_mode="invalid",
            trigger_interval="1 second",  # type: ignore[arg-type]
        )


def test_maps_query_progress_to_outcome() -> None:
    query = SimpleNamespace(
        name="radar_stream",
        id="query-1",
        lastProgress={
            "batchId": 7,
            "numInputRows": 42,
            "inputRowsPerSecond": 8.5,
            "processedRowsPerSecond": 10.25,
        },
        exception=lambda: None,
    )
    outcome = outcome_from_query(query, run_id="run-1")
    assert outcome.status == "SUCCEEDED"
    assert outcome.input_rows == 42
    assert outcome.batch_id == 7
    assert isinstance(outcome.recorded_at, datetime)
    assert outcome.recorded_at.tzinfo == UTC
