from pathlib import Path

import pytest
from pydantic import ValidationError

from radar.common.config import load_config


def test_loads_local_configuration() -> None:
    config = load_config("local")

    assert config.environment == "local"
    assert config.project.timezone == "America/Sao_Paulo"
    assert config.storage.landing_root == "data/raw/landing"
    assert config.generator.seed == 20260818


def test_environment_variable_overrides_nested_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RADAR__GENERATOR__SEED", "42")
    monkeypatch.setenv("RADAR__QUALITY__MAX_QUARANTINE_RATE", "0.02")

    config = load_config("local")

    assert config.generator.seed == 42
    assert config.quality.max_quarantine_rate == 0.02


def test_rejects_unknown_configuration_key(tmp_path: Path) -> None:
    (tmp_path / "base.yml").write_text(
        """
project: {name: radar, timezone: UTC, schema_version: 1.0.0}
storage:
  landing_root: x
  bronze_database: x
  silver_database: x
  gold_database: x
  checkpoint_root: x
streaming:
  bootstrap_servers: x
  topic: x
  consumer_group: x
  checkpoint_interval_seconds: 1
  watermark_delay: 1 hour
generator:
  seed: 1
  events_per_second: 1
  duplicate_rate: 0
  late_event_rate: 0
  invalid_event_rate: 0
  default_order_count: 1
quality: {max_quarantine_rate: 0, max_duplicate_rate: 0, freshness_minutes: 1}
unexpected: true
""",
        encoding="utf-8",
    )
    (tmp_path / "local.yml").write_text("environment: local\n", encoding="utf-8")

    with pytest.raises(ValidationError, match="unexpected"):
        load_config("local", tmp_path)


def test_missing_environment_file_fails_fast(tmp_path: Path) -> None:
    (tmp_path / "base.yml").write_text("{}", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match=r"prod\.yml"):
        load_config("prod", tmp_path)
