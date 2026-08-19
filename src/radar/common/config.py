"""Carregamento tipado e determinístico de configuração por ambiente."""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    """Modelo que rejeita chaves desconhecidas para detectar erros de configuração."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ProjectConfig(StrictModel):
    name: str = "radar"
    timezone: str
    schema_version: str


class StorageConfig(StrictModel):
    landing_root: str
    bronze_database: str
    silver_database: str
    gold_database: str
    checkpoint_root: str


class StreamingConfig(StrictModel):
    bootstrap_servers: str
    topic: str
    consumer_group: str
    checkpoint_interval_seconds: int = Field(gt=0, le=3600)
    watermark_delay: str


class GeneratorConfig(StrictModel):
    seed: int
    events_per_second: int = Field(gt=0, le=100_000)
    duplicate_rate: float = Field(ge=0, lt=1)
    late_event_rate: float = Field(ge=0, lt=1)
    invalid_event_rate: float = Field(ge=0, lt=1)
    default_order_count: int = Field(gt=0)

    @field_validator("invalid_event_rate")
    @classmethod
    def validate_combined_error_rate(cls, value: float) -> float:
        return value


class QualityConfig(StrictModel):
    max_quarantine_rate: float = Field(ge=0, lt=1)
    max_duplicate_rate: float = Field(ge=0, lt=1)
    freshness_minutes: int = Field(gt=0)


class AppConfig(StrictModel):
    environment: str
    project: ProjectConfig
    storage: StorageConfig
    streaming: StreamingConfig
    generator: GeneratorConfig
    quality: QualityConfig


def _deep_merge(base: MutableMapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        current = result.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            result[key] = _deep_merge(dict(current), value)
        else:
            result[key] = value
    return result


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"A raiz de {path} deve ser um objeto YAML")
    return loaded


def _parse_env_value(raw: str) -> Any:
    """Usa o parser YAML para números/booleanos, mantendo strings comuns."""

    return yaml.safe_load(raw)


def _environment_overrides(prefix: str = "RADAR__") -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, raw_value in os.environ.items():
        if not name.startswith(prefix):
            continue
        path = [part.lower() for part in name[len(prefix) :].split("__") if part]
        if not path:
            continue
        cursor = result
        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[path[-1]] = _parse_env_value(raw_value)
    return result


def load_config(environment: str | None = None, config_dir: str | Path | None = None) -> AppConfig:
    """Combina base, ambiente e overrides `RADAR__SECAO__CHAVE`."""

    selected_environment = environment or os.getenv("RADAR_ENVIRONMENT", "local")
    directory_value: str | Path = (
        config_dir if config_dir is not None else os.environ.get("RADAR_CONFIG_DIR", "config")
    )
    directory = Path(directory_value)
    merged = _deep_merge(
        _read_yaml(directory / "base.yml"), _read_yaml(directory / f"{selected_environment}.yml")
    )
    merged = _deep_merge(merged, _environment_overrides())
    merged["environment"] = selected_environment
    return AppConfig.model_validate(merged)
