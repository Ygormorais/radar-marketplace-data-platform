"""Manifestos imutáveis para rastrear arquivos recebidos no landing."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from radar.common.hashing import sha256_file


class FileManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relative_path: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: int = Field(ge=0)
    row_count: int | None = Field(default=None, ge=0)


class DatasetManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset: str
    dataset_version: str
    generated_at: datetime
    source_reference: str
    files: list[FileManifest]


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream)
        next(reader, None)
        return sum(1 for _ in reader)


def build_manifest(
    dataset_root: Path,
    *,
    dataset: str,
    dataset_version: str,
    source_reference: str,
    generated_at: datetime | None = None,
) -> DatasetManifest:
    files: list[FileManifest] = []
    for path in sorted(item for item in dataset_root.rglob("*") if item.is_file()):
        if path.name == "_manifest.json":
            continue
        files.append(
            FileManifest(
                relative_path=path.relative_to(dataset_root).as_posix(),
                sha256=sha256_file(path),
                size_bytes=path.stat().st_size,
                row_count=count_csv_rows(path) if path.suffix.lower() == ".csv" else None,
            )
        )
    if not files:
        raise ValueError(f"Nenhum arquivo encontrado em {dataset_root}")
    return DatasetManifest(
        dataset=dataset,
        dataset_version=dataset_version,
        generated_at=generated_at or datetime.now(UTC),
        source_reference=source_reference,
        files=files,
    )


def write_manifest(manifest: DatasetManifest, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = manifest.model_dump(mode="json")
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
