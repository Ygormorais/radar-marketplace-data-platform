import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from radar.ingestion.manifest import build_manifest, count_csv_rows, write_manifest


def test_builds_stable_manifest_with_csv_row_count(tmp_path: Path) -> None:
    (tmp_path / "orders.csv").write_text("order_id,value\na,10\nb,20\n", encoding="utf-8")
    (tmp_path / "notes.json").write_text("{}", encoding="utf-8")
    generated_at = datetime(2026, 8, 18, tzinfo=UTC)

    manifest = build_manifest(
        tmp_path,
        dataset="fixture",
        dataset_version="1",
        source_reference="unit-test",
        generated_at=generated_at,
    )

    assert [item.relative_path for item in manifest.files] == ["notes.json", "orders.csv"]
    assert manifest.files[1].row_count == 2
    assert manifest.files[0].row_count is None

    target = tmp_path / "_manifest.json"
    write_manifest(manifest, target)
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["generated_at"] == "2026-08-18T00:00:00Z"
    assert len(payload["files"][1]["sha256"]) == 64


def test_manifest_ignores_existing_manifest(tmp_path: Path) -> None:
    (tmp_path / "data.csv").write_text("id\n1\n", encoding="utf-8")
    (tmp_path / "_manifest.json").write_text("stale", encoding="utf-8")

    manifest = build_manifest(
        tmp_path,
        dataset="fixture",
        dataset_version="1",
        source_reference="test",
    )

    assert [item.relative_path for item in manifest.files] == ["data.csv"]


def test_empty_dataset_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Nenhum arquivo"):
        build_manifest(tmp_path, dataset="empty", dataset_version="1", source_reference="test")


def test_empty_csv_has_zero_data_rows(tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("id\n", encoding="utf-8")
    assert count_csv_rows(path) == 0
