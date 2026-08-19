import zipfile
from pathlib import Path

import pytest

from radar.ingestion.olist import REQUIRED_OLIST_FILES, extract_olist_archive


def _create_archive(path: Path, files: set[str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for filename in files:
            archive.writestr(f"nested/{filename}", "id\n1\n")


def test_extracts_only_complete_expected_dataset(tmp_path: Path) -> None:
    archive = tmp_path / "olist.zip"
    _create_archive(archive, set(REQUIRED_OLIST_FILES) | {"README.txt"})

    extracted = extract_olist_archive(archive, tmp_path / "target")

    assert {path.name for path in extracted} == REQUIRED_OLIST_FILES
    assert not (tmp_path / "target" / "README.txt").exists()


def test_rejects_incomplete_dataset(tmp_path: Path) -> None:
    archive = tmp_path / "olist.zip"
    _create_archive(archive, {"olist_orders_dataset.csv"})

    with pytest.raises(ValueError, match="incompleto"):
        extract_olist_archive(archive, tmp_path / "target")


def test_rejects_duplicate_expected_file(tmp_path: Path) -> None:
    archive = tmp_path / "olist.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for filename in REQUIRED_OLIST_FILES:
            bundle.writestr(filename, "id\n1\n")
        bundle.writestr("duplicate/olist_orders_dataset.csv", "id\n2\n")

    with pytest.raises(ValueError, match="duplicado"):
        extract_olist_archive(archive, tmp_path / "target")
