from pathlib import Path

import pytest

from radar.serving.dbt import build_dbt_command


def test_builds_fail_fast_dbt_command() -> None:
    command = build_dbt_command(
        project_dir=Path("Files/dbt"),
        profiles_dir=Path("Files/dbt"),
        target="prod",
        select="tag:published",
        threads=8,
    )
    assert command[:2] == ["dbt", "build"]
    assert command[-2:] == ["--select", "tag:published"]
    assert "--fail-fast" in command
    assert command[command.index("--threads") + 1] == "8"


def test_rejects_zero_threads() -> None:
    with pytest.raises(ValueError, match="threads"):
        build_dbt_command(
            project_dir=Path("dbt"), profiles_dir=Path("dbt"), target="dev", threads=0
        )
