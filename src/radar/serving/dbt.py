"""Contrato de execução do dbt no estágio de serving."""

from __future__ import annotations

from pathlib import Path


def build_dbt_command(
    *,
    project_dir: Path,
    profiles_dir: Path,
    target: str,
    select: str | None = None,
    threads: int = 4,
) -> list[str]:
    if threads < 1:
        raise ValueError("threads deve ser maior que zero")
    command = [
        "dbt",
        "build",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target",
        target,
        "--threads",
        str(threads),
        "--fail-fast",
    ]
    if select:
        command.extend(["--select", select])
    return command
