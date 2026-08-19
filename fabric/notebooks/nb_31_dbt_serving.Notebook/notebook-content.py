# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
run_id = "manual"
project_dir = "/lakehouse/default/Files/dbt"
profiles_dir = "/lakehouse/default/Files/dbt"
target = "prod"
select = ""
threads = 4

# CELL ********************
import json
import subprocess
from pathlib import Path

from radar.serving.dbt import build_dbt_command

# CELL ********************
project_path = Path(project_dir)
profiles_path = Path(profiles_dir)
if not (project_path / "dbt_project.yml").is_file():
    raise FileNotFoundError(f"Projeto dbt ausente no OneLake: {project_path}")
if not (profiles_path / "profiles.yml").is_file():
    raise FileNotFoundError(f"profiles.yml ausente no OneLake: {profiles_path}")

command = build_dbt_command(
    project_dir=project_path,
    profiles_dir=profiles_path,
    target=target,
    select=select or None,
    threads=int(threads),
)
result = subprocess.run(  # noqa: S603
    command,
    check=False,
    capture_output=True,
    text=True,
)
print(result.stdout)
if result.returncode:
    print(result.stderr)
    raise RuntimeError(f"dbt build falhou com exit code {result.returncode}")

summary = {"run_id": run_id, "status": "SUCCEEDED", "target": target}
notebookutils.notebook.exit(json.dumps(summary))
