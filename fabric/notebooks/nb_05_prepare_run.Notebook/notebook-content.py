# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
environment = "dev"
run_id = "manual"
manifest_path = "Files/landing/olist/v1/_manifest.json"

# CELL ********************
import json
import re

from radar.contracts.sources import OLIST_CONTRACTS

# CELL ********************
manifest = json.loads(notebookutils.fs.head(manifest_path, 1024 * 1024))
if manifest.get("dataset") != "olist_brazilian_ecommerce":
    raise ValueError(f"Dataset inesperado no manifesto: {manifest.get('dataset')}")

files = {item["relative_path"]: item for item in manifest.get("files", [])}
source_hashes = {}
for source_name, contract in OLIST_CONTRACTS.items():
    item = files.get(contract.filename)
    if item is None:
        raise ValueError(f"Arquivo obrigatório ausente no manifesto: {contract.filename}")
    sha256 = item.get("sha256", "")
    if not re.fullmatch(r"[a-f0-9]{64}", sha256):
        raise ValueError(f"SHA-256 inválido no manifesto: {contract.filename}")
    source_hashes[source_name] = sha256

# CELL ********************
# O exit value é consumido diretamente pela atividade Bronze do pipeline mestre.
notebookutils.notebook.exit(json.dumps(source_hashes, sort_keys=True))
