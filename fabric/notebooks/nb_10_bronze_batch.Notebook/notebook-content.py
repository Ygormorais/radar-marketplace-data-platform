# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
environment = "dev"
run_id = "manual"
source_root = "Files/landing/olist/v1"
bronze_root = "Tables"
quarantine_path = "Tables/bronze_quarantine"
audit_path = "Tables/ctl_ingestion_audit"
source_hashes_json = "{}"

# CELL ********************
import json
from pathlib import PurePosixPath

from radar.bronze.batch import ingest_batch
from radar.contracts.sources import OLIST_CONTRACTS

# CELL ********************
# O hash vem do manifesto de landing. O pipeline substitui este mapa por parâmetros
# derivados de `_manifest.json`; valor vazio é proibido para evitar carga não auditável.
source_hashes = json.loads(source_hashes_json)
missing_hashes = sorted(set(OLIST_CONTRACTS) - set(source_hashes))
if missing_hashes:
    raise ValueError(f"Hashes SHA-256 ausentes no parâmetro source_hashes_json: {missing_hashes}")

# CELL ********************
results = []
for source_name, contract in OLIST_CONTRACTS.items():
    result = ingest_batch(
        spark,
        source_path=str(PurePosixPath(source_root) / contract.filename),
        contract=contract,
        source_file_hash=source_hashes[source_name],
        run_id=run_id,
        target_path=str(PurePosixPath(bronze_root) / f"bronze_{source_name}"),
        quarantine_path=quarantine_path,
        audit_path=audit_path,
    )
    results.append(result)

# CELL ********************
summary = [result.__dict__ for result in results]
display(spark.createDataFrame(summary))
