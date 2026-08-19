"""Ingestão batch Bronze com schema enforcement, quarantine e idempotência Delta."""

from __future__ import annotations

from datetime import UTC, datetime
from functools import reduce
from operator import or_
from typing import Any

from radar.bronze.spark_schemas import source_struct_type
from radar.contracts.sources import SourceContract
from radar.observability.audit import IngestionAudit, append_audit


def _record_hash_expression(columns: tuple[str, ...]) -> Any:
    from pyspark.sql import functions as F

    normalized = [F.coalesce(F.col(name).cast("string"), F.lit("∅")) for name in columns]
    return F.sha2(F.concat_ws("\u001f", *normalized), 256)


def read_source_csv(spark: Any, source_path: str, contract: SourceContract) -> Any:
    """Valida o header e lê o CSV com schema explícito, sem inferência."""

    observed_header = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .load(source_path)
        .columns
    )
    contract.validate_header(observed_header)
    return (
        spark.read.format("csv")
        .schema(source_struct_type(contract))
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("enforceSchema", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(source_path)
    )


def enrich_with_metadata(
    dataframe: Any,
    *,
    contract: SourceContract,
    run_id: str,
    source_file_hash: str,
    ingested_at: datetime,
) -> Any:
    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("_record_hash", _record_hash_expression(contract.column_names))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_file_hash", F.lit(source_file_hash))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.lit(ingested_at))
        .withColumn("_ingestion_date", F.to_date("_ingested_at"))
        .withColumn("_source_name", F.lit(contract.name))
    )


def split_valid_and_quarantine(dataframe: Any, contract: SourceContract) -> tuple[Any, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    missing_required = reduce(
        or_,
        (F.col(name).isNull() for name in contract.required_columns),
        F.lit(False),
    )
    classified = dataframe.withColumn(
        "_quarantine_reason",
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("MALFORMED_RECORD")).when(
            missing_required, F.lit("MISSING_REQUIRED_FIELD")
        ),
    )
    invalid = classified.filter(F.col("_quarantine_reason").isNotNull())
    valid_candidates = classified.filter(F.col("_quarantine_reason").isNull()).drop(
        "_quarantine_reason"
    )

    duplicate_window = Window.partitionBy("_source_file_hash", "_record_hash")
    ranked = valid_candidates.withColumn(
        "_duplicate_count", F.count(F.lit(1)).over(duplicate_window)
    )
    duplicates = (
        ranked.filter(F.col("_duplicate_count") > 1)
        .withColumn("_quarantine_reason", F.lit("DUPLICATE_IN_SOURCE_FILE"))
        .dropDuplicates(["_source_file_hash", "_record_hash"])
    )
    valid = ranked.dropDuplicates(["_source_file_hash", "_record_hash"]).drop("_duplicate_count")
    quarantine = invalid.unionByName(duplicates, allowMissingColumns=True)
    return valid, quarantine


def merge_idempotently(spark: Any, dataframe: Any, target_path: str) -> int:
    """Insere uma vez por arquivo+registro, usando optimistic concurrency do Delta."""

    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, target_path):
        rows_to_write = int(dataframe.count())
        (
            dataframe.write.format("delta")
            .mode("append")
            .partitionBy("_ingestion_date")
            .save(target_path)
        )
        return rows_to_write

    target = DeltaTable.forPath(spark, target_path)
    existing_keys = target.toDF().select("_source_file_hash", "_record_hash")
    new_rows = dataframe.join(
        existing_keys,
        on=["_source_file_hash", "_record_hash"],
        how="left_anti",
    ).cache()
    rows_to_write = int(new_rows.count())
    if rows_to_write == 0:
        new_rows.unpersist()
        return 0
    condition = (
        "target._source_file_hash = source._source_file_hash "
        "AND target._record_hash = source._record_hash"
    )
    target.alias("target").merge(
        new_rows.alias("source"), condition
    ).whenNotMatchedInsertAll().execute()
    new_rows.unpersist()
    return rows_to_write


def merge_quarantine_idempotently(spark: Any, dataframe: Any, target_path: str) -> int:
    from delta.tables import DeltaTable

    keys = ["_source_file_hash", "_record_hash", "_quarantine_reason"]
    staged = dataframe.dropDuplicates(keys)
    if not DeltaTable.isDeltaTable(spark, target_path):
        rows_to_write = int(staged.count())
        (
            staged.write.format("delta")
            .mode("append")
            .partitionBy("_source_name", "_ingestion_date")
            .save(target_path)
        )
        return rows_to_write
    target = DeltaTable.forPath(spark, target_path)
    new_rows = staged.join(target.toDF().select(*keys), on=keys, how="left_anti").cache()
    rows_to_write = int(new_rows.count())
    if rows_to_write:
        condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
        target.alias("target").merge(
            new_rows.alias("source"), condition
        ).whenNotMatchedInsertAll().execute()
    new_rows.unpersist()
    return rows_to_write


def ingest_batch(
    spark: Any,
    *,
    source_path: str,
    contract: SourceContract,
    source_file_hash: str,
    run_id: str,
    target_path: str,
    quarantine_path: str,
    audit_path: str,
) -> IngestionAudit:
    started_at = datetime.now(UTC)
    try:
        raw = read_source_csv(spark, source_path, contract)
        enriched = enrich_with_metadata(
            raw,
            contract=contract,
            run_id=run_id,
            source_file_hash=source_file_hash,
            ingested_at=started_at,
        ).cache()
        rows_read = enriched.count()
        valid, quarantine = split_valid_and_quarantine(enriched, contract)
        rows_quarantined = quarantine.count()
        if rows_quarantined:
            merge_quarantine_idempotently(spark, quarantine, quarantine_path)
        rows_written = merge_idempotently(spark, valid, target_path)
        enriched.unpersist()
        audit = IngestionAudit(
            run_id=run_id,
            source_name=contract.name,
            target=target_path,
            status="SUCCEEDED",
            started_at=started_at,
            ended_at=datetime.now(UTC),
            rows_read=rows_read,
            rows_written=rows_written,
            rows_quarantined=rows_quarantined,
            source_file_hash=source_file_hash,
        )
    except Exception as error:
        audit = IngestionAudit.failed(
            run_id=run_id,
            source_name=contract.name,
            target=target_path,
            started_at=started_at,
            source_file_hash=source_file_hash,
            error=error,
        )
        append_audit(spark, audit, audit_path)
        raise
    append_audit(spark, audit, audit_path)
    return audit
