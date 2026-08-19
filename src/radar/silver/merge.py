"""Primitivas Delta para CDC current-state e dimensões SCD tipo 2."""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from operator import and_
from typing import Any


def _hash_columns(dataframe: Any, columns: list[str]) -> Any:
    from pyspark.sql import functions as F

    values = [F.coalesce(F.col(name).cast("string"), F.lit("∅")) for name in columns]
    return F.sha2(F.concat_ws("\u001f", *values), 256)


def _require_columns(dataframe: Any, columns: list[str]) -> None:
    missing = sorted(set(columns) - set(dataframe.columns))
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")


def merge_current_state(
    spark: Any,
    dataframe: Any,
    *,
    target_path: str,
    business_keys: list[str],
    sequence_column: str,
) -> None:
    """Mantém a versão mais recente por chave, protegendo contra CDC fora de ordem."""

    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    _require_columns(dataframe, [*business_keys, sequence_column])
    tracked = sorted(column for column in dataframe.columns if not column.startswith("_"))
    window = Window.partitionBy(*business_keys).orderBy(F.col(sequence_column).desc_nulls_last())
    source = (
        dataframe.withColumn("_cdc_rank", F.row_number().over(window))
        .filter(F.col("_cdc_rank") == 1)
        .drop("_cdc_rank")
        .withColumn("_change_hash", _hash_columns(dataframe, tracked))
    )
    if not DeltaTable.isDeltaTable(spark, target_path):
        source.write.format("delta").mode("append").save(target_path)
        return
    condition = " AND ".join(f"target.{key} = source.{key}" for key in business_keys)
    target = DeltaTable.forPath(spark, target_path)
    target.alias("target").merge(source.alias("source"), condition).whenMatchedUpdateAll(
        condition=(
            f"(target.{sequence_column} IS NULL OR "
            f"source.{sequence_column} >= target.{sequence_column}) "
            "AND source._change_hash <> target._change_hash"
        )
    ).whenNotMatchedInsertAll().execute()


@dataclass(frozen=True)
class SCD2MergeResult:
    late_arrivals: Any
    changed_records: Any


def apply_scd2(
    spark: Any,
    dataframe: Any,
    *,
    target_path: str,
    business_keys: list[str],
    tracked_columns: list[str],
    effective_at_column: str,
    run_id: str,
) -> SCD2MergeResult:
    """Aplica SCD2 atomicamente; versões anteriores à corrente são devolvidas para tratamento."""

    from delta.tables import DeltaTable
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    required = [*business_keys, *tracked_columns, effective_at_column]
    _require_columns(dataframe, required)
    window = Window.partitionBy(*business_keys).orderBy(F.col(effective_at_column).desc())
    incoming = (
        dataframe.withColumn("_batch_rank", F.row_number().over(window))
        .filter(F.col("_batch_rank") == 1)
        .drop("_batch_rank")
        .withColumn("attribute_hash", _hash_columns(dataframe, tracked_columns))
        .withColumn("valid_from", F.col(effective_at_column))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("created_run_id", F.lit(run_id))
        .withColumn("updated_run_id", F.lit(run_id))
    )
    if not DeltaTable.isDeltaTable(spark, target_path):
        incoming.write.format("delta").mode("append").save(target_path)
        empty = incoming.limit(0).withColumn("_quarantine_reason", F.lit(None).cast("string"))
        return SCD2MergeResult(late_arrivals=empty, changed_records=incoming)

    target = DeltaTable.forPath(spark, target_path)
    current = (
        target.toDF()
        .filter(F.col("is_current"))
        .select(
            *[F.col(key).alias(f"_target_key_{key}") for key in business_keys],
            F.col("attribute_hash").alias("_target_hash"),
            F.col("valid_from").alias("_target_valid_from"),
        )
    )
    join_condition = reduce(
        and_,
        (incoming[key] == current[f"_target_key_{key}"] for key in business_keys),
    )
    compared = incoming.join(current, join_condition, "left")
    is_existing = F.col("_target_hash").isNotNull()
    is_changed = is_existing & (F.col("attribute_hash") != F.col("_target_hash"))
    is_late = is_changed & (F.col("valid_from") <= F.col("_target_valid_from"))
    late = compared.filter(is_late).withColumn(
        "_quarantine_reason", F.lit("LATE_ARRIVING_SCD2_VERSION")
    )
    eligible = compared.filter(~is_late)
    comparison_columns = [
        "_target_hash",
        "_target_valid_from",
        *[f"_target_key_{key}" for key in business_keys],
    ]
    changed = eligible.filter(is_changed).drop(*comparison_columns)
    new = eligible.filter(~is_existing).drop(*comparison_columns)
    changes_to_insert = changed.unionByName(new)

    merge_key_columns = [f"_merge_{key}" for key in business_keys]
    expire_rows = changed
    for key, merge_key in zip(business_keys, merge_key_columns, strict=True):
        expire_rows = expire_rows.withColumn(merge_key, F.col(key))
    insert_rows = changes_to_insert
    for key, merge_key in zip(business_keys, merge_key_columns, strict=True):
        insert_rows = insert_rows.withColumn(
            merge_key, F.lit(None).cast(dataframe.schema[key].dataType)
        )
    staged = expire_rows.unionByName(insert_rows)

    merge_condition = " AND ".join(
        f"target.{key} = source.{merge_key}"
        for key, merge_key in zip(business_keys, merge_key_columns, strict=True)
    )
    target_columns = [field.name for field in target.toDF().schema.fields]
    insert_values: dict[str, Any] = {column: f"source.{column}" for column in target_columns}
    target.alias("target").merge(staged.alias("source"), merge_condition).whenMatchedUpdate(
        condition="target.is_current = true AND target.attribute_hash <> source.attribute_hash",
        set={
            "is_current": "false",
            "valid_to": "source.valid_from",
            "updated_run_id": "source.updated_run_id",
        },
    ).whenNotMatchedInsert(values=insert_values).execute()
    return SCD2MergeResult(late_arrivals=late, changed_records=changes_to_insert)
