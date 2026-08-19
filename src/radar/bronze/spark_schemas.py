"""Conversão dos contratos lógicos para schemas Spark explícitos."""

from __future__ import annotations

from typing import Any

from radar.contracts.sources import LogicalType, SourceContract


def source_struct_type(contract: SourceContract) -> Any:
    """Cria StructType tardiamente para manter o core importável sem PySpark."""

    from pyspark.sql.types import (
        DecimalType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_mapping = {
        LogicalType.STRING: StringType(),
        LogicalType.INTEGER: IntegerType(),
        LogicalType.DECIMAL_18_2: DecimalType(18, 2),
        LogicalType.DOUBLE: DoubleType(),
        LogicalType.TIMESTAMP: TimestampType(),
    }
    fields = [
        StructField(column.name, type_mapping[column.logical_type], column.nullable)
        for column in contract.columns
    ]
    fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)


def delivery_event_struct_type() -> Any:
    from pyspark.sql.types import (
        IntegerType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("schema_version", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("status", StringType(), False),
            StructField("occurred_at", TimestampType(), False),
            StructField("produced_at", TimestampType(), False),
            StructField("location_state", StringType(), False),
            StructField("carrier_code", StringType(), False),
            StructField("sequence_number", IntegerType(), False),
            StructField("attributes", MapType(StringType(), StringType()), True),
        ]
    )


def clickstream_event_struct_type() -> Any:
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("schema_version", StringType(), False),
            StructField("event_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("anonymous_id", StringType(), True),
            StructField("session_source_id", StringType(), True),
            StructField("event_type", StringType(), False),
            StructField("occurred_at", TimestampType(), False),
            StructField("product_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("device_type", StringType(), True),
            StructField("traffic_source", StringType(), True),
        ]
    )
