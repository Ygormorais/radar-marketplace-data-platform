# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
silver_root = "Tables"
payment_tolerance = 0.01

# CELL ********************
from pyspark.sql import functions as F

from radar.silver.merge import merge_current_state
from radar.silver.reconciliation import build_financial_reconciliation

# CELL ********************
orders = spark.read.format("delta").load(f"{silver_root}/silver_orders")
items = spark.read.format("delta").load(f"{silver_root}/silver_order_items")
payments = spark.read.format("delta").load(f"{silver_root}/silver_payments")
reconciliation = build_financial_reconciliation(
    orders,
    items,
    payments,
    tolerance=payment_tolerance,
).withColumn("_reconciled_at", F.current_timestamp())

# CELL ********************
merge_current_state(
    spark,
    reconciliation,
    target_path=f"{silver_root}/silver_financial_reconciliation",
    business_keys=["order_id"],
    sequence_column="_reconciled_at",
)
