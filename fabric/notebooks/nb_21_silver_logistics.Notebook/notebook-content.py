# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
run_id = "manual"
bronze_root = "Tables"
silver_root = "Tables"

# CELL ********************
from radar.silver.merge import merge_current_state
from radar.silver.reconciliation import (
    build_delivery_snapshot,
    classify_delivery_transitions,
)

# CELL ********************
orders = spark.read.format("delta").load(f"{silver_root}/silver_orders")
events = spark.read.format("delta").load(f"{bronze_root}/bronze_delivery_events")
classified = classify_delivery_transitions(events)
snapshot = build_delivery_snapshot(orders, events)

# CELL ********************
merge_current_state(
    spark,
    classified,
    target_path=f"{silver_root}/silver_delivery_event_history",
    business_keys=["event_id"],
    sequence_column="produced_at",
)
merge_current_state(
    spark,
    snapshot,
    target_path=f"{silver_root}/silver_delivery_snapshot",
    business_keys=["order_id"],
    sequence_column="latest_event_received_at",
)
