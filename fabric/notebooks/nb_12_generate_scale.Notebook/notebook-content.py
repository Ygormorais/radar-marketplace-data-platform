# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
order_count = 1_000_000
seed = 20260818
partitions = 200
output_path = "Files/landing/synthetic/delivery_events/v1"

# CELL ********************
from radar.generators.spark_events import generate_delivery_events_spark

# CELL ********************
events = generate_delivery_events_spark(
    spark,
    order_count=order_count,
    seed=seed,
    partitions=partitions,
)
expected_count = order_count * 7
actual_count = events.count()
if actual_count != expected_count:
    raise RuntimeError(f"Cardinalidade inesperada: esperado={expected_count}; atual={actual_count}")

# CELL ********************
(
    events.repartition(partitions, "occurred_at")
    .write.mode("overwrite")
    .format("json")
    .save(output_path)
)
print({"orders": order_count, "events": actual_count, "seed": seed, "output": output_path})
