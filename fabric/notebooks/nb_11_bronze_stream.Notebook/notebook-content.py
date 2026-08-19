# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
bootstrap_servers = "localhost:19092"
topic = "delivery-events-v1"
starting_offsets = "latest"
target_path = "Tables/bronze_delivery_events"
quarantine_path = "Tables/bronze_quarantine_events"
checkpoint_root = "Files/checkpoints/bronze"
watermark_delay = "2 hours"
trigger_interval = "30 seconds"
trigger_mode = "available_now"
run_id = "manual"
audit_path = "Tables/ctl_streaming_runs"

# CELL ********************
from radar.bronze.streaming import (
    parse_delivery_stream,
    read_kafka_stream,
    start_bronze_streams,
)
from radar.observability.streaming import await_and_record_streams

# CELL ********************
kafka_stream = read_kafka_stream(
    spark,
    bootstrap_servers=bootstrap_servers,
    topic=topic,
    starting_offsets=starting_offsets,
)
parsed_stream = parse_delivery_stream(kafka_stream)
valid_query, quarantine_query = start_bronze_streams(
    parsed_stream,
    target_path=target_path,
    quarantine_path=quarantine_path,
    checkpoint_root=checkpoint_root,
    watermark_delay=watermark_delay,
    trigger_interval=trigger_interval,
    trigger_mode=trigger_mode,
)

# CELL ********************
print(f"Consultas iniciadas: {valid_query.id}, {quarantine_query.id}")
outcomes = await_and_record_streams(
    spark,
    queries=(valid_query, quarantine_query),
    run_id=run_id,
    audit_path=audit_path,
)
notebookutils.notebook.exit(outcomes)
