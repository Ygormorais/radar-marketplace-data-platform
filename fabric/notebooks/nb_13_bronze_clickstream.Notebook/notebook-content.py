# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
bootstrap_servers = "localhost:19092"
topic = "clickstream-events-v1"
target_path = "Tables/bronze_clickstream_events"
quarantine_path = "Tables/bronze_quarantine_events"
checkpoint_root = "Files/checkpoints/bronze"
watermark_delay = "2 hours"
trigger_interval = "30 seconds"
trigger_mode = "available_now"
starting_offsets = "latest"
run_id = "manual"
audit_path = "Tables/ctl_streaming_runs"

# CELL ********************
from radar.bronze.clickstream import parse_clickstream, start_clickstream
from radar.bronze.streaming import read_kafka_stream

# CELL ********************
raw_stream = read_kafka_stream(
    spark,
    bootstrap_servers=bootstrap_servers,
    topic=topic,
    starting_offsets=starting_offsets,
)
parsed = parse_clickstream(raw_stream)
valid_query, invalid_query = start_clickstream(
    parsed,
    target_path=target_path,
    quarantine_path=quarantine_path,
    checkpoint_root=checkpoint_root,
    watermark_delay=watermark_delay,
    trigger_interval=trigger_interval,
    trigger_mode=trigger_mode,
)
print(f"Consultas iniciadas: {valid_query.id}, {invalid_query.id}")

# CELL ********************
from radar.observability.streaming import await_and_record_streams

outcomes = await_and_record_streams(
    spark,
    queries=(valid_query, invalid_query),
    run_id=run_id,
    audit_path=audit_path,
)
notebookutils.notebook.exit(outcomes)
