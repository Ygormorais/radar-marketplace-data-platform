# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
bronze_root = "Tables"
silver_root = "Tables"
inactivity_minutes = 30

# CELL ********************
from radar.silver.sessionization import sessionize_clickstream

# CELL ********************
events = spark.read.format("delta").load(f"{bronze_root}/bronze_clickstream_events")
sessions = sessionize_clickstream(events, inactivity_minutes=inactivity_minutes)
(
    sessions.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{silver_root}/silver_clickstream_sessions")
)
