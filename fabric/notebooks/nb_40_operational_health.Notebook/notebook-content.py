# Fabric notebook source

# METADATA ********************
# META {"kernel_info":{"name":"synapse_pyspark"},"language_info":{"name":"python"}}

# PARAMETERS CELL ********************
run_id = "manual"
streaming_audit_path = "Tables/ctl_streaming_runs"
quality_results_path = "Tables/ctl_data_quality_results"
quarantine_path = "Tables/bronze_quarantine_events"
alerts_path = "Tables/ctl_operational_alerts"
max_heartbeat_age_minutes = 20
max_quarantine_rate = 0.02
fail_on_critical = False

# CELL ********************
import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from pyspark.sql import functions as F

from radar.observability.health import AlertSeverity, HealthSnapshot, evaluate_health

# CELL ********************
observed_at = datetime.now(UTC)
window_start = observed_at - timedelta(hours=24)
streaming = (
    spark.read.format("delta")
    .load(streaming_audit_path)
    .filter(F.col("recorded_at") >= F.lit(window_start))
)
stream_metrics = streaming.agg(
    F.max("recorded_at").alias("latest_stream_heartbeat"),
    F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("streaming_failures"),
    F.sum("input_rows").alias("input_rows"),
).first()

quarantined_rows = (
    spark.read.format("delta")
    .load(quarantine_path)
    .filter(F.col("_ingested_at") >= F.lit(window_start))
    .count()
)
input_rows = int(stream_metrics["input_rows"] or 0)
quarantine_rate = quarantined_rows / max(input_rows + quarantined_rows, 1)

failed_quality_checks = 0
if spark.catalog.tableExists("ctl_data_quality_results"):
    failed_quality_checks = (
        spark.read.format("delta")
        .load(quality_results_path)
        .filter((F.col("run_id") == run_id) & (~F.col("success")))
        .count()
    )

snapshot = HealthSnapshot(
    observed_at=observed_at,
    latest_stream_heartbeat=stream_metrics["latest_stream_heartbeat"],
    streaming_failures=int(stream_metrics["streaming_failures"] or 0),
    quarantine_rate=quarantine_rate,
    failed_quality_checks=failed_quality_checks,
)
alerts = evaluate_health(
    snapshot,
    max_heartbeat_age_minutes=max_heartbeat_age_minutes,
    max_quarantine_rate=max_quarantine_rate,
)

# CELL ********************
if alerts:
    alert_rows = [
        {
            **asdict(alert),
            "severity": alert.severity.value,
            "run_id": run_id,
            "observed_at": observed_at,
            "delivery_status": "PENDING",
        }
        for alert in alerts
    ]
    spark.createDataFrame(alert_rows).write.format("delta").mode("append").save(alerts_path)

summary = {
    "run_id": run_id,
    "status": (
        "CRITICAL" if any(a.severity == AlertSeverity.CRITICAL for a in alerts) else "HEALTHY"
    ),
    "quarantine_rate": quarantine_rate,
    "alerts": [alert.alert_code for alert in alerts],
}
print(json.dumps(summary, ensure_ascii=False))
if fail_on_critical and summary["status"] == "CRITICAL":
    raise RuntimeError(f"SLO operacional violado: {summary['alerts']}")
notebookutils.notebook.exit(json.dumps(summary))
