# monitoring/metrics.py
"""
Pipeline Metrics Collector
============================
Captures quantitative measurements for every pipeline run.
Writes metrics to Snowflake for historical analysis and
to an in-memory store for real-time dashboard queries.

WHAT WE MEASURE:
  Throughput   — records per second processed
  Latency      — seconds from Kafka publish to Snowflake loaded
  Error rate   — % of records that failed validation
  Consumer lag — how many Kafka messages are waiting unprocessed
  Freshness    — minutes since last record landed in each layer

WHY STORE METRICS IN SNOWFLAKE:
  If metrics live only in memory (or only in Airflow),
  they disappear when the container restarts.
  In Snowflake, you can query: "Was our pipeline slower last Tuesday?"
  This is critical for SLA reporting to stakeholders.

USAGE:
  from monitoring.metrics import MetricsCollector
  metrics = MetricsCollector()
  with metrics.measure_run("flight_ingestion_dag", run_id="abc"):
      process_data()
  # Automatically records duration, records, errors
"""

import os
import time
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RunMetrics:
    """
    Snapshot of all metrics for one pipeline run.
    Dataclass automatically generates __init__, __repr__, __eq__.
    """
    run_id              : str
    dag_id              : str
    task_id             : str   = ""
    started_at          : datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at         : Optional[datetime] = None
    records_read        : int   = 0
    records_written     : int   = 0
    records_failed      : int   = 0
    records_flagged     : int   = 0
    duplicates_removed  : int   = 0
    elapsed_seconds     : float = 0.0
    throughput_rps      : float = 0.0     # Records per second
    error_rate_pct      : float = 0.0
    status              : str   = "running"    # running / success / failed
    error_message       : Optional[str] = None
    extra               : dict  = field(default_factory=dict)

    def finalise(self, status: str = "success", error: Optional[str] = None):
        """Mark the run as complete and compute derived metrics."""
        self.finished_at      = datetime.now(timezone.utc)
        self.status           = status
        self.error_message    = error
        self.elapsed_seconds  = (
            self.finished_at - self.started_at
        ).total_seconds()
        self.throughput_rps   = round(
            self.records_written / max(self.elapsed_seconds, 0.001), 2
        )
        self.error_rate_pct   = round(
            self.records_failed / max(self.records_read, 1) * 100, 4
        )

    def to_dict(self) -> dict:
        return {
            "run_id"            : self.run_id,
            "dag_id"            : self.dag_id,
            "task_id"           : self.task_id,
            "started_at"        : self.started_at.isoformat(),
            "finished_at"       : self.finished_at.isoformat() if self.finished_at else None,
            "records_read"      : self.records_read,
            "records_written"   : self.records_written,
            "records_failed"    : self.records_failed,
            "records_flagged"   : self.records_flagged,
            "duplicates_removed": self.duplicates_removed,
            "elapsed_seconds"   : self.elapsed_seconds,
            "throughput_rps"    : self.throughput_rps,
            "error_rate_pct"    : self.error_rate_pct,
            "status"            : self.status,
            "error_message"     : self.error_message,
        }


class MetricsCollector:
    """
    Collects pipeline run metrics and persists them to Snowflake.
    Thread-safe: each call creates its own RunMetrics instance.
    """

    METRICS_TABLE = "FLIGHT_DB.RAW.PIPELINE_RUN_METRICS"

    def __init__(self):
        # In-memory cache of recent runs for dashboard
        self._recent_runs: list[RunMetrics] = []
        self._max_cache   = 1000

    @contextmanager
    def measure_run(
        self,
        dag_id  : str,
        run_id  : str,
        task_id : str = "",
    ):
        """
        Context manager that automatically times a pipeline run
        and records metrics whether it succeeds or fails.

        USAGE:
            with metrics.measure_run("ingestion_dag", run_id) as m:
                m.records_read = fetch_from_kafka()
                m.records_written = load_to_snowflake()
            # Metrics saved automatically on exit
        """
        run = RunMetrics(run_id=run_id, dag_id=dag_id, task_id=task_id)
        logger.info(
            "Metrics: run started",
            extra={"run_id": run_id, "dag_id": dag_id, "task_id": task_id}
        )
        try:
            yield run                    # Code inside 'with' block runs here
            run.finalise(status="success")
        except Exception as e:
            run.finalise(status="failed", error=str(e)[:1000])
            logger.error(
                "Metrics: run FAILED",
                extra={"run_id": run_id, "error": str(e)[:500]}
            )
            raise                        # Re-raise so Airflow sees the failure
        finally:
            # Always save metrics — even on failure
            self._save_metrics(run)
            self._cache_run(run)
            logger.info(
                "Metrics: run complete",
                extra=run.to_dict()
            )

    def record_kafka_lag(self, topic: str, lag: int) -> None:
        """
        Record how many messages are waiting in Kafka unprocessed.
        High lag = consumer is falling behind producer.
        Alert if lag > 1000 messages (configurable threshold).
        """
        logger.info(
            "Kafka consumer lag",
            extra={"topic": topic, "consumer_lag_messages": lag}
        )
        if lag > int(os.getenv("KAFKA_LAG_ALERT_THRESHOLD", "1000")):
            from monitoring.alerts import get_alert_manager
            get_alert_manager().send_warning(
                f"Kafka consumer lag is high: {lag:,} messages behind",
                topic           = topic,
                lag_messages    = lag,
                action_required = (
                    "Consumer may be processing slowly or is down. "
                    "Check flight_kafka_consumer container logs."
                ),
            )

    def get_recent_runs(self, dag_id: Optional[str] = None, n: int = 20) -> list[dict]:
        """Return the most recent N run metrics from the in-memory cache."""
        runs = self._recent_runs
        if dag_id:
            runs = [r for r in runs if r.dag_id == dag_id]
        return [r.to_dict() for r in runs[-n:]]

    def get_throughput_trend(self, dag_id: str, n: int = 10) -> dict:
        """
        Compute throughput trend for the last N runs.
        Used by the monitoring dashboard to show if performance is changing.
        """
        recent = self.get_recent_runs(dag_id=dag_id, n=n)
        if not recent:
            return {"avg_rps": 0, "trend": "no_data", "runs": 0}

        rps_values = [r["throughput_rps"] for r in recent if r["throughput_rps"] > 0]
        if len(rps_values) < 2:
            return {"avg_rps": rps_values[0] if rps_values else 0, "trend": "stable", "runs": 1}

        avg_rps    = sum(rps_values) / len(rps_values)
        # Compare last 3 runs to previous 3 runs
        half       = len(rps_values) // 2
        recent_avg = sum(rps_values[half:]) / len(rps_values[half:])
        older_avg  = sum(rps_values[:half])  / len(rps_values[:half])

        trend = (
            "improving"  if recent_avg > older_avg * 1.10 else
            "degrading"  if recent_avg < older_avg * 0.90 else
            "stable"
        )
        return {
            "avg_rps"   : round(avg_rps, 2),
            "trend"     : trend,
            "runs"      : len(rps_values),
            "recent_avg": round(recent_avg, 2),
            "older_avg" : round(older_avg, 2),
        }

    # ── Private helpers ───────────────────────────────────────────────────────

    def _save_metrics(self, run: RunMetrics) -> None:
        """Persist metrics to Snowflake PIPELINE_RUN_METRICS table."""
        try:
            from snowflake.connection import SnowflakeConnection
            with SnowflakeConnection(database="FLIGHT_DB", schema="RAW") as sf:
                sf.execute(
                    f"""
                    INSERT INTO {self.METRICS_TABLE} (
                        run_id, dag_id, task_id,
                        started_at, finished_at,
                        records_read, records_written, records_failed,
                        records_flagged, duplicates_removed,
                        elapsed_seconds, throughput_rps,
                        error_rate_pct, status, error_message
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    """,
                    params=(
                        run.run_id, run.dag_id, run.task_id,
                        run.started_at, run.finished_at,
                        run.records_read, run.records_written, run.records_failed,
                        run.records_flagged, run.duplicates_removed,
                        run.elapsed_seconds, run.throughput_rps,
                        run.error_rate_pct, run.status, run.error_message,
                    )
                )
        except Exception as e:
            # Metrics save failure should NEVER crash the pipeline
            logger.warning("Could not save metrics to Snowflake: %s", e)

    def _cache_run(self, run: RunMetrics) -> None:
        """Add run to in-memory cache, trim if over limit."""
        self._recent_runs.append(run)
        if len(self._recent_runs) > self._max_cache:
            self._recent_runs = self._recent_runs[-self._max_cache:]