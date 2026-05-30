# monitoring/pipeline_monitor.py
"""
Pipeline Monitor — Central Orchestrator
=========================================
Ties together all monitoring components.
Run this standalone for a full system health check.
Also used by the monitoring Airflow DAG.

USAGE:
  # Full health check report
  python -m monitoring.pipeline_monitor

  # Just the KPI summary
  python -m monitoring.pipeline_monitor --mode kpi

  # Trigger all alerts manually (for testing)
  python -m monitoring.pipeline_monitor --mode test-alerts
"""

import sys
import json
import logging
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from monitoring.logging_config import setup_logging, new_correlation_id
from monitoring.health_checker import HealthChecker
from monitoring.alerts        import get_alert_manager
from monitoring.metrics       import MetricsCollector
from monitoring.dead_letter_queue import DeadLetterQueue

setup_logging()
logger = logging.getLogger(__name__)


class PipelineMonitor:
    """Central coordinator for all monitoring activities."""

    def __init__(self):
        self.health_checker = HealthChecker()
        self.alert_manager  = get_alert_manager()
        self.metrics        = MetricsCollector()
        self.dlq            = DeadLetterQueue()

    def run_health_check(self, alert_on_degraded: bool = True) -> dict:
        """
        Run full health check and optionally send alerts.
        Returns the health report as a dict.
        """
        cid = new_correlation_id()
        logger.info("Starting health check | correlation_id=%s", cid)

        report = self.health_checker.run_all_checks()
        report.print_report()

        if alert_on_degraded:
            if report.overall_status == "DOWN":
                self.alert_manager.send_critical(
                    "Pipeline is DOWN — immediate action required",
                    healthy_checks = report.healthy_count,
                    total_checks   = report.total_count,
                    failed_checks  = [
                        c.name for c in report.checks if c.status == "down"
                    ],
                    action_required= (
                        "Check Docker containers: docker compose ps\n"
                        "Check Airflow DAG status: http://localhost:8080\n"
                        "Check Kafka UI: http://localhost:8085"
                    ),
                )
            elif report.overall_status == "DEGRADED":
                self.alert_manager.send_warning(
                    "Pipeline is DEGRADED — performance below threshold",
                    healthy_checks  = report.healthy_count,
                    total_checks    = report.total_count,
                    degraded_checks = [
                        c.name for c in report.checks if c.status == "degraded"
                    ],
                )

        return report.summary()

    def run_daily_summary(self) -> dict:
        """
        Compile and send end-of-day summary.
        Called by Airflow at midnight via a scheduled task.
        """
        logger.info("Compiling daily summary")
        try:
            from snowflake.connection import SnowflakeConnection
            with SnowflakeConnection(database="FLIGHT_DB", schema="ANALYTICS") as sf:
                result = sf.execute("""
                    SELECT
                        COUNT(DISTINCT flight_id)                AS unique_flights,
                        COUNT(*)                                 AS total_events,
                        ROUND(AVG(delay_minutes), 1)             AS avg_delay_mins,
                        ROUND(
                            SUM(IFF(delay_bucket='on_time',1,0))::FLOAT
                            / NULLIF(COUNT(*),0)*100, 1
                        )                                        AS otp_rate_pct,
                        SUM(IFF(status='cancelled',1,0))         AS cancellations,
                        COUNT(DISTINCT airline_iata)             AS active_airlines,
                        COUNT(DISTINCT route_key)                AS routes_flown
                    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
                    WHERE event_date = CURRENT_DATE()
                    AND data_quality_flag = FALSE
                """)

            row = result[0] if result else {}
            summary = {
                "date"            : datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "unique_flights"  : int(row.get("UNIQUE_FLIGHTS") or 0),
                "total_events"    : int(row.get("TOTAL_EVENTS") or 0),
                "avg_delay_mins"  : float(row.get("AVG_DELAY_MINS") or 0),
                "otp_rate_pct"    : float(row.get("OTP_RATE_PCT") or 0),
                "cancellations"   : int(row.get("CANCELLATIONS") or 0),
                "active_airlines" : int(row.get("ACTIVE_AIRLINES") or 0),
                "routes_flown"    : int(row.get("ROUTES_FLOWN") or 0),
                "dlq_stats"       : self.dlq.get_dlq_stats(),
                "throughput_trend": self.metrics.get_throughput_trend(
                    "flight_ingestion_dag"
                ),
            }
            self.alert_manager.send_daily_summary(summary)
            return summary
        except Exception as e:
            logger.error("Daily summary failed: %s", e)
            return {"error": str(e)}

    def test_all_alerts(self) -> None:
        """Send a test message through every alert channel. Use for setup verification."""
        print("\nSending test alerts to all configured channels...")
        self.alert_manager.send_info(
            "Test alert: INFO — pipeline monitoring configured correctly",
            test=True, environment=self.alert_manager.env
        )
        self.alert_manager.send_warning(
            "Test alert: WARNING — this is a test warning",
            test=True
        )
        self.alert_manager.send_error(
            "Test alert: ERROR — this is a test error (not a real error)",
            test=True
        )
        print("Done. Check your Slack channel for 3 test messages.")


# ── Airflow DAG task callables ────────────────────────────────────────────────

def airflow_health_check(**context) -> dict:
    """
    Called by Airflow flight_validation_dag as a PythonOperator.
    Runs health check, pushes results to XCom for downstream tasks.
    """
    from monitoring.logging_config import set_correlation_id
    set_correlation_id(context.get("run_id", "airflow"))

    monitor = PipelineMonitor()
    report  = monitor.run_health_check(alert_on_degraded=True)

    context["ti"].xcom_push(key="health_report", value=report)
    return report


def airflow_daily_summary(**context) -> dict:
    """Called by a scheduled Airflow DAG at midnight."""
    monitor = PipelineMonitor()
    return monitor.run_daily_summary()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flight Pipeline Monitor")
    parser.add_argument(
        "--mode",
        choices=["health", "kpi", "dlq", "daily-summary", "test-alerts"],
        default="health",
        help="What to run (default: health)"
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        help="Run checks without sending alerts"
    )
    args = parser.parse_args()

    monitor = PipelineMonitor()

    if args.mode == "health":
        report = monitor.run_health_check(alert_on_degraded=not args.no_alert)
        sys.exit(0 if report["overall_status"] == "HEALTHY" else 1)

    elif args.mode == "dlq":
        stats   = monitor.dlq.get_dlq_stats()
        pending = monitor.dlq.get_pending_messages(limit=10)
        print(json.dumps({"stats": stats, "pending_sample": pending}, indent=2, default=str))

    elif args.mode == "daily-summary":
        summary = monitor.run_daily_summary()
        print(json.dumps(summary, indent=2, default=str))

    elif args.mode == "test-alerts":
        monitor.test_all_alerts()

    sys.exit(0)