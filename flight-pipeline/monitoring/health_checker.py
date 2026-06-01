# monitoring/health_checker.py
"""
Pipeline Health Checker
========================
Runs comprehensive health checks across all pipeline components.
Called by Airflow validation DAG and standalone monitoring scripts.

CHECKS PERFORMED:
  Component checks:
    1. Kafka broker reachable?
    2. PostgreSQL connection working?
    3. Snowflake connection working?
    4. Airflow scheduler running?

  Data checks:
    5. RAW layer freshness (< 15 min lag?)
    6. CLEAN layer freshness (< 30 min lag?)
    7. ANALYTICS layer freshness (< 60 min lag?)
    8. Quality rate > 95%?
    9. DLQ empty or low volume?
    10. Kafka consumer lag acceptable?

RESULT FORMAT:
  Each check returns: {
    "name": str,
    "status": "healthy" | "degraded" | "down",
    "message": str,
    "value": any,      # The measured value (e.g., lag_minutes=12)
    "threshold": any,  # What we consider healthy (e.g., 30)
  }

USAGE:
  from monitoring.health_checker import HealthChecker
  checker = HealthChecker()
  report  = checker.run_all_checks()
  print(report.summary())
"""

import os
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Result of one health check."""
    name      : str
    status    : str      # "healthy", "degraded", "down"
    message   : str
    value     : object   = None
    threshold : object   = None
    checked_at: str      = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def is_healthy(self) -> bool:
        return self.status == "healthy"

    def to_dict(self) -> dict:
        return {
            "name"      : self.name,
            "status"    : self.status,
            "message"   : self.message,
            "value"     : self.value,
            "threshold" : self.threshold,
            "checked_at": self.checked_at,
        }


@dataclass
class HealthReport:
    """Aggregated result of all health checks."""
    checks     : list[CheckResult] = field(default_factory=list)
    checked_at : str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def overall_status(self) -> str:
        if any(c.status == "down" for c in self.checks):
            return "DOWN"
        if any(c.status == "degraded" for c in self.checks):
            return "DEGRADED"
        return "HEALTHY"

    @property
    def healthy_count(self) -> int:
        return sum(1 for c in self.checks if c.is_healthy)

    @property
    def total_count(self) -> int:
        return len(self.checks)

    def summary(self) -> dict:
        return {
            "overall_status" : self.overall_status,
            "healthy"        : self.healthy_count,
            "total"          : self.total_count,
            "checked_at"     : self.checked_at,
            "checks"         : [c.to_dict() for c in self.checks],
        }

    def print_report(self) -> None:
        status_icons = {"healthy": "✅", "degraded": "⚠️", "down": "🔴"}
        print(f"\n{'='*60}")
        print(
            f"  PIPELINE HEALTH REPORT — {self.overall_status} "
            f"({self.healthy_count}/{self.total_count} checks healthy)"
        )
        print(f"  {self.checked_at}")
        print(f"{'='*60}")
        for check in self.checks:
            icon = status_icons.get(check.status, "?")
            val  = f" | value={check.value}" if check.value is not None else ""
            print(f"  {icon} {check.name:<40} {val}")
            if not check.is_healthy:
                print(f"     → {check.message}")
        print(f"{'='*60}\n")


class HealthChecker:
    """
    Runs all health checks and returns a HealthReport.
    Each check is independent — one failure doesn't stop others.
    """

    def run_all_checks(self) -> HealthReport:
        """Run all checks and return the combined report."""
        report  = HealthReport()
        checkers = [
            self._check_kafka,
            self._check_postgres,
            self._check_snowflake,
            self._check_raw_freshness,
            self._check_clean_freshness,
            self._check_analytics_freshness,
            self._check_data_quality_rate,
            self._check_dlq_volume,
            self._check_kafka_consumer_lag,
        ]
        for check_fn in checkers:
            try:
                result = check_fn()
                report.checks.append(result)
                log_fn = logger.info if result.is_healthy else logger.warning
                log_fn(
                    "Health check: %s | status=%s | value=%s",
                    result.name, result.status, result.value
                )
            except Exception as e:
                report.checks.append(CheckResult(
                    name    = check_fn.__name__.replace("_check_", ""),
                    status  = "down",
                    message = f"Check itself failed: {e}",
                ))
                logger.error("Health check %s raised exception: %s", check_fn.__name__, e)

        return report

    # ── Component checks ──────────────────────────────────────────────────────

    def _check_kafka(self) -> CheckResult:
        """Verify Kafka broker is reachable and topics exist."""
        try:
            from kafka import KafkaAdminClient
            from kafka.errors import NoBrokersAvailable

            admin = KafkaAdminClient(
                bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                request_timeout_ms= 5000,
            )
            topics = admin.list_topics()
            admin.close()

            topic_raw   = os.getenv("KAFKA_TOPIC_RAW", "flights_raw")
            has_topic   = topic_raw in topics

            return CheckResult(
                name      = "kafka_broker",
                status    = "healthy" if has_topic else "degraded",
                message   = (
                    f"Kafka reachable. Topic '{topic_raw}' exists."
                    if has_topic else
                    f"Kafka reachable but topic '{topic_raw}' not found."
                ),
                value     = f"{len(topics)} topics",
            )
        except Exception as e:
            return CheckResult(
                name    = "kafka_broker",
                status  = "down",
                message = f"Cannot reach Kafka: {e}",
            )

    def _check_postgres(self) -> CheckResult:
        """Verify PostgreSQL is reachable and staging schema exists."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host    = os.getenv("POSTGRES_HOST", "localhost"),
                port    = int(os.getenv("POSTGRES_PORT", "5433")),
                user    = os.getenv("POSTGRES_USER", "airflow"),
                password= os.getenv("POSTGRES_PASSWORD", "airflow"),
                dbname  = os.getenv("POSTGRES_DB", "airflow"),
                connect_timeout=5,
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM staging.flights_raw WHERE processed=FALSE"
            )
            pending = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return CheckResult(
                name    = "postgresql",
                status  = "healthy",
                message = f"PostgreSQL healthy. {pending} records pending processing.",
                value   = f"{pending} pending records",
            )
        except Exception as e:
            return CheckResult(
                name    = "postgresql",
                status  = "down",
                message = f"PostgreSQL unreachable: {e}",
            )

    def _check_snowflake(self) -> CheckResult:
        """Verify Snowflake connection and warehouse are active."""
        try:
            from snowflake.connection import SnowflakeConnection
            with SnowflakeConnection() as sf:
                result = sf.execute(
                    "SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), "
                    "CURRENT_TIMESTAMP() AS ts"
                )
            wh = result[0].get("CURRENT_WAREHOUSE()", "unknown")
            return CheckResult(
                name    = "snowflake",
                status  = "healthy",
                message = f"Snowflake connected. Warehouse: {wh}",
                value   = wh,
            )
        except Exception as e:
            return CheckResult(
                name    = "snowflake",
                status  = "down",
                message = f"Snowflake unreachable: {e}",
            )

    # ── Data freshness checks ─────────────────────────────────────────────────

    def _check_raw_freshness(self) -> CheckResult:
        return self._check_layer_freshness(
            layer       = "RAW",
            table       = "FLIGHT_DB.RAW.FLIGHTS_RAW",
            ts_column   = "loaded_at",
            threshold   = int(os.getenv("RAW_FRESHNESS_THRESHOLD_MINS", "15")),
        )

    def _check_clean_freshness(self) -> CheckResult:
        return self._check_layer_freshness(
            layer       = "CLEAN",
            table       = "FLIGHT_DB.CLEAN.FLIGHTS_CLEAN",
            ts_column   = "transformed_at",
            threshold   = int(os.getenv("CLEAN_FRESHNESS_THRESHOLD_MINS", "30")),
        )

    def _check_analytics_freshness(self) -> CheckResult:
        return self._check_layer_freshness(
            layer       = "ANALYTICS",
            table       = "FLIGHT_DB.ANALYTICS.FACT_FLIGHTS",
            ts_column   = "transformed_at",
            threshold   = int(os.getenv("ANALYTICS_FRESHNESS_THRESHOLD_MINS", "60")),
        )

    def _check_layer_freshness(
        self,
        layer     : str,
        table     : str,
        ts_column : str,
        threshold : int,
    ) -> CheckResult:
        """Generic freshness check for any Snowflake layer."""
        try:
            from snowflake.connection import SnowflakeConnection
            with SnowflakeConnection() as sf:
                result = sf.execute(
                    f"""
                    SELECT DATEDIFF(minute, MAX({ts_column}), CURRENT_TIMESTAMP()) AS lag
                    FROM {table}
                    """
                )
            lag = result[0].get("LAG") if result else None
            if lag is None:
                return CheckResult(
                    name    = f"{layer.lower()}_freshness",
                    status  = "degraded",
                    message = f"{layer} table appears empty.",
                    value   = "no data",
                )
            status = (
                "healthy"  if lag <= threshold else
                "degraded" if lag <= threshold * 2 else
                "down"
            )
            return CheckResult(
                name      = f"{layer.lower()}_freshness",
                status    = status,
                message   = (
                    f"{layer} layer is {lag} minutes old "
                    f"(threshold: {threshold} minutes)."
                ),
                value     = f"{lag} minutes",
                threshold = f"{threshold} minutes",
            )
        except Exception as e:
            return CheckResult(
                name    = f"{layer.lower()}_freshness",
                status  = "down",
                message = f"Could not check {layer} freshness: {e}",
            )

    # ── Quality checks ────────────────────────────────────────────────────────

    def _check_data_quality_rate(self) -> CheckResult:
        """Check what % of today's CLEAN records are quality-flagged."""
        threshold = float(os.getenv("QUALITY_FLAG_THRESHOLD_PCT", "5.0"))
        try:
            from snowflake.connection import SnowflakeConnection
            with SnowflakeConnection() as sf:
                result = sf.execute("""
                    SELECT
                        ROUND(
                            SUM(IFF(data_quality_flag=TRUE,1,0))::FLOAT
                            / NULLIF(COUNT(*),0) * 100,
                        2) AS flag_rate,
                        COUNT(*) AS total
                    FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                    WHERE DATE(transformed_at) = CURRENT_DATE()
                """)
            flag_rate = float(result[0].get("FLAG_RATE") or 0)
            total     = int(result[0].get("TOTAL") or 0)
            status    = (
                "healthy"  if flag_rate <= threshold else
                "degraded" if flag_rate <= threshold * 2 else
                "down"
            )
            return CheckResult(
                name      = "data_quality_rate",
                status    = status,
                message   = (
                    f"{flag_rate:.1f}% of {total:,} records flagged today "
                    f"(threshold: {threshold}%)."
                ),
                value     = f"{flag_rate:.1f}%",
                threshold = f"{threshold}%",
            )
        except Exception as e:
            return CheckResult(
                name    = "data_quality_rate",
                status  = "down",
                message = f"Could not check quality rate: {e}",
            )

    def _check_dlq_volume(self) -> CheckResult:
        """Check how many messages are stuck in the Dead Letter Queue."""
        threshold = int(os.getenv("DLQ_ALERT_THRESHOLD", "50"))
        try:
            from monitoring.dead_letter_queue import DeadLetterQueue
            dlq   = DeadLetterQueue()
            stats = dlq.get_dlq_stats()
            pending = next(
                (s["count"] for s in stats.get("by_status", [])
                 if s["status"] == "pending"),
                0
            )
            return CheckResult(
                name      = "dead_letter_queue",
                status    = (
                    "healthy"  if pending == 0 else
                    "degraded" if pending < threshold else
                    "down"
                ),
                message   = f"{pending} messages in DLQ (threshold: {threshold}).",
                value     = pending,
                threshold = threshold,
            )
        except Exception as e:
            return CheckResult(
                name    = "dead_letter_queue",
                status  = "degraded",
                message = f"Could not check DLQ: {e}",
            )

    def _check_kafka_consumer_lag(self) -> CheckResult:
        """Check how far behind the Kafka consumer is."""
        threshold = int(os.getenv("KAFKA_LAG_ALERT_THRESHOLD", "1000"))
        try:
            from kafka import KafkaConsumer, TopicPartition
            topic   = os.getenv("KAFKA_TOPIC_RAW", "flights_raw")
            servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            group   = os.getenv("KAFKA_GROUP_ID", "flight_consumers")

            consumer = KafkaConsumer(
                bootstrap_servers = servers,
                group_id          = group,
                enable_auto_commit= False,
            )
            # Get end offsets (latest message position per partition)
            partitions    = consumer.partitions_for_topic(topic) or set()
            tps           = [TopicPartition(topic, p) for p in partitions]
            end_offsets   = consumer.end_offsets(tps)
            # Get committed offsets (last processed position per partition)
            total_lag     = 0
            for tp in tps:
                committed = consumer.committed(tp)
                end       = end_offsets.get(tp, 0)
                lag       = end - (committed or 0)
                total_lag += max(lag, 0)
            consumer.close()

            return CheckResult(
                name      = "kafka_consumer_lag",
                status    = (
                    "healthy"  if total_lag <= threshold // 10 else
                    "degraded" if total_lag <= threshold else
                    "down"
                ),
                message   = (
                    f"Consumer lag: {total_lag:,} messages "
                    f"(threshold: {threshold:,})."
                ),
                value     = total_lag,
                threshold = threshold,
            )
        except Exception as e:
            return CheckResult(
                name    = "kafka_consumer_lag",
                status  = "degraded",
                message = f"Could not measure consumer lag: {e}",
            )