# monitoring/alerts.py
"""
Alerting System
================
Sends notifications when the pipeline encounters problems.

ALERT CHANNELS:
  1. Slack webhook  — real-time team notifications (primary)
  2. Email          — formal escalation (secondary)
  3. Log file       — always written regardless of channel (backup)

ALERT SEVERITY LEVELS:
  INFO     — pipeline milestone (batch complete, daily summary)
  WARNING  — degraded performance (high delay rate, slow ingestion)
  ERROR    — task failed, retrying
  CRITICAL — retry limit exhausted, data loss risk, SLA breach

ALERT DESIGN PRINCIPLE:
  Every alert must answer:
    WHAT happened?    → "Kafka consumer failed"
    WHERE?            → DAG: flight_ingestion, Task: validate_schema
    WHEN?             → 2024-06-15 14:32:01 UTC
    HOW BAD?          → 3 retries exhausted, 0 records loaded
    WHAT TO DO?       → Link to Airflow logs, suggested fix

USAGE:
  from monitoring.alerts import AlertManager
  alerts = AlertManager()
  alerts.send_error("Snowflake load failed", dag="flight_ingestion", records_lost=150)
  alerts.send_task_failure(context)  # Direct Airflow callback
"""

import os
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional
from enum import Enum

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    INFO     = "info"
    WARNING  = "warning"
    ERROR    = "error"
    CRITICAL = "critical"


# Slack emoji and colour for each severity
SEVERITY_CONFIG = {
    AlertSeverity.INFO    : {"emoji": "ℹ️",  "color": "#2196F3"},
    AlertSeverity.WARNING : {"emoji": "⚠️",  "color": "#FF9800"},
    AlertSeverity.ERROR   : {"emoji": "🔴",  "color": "#F44336"},
    AlertSeverity.CRITICAL: {"emoji": "💀",  "color": "#9C27B0"},
}


class AlertManager:
    """
    Central alert dispatcher.
    Sends to Slack and/or email based on configuration.
    Falls back to log-only if no webhook is configured.
    """

    def __init__(self):
        self.slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
        self.alert_email   = os.getenv("ALERT_EMAIL")
        self.pipeline_name = os.getenv("PIPELINE_NAME", "Flight Pipeline")
        self.env           = os.getenv("ENVIRONMENT", "development")

        if not self.slack_webhook:
            logger.warning(
                "SLACK_WEBHOOK_URL not configured. "
                "Alerts will only be written to logs. "
                "Set this in .env to enable Slack notifications."
            )

    # ── Public alert methods ──────────────────────────────────────────────────

    def send_info(self, message: str, **context) -> bool:
        """Send an informational alert (pipeline milestones, daily summaries)."""
        return self._dispatch(AlertSeverity.INFO, message, **context)

    def send_warning(self, message: str, **context) -> bool:
        """Send a warning alert (degraded performance, quality issues)."""
        return self._dispatch(AlertSeverity.WARNING, message, **context)

    def send_error(self, message: str, **context) -> bool:
        """Send an error alert (task failed, data issue)."""
        return self._dispatch(AlertSeverity.ERROR, message, **context)

    def send_critical(self, message: str, **context) -> bool:
        """Send a critical alert (SLA breach, data loss risk)."""
        return self._dispatch(AlertSeverity.CRITICAL, message, **context)

    def send_task_failure(self, context: dict) -> None:
        """
        Airflow on_failure_callback compatible method.
        Called automatically by Airflow when any task fails.

        Args:
            context: Airflow task context dict containing:
                     dag, task_instance, exception, execution_date, etc.
        """
        try:
            dag_id      = context.get("dag").dag_id
            task_id     = context.get("task_instance").task_id
            exec_date   = str(context.get("execution_date", ""))
            exception   = context.get("exception")
            log_url     = context.get("task_instance").log_url
            try_number  = context.get("task_instance").try_number
            max_tries   = context.get("task_instance").max_tries

            retry_info = f"Attempt {try_number} of {max_tries + 1}"
            is_final   = try_number > max_tries

            severity   = AlertSeverity.CRITICAL if is_final else AlertSeverity.ERROR
            status_msg = "RETRY LIMIT EXHAUSTED" if is_final else "TASK FAILED — WILL RETRY"

            self._dispatch(
                severity,
                f"{status_msg}: {dag_id} → {task_id}",
                dag_id        = dag_id,
                task_id       = task_id,
                execution_date= exec_date,
                retry_info    = retry_info,
                exception     = str(exception)[:500] if exception else "Unknown error",
                log_url       = log_url,
                is_final_retry= is_final,
            )
        except Exception as e:
            logger.error("Failed to send task failure alert: %s", e)

    def send_quality_alert(
        self,
        fail_rate_pct    : float,
        total_records    : int,
        failed_checks    : list,
        dag_id           : str = "unknown",
    ) -> bool:
        """Send an alert when data quality thresholds are breached."""
        severity = (
            AlertSeverity.CRITICAL if fail_rate_pct > 20
            else AlertSeverity.ERROR if fail_rate_pct > 10
            else AlertSeverity.WARNING
        )
        return self._dispatch(
            severity,
            f"Data quality alert: {fail_rate_pct:.1f}% records failed validation",
            dag_id        = dag_id,
            fail_rate_pct = fail_rate_pct,
            total_records = total_records,
            failed_checks = failed_checks,
            action_required = (
                "Investigate failing records in staging.flights_raw. "
                "Check Airflow DAG logs for root cause."
            ),
        )

    def send_freshness_alert(
        self,
        layer        : str,
        lag_minutes  : int,
        threshold_min: int = 30,
    ) -> bool:
        """Send an alert when data in a layer becomes stale."""
        severity = (
            AlertSeverity.CRITICAL if lag_minutes > 120
            else AlertSeverity.ERROR if lag_minutes > 60
            else AlertSeverity.WARNING
        )
        return self._dispatch(
            severity,
            f"Data freshness alert: {layer} layer is {lag_minutes} minutes stale",
            layer           = layer,
            lag_minutes     = lag_minutes,
            threshold_minutes= threshold_min,
            action_required = (
                f"Check if flight_ingestion_dag is running. "
                f"Verify Kafka producer is active. "
                f"Current lag: {lag_minutes}m (threshold: {threshold_min}m)."
            ),
        )

    def send_daily_summary(self, stats: dict) -> bool:
        """Send end-of-day pipeline summary (scheduled at midnight)."""
        return self._dispatch(
            AlertSeverity.INFO,
            f"Daily pipeline summary — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
            **stats,
        )

    # ── Internal dispatch logic ───────────────────────────────────────────────

    def _dispatch(
        self,
        severity : AlertSeverity,
        message  : str,
        **context,
    ) -> bool:
        """Route the alert to configured channels."""
        config    = SEVERITY_CONFIG[severity]
        timestamp = datetime.now(timezone.utc).isoformat()

        # Always log the alert (never silently drop alerts)
        log_fn = {
            AlertSeverity.INFO    : logger.info,
            AlertSeverity.WARNING : logger.warning,
            AlertSeverity.ERROR   : logger.error,
            AlertSeverity.CRITICAL: logger.critical,
        }[severity]

        log_fn(
            "ALERT [%s] %s",
            severity.value.upper(),
            message,
            extra={"alert_context": context, "alert_severity": severity.value}
        )

        success = True

        # Send to Slack
        if self.slack_webhook:
            slack_success = self._send_slack(
                severity, message, timestamp, config, context
            )
            success = success and slack_success

        return success

    def _send_slack(
        self,
        severity  : AlertSeverity,
        message   : str,
        timestamp : str,
        config    : dict,
        context   : dict,
    ) -> bool:
        """
        Send a formatted Slack message via webhook.

        Slack Block Kit format gives rich formatting:
        - Coloured sidebar (green/yellow/red based on severity)
        - Bold header, body text, context fields
        - Monospace code blocks for technical details
        """
        env_badge = f"[{self.env.upper()}]"

        # Build context fields from the extra kwargs
        fields = []
        priority_fields = [
            "dag_id", "task_id", "retry_info", "fail_rate_pct",
            "total_records", "lag_minutes", "layer", "records_loaded",
            "records_failed", "elapsed_seconds",
        ]
        # Show priority fields first, then others
        for key in priority_fields:
            if key in context:
                fields.append({
                    "type" : "mrkdwn",
                    "text" : f"*{key.replace('_',' ').title()}:*\n`{context[key]}`",
                })

        # Action required stands out
        action_text = ""
        if context.get("action_required"):
            action_text = (
                f"\n\n*🔧 Action Required:*\n{context['action_required']}"
            )

        # Log URL for direct access to Airflow task logs
        log_link = ""
        if context.get("log_url"):
            log_link = f"\n<{context['log_url']}|📋 View task logs in Airflow>"

        slack_payload = {
            "attachments": [{
                "color"    : config["color"],
                "blocks"   : [
                    {
                        "type": "header",
                        "text": {
                            "type" : "plain_text",
                            "text" : (
                                f"{config['emoji']} "
                                f"{env_badge} {self.pipeline_name} — "
                                f"{severity.value.upper()}"
                            ),
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"*{message}*"
                                f"{action_text}"
                                f"{log_link}"
                            ),
                        }
                    },
                    # Context fields (2 columns)
                    *(
                        [{
                            "type"  : "section",
                            "fields": fields[i:i+2],
                        }]
                        for i in range(0, len(fields), 2)
                    ),
                    {
                        "type"    : "context",
                        "elements": [{
                            "type": "mrkdwn",
                            "text": f"🕐 {timestamp} UTC",
                        }]
                    },
                ]
            }]
        }

        try:
            response = requests.post(
                self.slack_webhook,
                json    = slack_payload,
                timeout = 5,
            )
            if response.status_code == 200:
                logger.debug("Slack alert sent successfully")
                return True
            else:
                logger.warning(
                    "Slack alert failed | status=%d | body=%s",
                    response.status_code, response.text[:200]
                )
                return False
        except requests.exceptions.Timeout:
            logger.warning("Slack alert timed out after 5 seconds")
            return False
        except Exception as e:
            logger.warning("Slack alert delivery error: %s", e)
            return False


# ── Convenience singleton ─────────────────────────────────────────────────────
# Import this directly in DAGs and scripts
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Return the shared AlertManager instance (singleton pattern)."""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


def alert_on_failure(context: dict) -> None:
    """
    Airflow on_failure_callback.
    Import this directly into dag_utils.DEFAULT_ARGS.
    """
    get_alert_manager().send_task_failure(context)