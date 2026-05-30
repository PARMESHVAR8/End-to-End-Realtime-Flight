# airflow/dags/flight_validation_dag.py
"""
DAG 3 — Data Validation & Quality Monitoring
=============================================
Schedule: Every hour
Purpose : Run comprehensive data quality checks across all three layers.
          Alert if any metric falls below threshold.

This DAG is your DATA OBSERVABILITY layer.
In production companies: data engineers get paged at 3am because
"the dashboard is showing zero flights." This DAG catches problems
BEFORE business users notice them.

Checks performed:
  1. Volume check  — are we receiving expected number of records?
  2. Freshness check — is data too old? (pipeline may have stalled)
  3. Completeness — what % of critical fields are populated?
  4. Consistency  — do RAW and CLEAN record counts roughly match?
  5. Distribution — are delays, speeds within expected ranges?
"""

import logging
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from dag_utils import (
    DEFAULT_ARGS, SNOWFLAKE_CONN_ID, POSTGRES_CONN_ID,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE,
    get_snowflake_connection, alert_on_failure,
)

logger = logging.getLogger(__name__)

# Quality thresholds — tune these as you learn your data's patterns
THRESHOLDS = {
    "min_records_per_hour"  : 10,    # Expect at least 10 events per hour
    "max_data_age_minutes"  : 30,    # Data older than 30min = stale pipeline
    "min_completeness_pct"  : 0.90,  # 90% of critical fields must be non-null
    "max_raw_clean_drift"   : 0.10,  # RAW vs CLEAN counts can differ by 10%
    "max_avg_delay_minutes" : 300,   # If avg delay > 5h, something is wrong
}

with DAG(
    dag_id          = "flight_validation_dag",
    description     = "Hourly data quality checks across RAW, CLEAN, ANALYTICS layers",
    default_args    = {**DEFAULT_ARGS, "on_failure_callback": alert_on_failure},
    schedule        = "@hourly",
    catchup         = False,
    max_active_runs = 1,
    tags            = ["validation", "data-quality", "monitoring"],
) as dag:

    # ── TASK 1: Run all quality checks ────────────────────────────────────────
    def run_quality_checks_fn(**context) -> dict:
        """
        Execute all quality checks against Snowflake.
        Returns a report dict with pass/fail for each check.
        """
        snow   = get_snowflake_connection()
        cursor = snow.cursor()
        now    = datetime.now(timezone.utc)
        report = {
            "run_time"  : now.isoformat(),
            "checks"    : {},
            "passed"    : 0,
            "failed"    : 0,
            "warnings"  : 0,
        }

        def run_check(name: str, sql: str, threshold_fn, severity: str = "error"):
            """Helper: run one SQL check and evaluate against threshold."""
            try:
                cursor.execute(sql)
                value = cursor.fetchone()[0]
                passed = threshold_fn(value)
                status = "PASS" if passed else ("WARN" if severity == "warn" else "FAIL")
                report["checks"][name] = {
                    "value": value, "status": status, "severity": severity
                }
                if passed:
                    report["passed"] += 1
                elif severity == "warn":
                    report["warnings"] += 1
                else:
                    report["failed"] += 1
                logger.info("Check %-35s | value=%-10s | %s", name, value, status)
            except Exception as e:
                report["checks"][name] = {"value": None, "status": "ERROR", "error": str(e)}
                report["failed"] += 1
                logger.error("Check %s ERRORED: %s", name, e)

        # ── Check 1: Volume — records in last hour ────────────────────────────
        run_check(
            name         = "raw_records_last_hour",
            sql          = """
                SELECT COUNT(*) FROM FLIGHT_DB.RAW.FLIGHTS_RAW
                WHERE loaded_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """,
            threshold_fn = lambda n: n >= THRESHOLDS["min_records_per_hour"],
        )

        # ── Check 2: Freshness — how old is the newest record? ────────────────
        run_check(
            name         = "data_freshness_minutes",
            sql          = """
                SELECT DATEDIFF(
                    minute,
                    MAX(loaded_at),
                    CURRENT_TIMESTAMP()
                )
                FROM FLIGHT_DB.RAW.FLIGHTS_RAW
            """,
            threshold_fn = lambda mins: (mins or 9999) <= THRESHOLDS["max_data_age_minutes"],
        )

        # ── Check 3: Completeness — null rate for critical fields ─────────────
        run_check(
            name         = "flight_id_completeness",
            sql          = """
                SELECT 1.0 - (
                    SUM(CASE WHEN flight_id IS NULL THEN 1 ELSE 0 END)::FLOAT
                    / NULLIF(COUNT(*), 0)
                )
                FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                WHERE transformed_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """,
            threshold_fn = lambda pct: (pct or 0) >= THRESHOLDS["min_completeness_pct"],
        )

        run_check(
            name         = "coordinates_completeness",
            sql          = """
                SELECT 1.0 - (
                    SUM(CASE WHEN latitude IS NULL OR longitude IS NULL
                             THEN 1 ELSE 0 END)::FLOAT
                    / NULLIF(COUNT(*), 0)
                )
                FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                WHERE transformed_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """,
            threshold_fn = lambda pct: (pct or 0) >= THRESHOLDS["min_completeness_pct"],
        )

        # ── Check 4: Consistency — RAW vs CLEAN counts within last hour ───────
        run_check(
            name         = "raw_clean_drift",
            sql          = """
                SELECT ABS(raw_count - clean_count)::FLOAT / NULLIF(raw_count, 0)
                FROM (
                    SELECT
                        (SELECT COUNT(*) FROM FLIGHT_DB.RAW.FLIGHTS_RAW
                         WHERE loaded_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
                        ) AS raw_count,
                        (SELECT COUNT(*) FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                         WHERE transformed_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
                        ) AS clean_count
                )
            """,
            threshold_fn = lambda drift: (drift or 0) <= THRESHOLDS["max_raw_clean_drift"],
            severity     = "warn",   # Drift is a warning, not hard failure
        )

        # ── Check 5: Distribution — average delay sanity check ────────────────
        run_check(
            name         = "avg_delay_minutes",
            sql          = """
                SELECT AVG(delay_minutes)
                FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                WHERE transformed_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """,
            threshold_fn = lambda avg: (avg or 0) <= THRESHOLDS["max_avg_delay_minutes"],
            severity     = "warn",
        )

        # ── Check 6: Fact table row count ─────────────────────────────────────
        run_check(
            name         = "fact_table_populated",
            sql          = """
                SELECT COUNT(*) FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
                WHERE event_date = CURRENT_DATE()
            """,
            threshold_fn = lambda n: n >= THRESHOLDS["min_records_per_hour"],
        )

        cursor.close()
        snow.close()

        # Overall verdict
        report["overall_status"] = "HEALTHY" if report["failed"] == 0 else "DEGRADED"
        logger.info(
            "Quality report | passed=%d | failed=%d | warnings=%d | status=%s",
            report["passed"], report["failed"],
            report["warnings"], report["overall_status"]
        )

        context["ti"].xcom_push(key="quality_report", value=report)
        return report

    run_quality_checks = PythonOperator(
        task_id         = "run_quality_checks",
        python_callable = run_quality_checks_fn,
    )

    # ── TASK 2: Branch on quality result ─────────────────────────────────────
    def check_thresholds_fn(**context) -> str:
        ti     = context["ti"]
        report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")
        if report.get("failed", 0) > 0:
            return "send_quality_alert"
        return "log_pipeline_healthy"

    check_thresholds = BranchPythonOperator(
        task_id         = "check_thresholds",
        python_callable = check_thresholds_fn,
    )

    # ── TASK 3a: Alert ────────────────────────────────────────────────────────
    def send_quality_alert_fn(**context) -> None:
        ti     = context["ti"]
        report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")
        failed_checks = {
            k: v for k, v in report.get("checks", {}).items()
            if v.get("status") == "FAIL"
        }
        logger.error(
            "DATA QUALITY ALERT | failed_checks=%s | run_time=%s",
            failed_checks, report.get("run_time")
        )
        # Phase 8: plug in Slack/PagerDuty here

    send_quality_alert = PythonOperator(
        task_id         = "send_quality_alert",
        python_callable = send_quality_alert_fn,
    )

    # ── TASK 3b: Log healthy ──────────────────────────────────────────────────
    def log_pipeline_healthy_fn(**context) -> None:
        ti     = context["ti"]
        report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")
        logger.info(
            "Pipeline HEALTHY | passed=%d checks | warnings=%d | run_time=%s",
            report.get("passed", 0),
            report.get("warnings", 0),
            report.get("run_time")
        )

    log_pipeline_healthy = PythonOperator(
        task_id         = "log_pipeline_healthy",
        python_callable = log_pipeline_healthy_fn,
    )

    # ── TASK 4: Write quality log to Snowflake ────────────────────────────────
    update_quality_log = SnowflakeOperator(
        task_id           = "update_quality_log",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse         = SNOWFLAKE_WAREHOUSE,
        database          = SNOWFLAKE_DATABASE,
        schema            = "ANALYTICS",
        sql               = """
            INSERT INTO FLIGHT_DB.ANALYTICS.DATA_QUALITY_LOG
                (check_run_time, overall_status, checks_passed,
                 checks_failed, checks_warned, created_at)
            VALUES (
                CURRENT_TIMESTAMP(),
                '{{ ti.xcom_pull(task_ids="run_quality_checks",
                                  key="quality_report")["overall_status"] }}',
                {{ ti.xcom_pull(task_ids="run_quality_checks",
                                key="quality_report")["passed"] }},
                {{ ti.xcom_pull(task_ids="run_quality_checks",
                                key="quality_report")["failed"] }},
                {{ ti.xcom_pull(task_ids="run_quality_checks",
                                key="quality_report")["warnings"] }},
                CURRENT_TIMESTAMP()
            );
        """,
        trigger_rule = "none_failed_min_one_success",
    )
    from monitoring.pipeline_monitor import airflow_health_check, airflow_daily_summary

    run_health_check = PythonOperator(
        task_id         = "run_full_health_check",
        python_callable = airflow_health_check,
        dag             = dag,
    )


    # ── Task Dependencies ─────────────────────────────────────────────────────
    run_quality_checks >> check_thresholds
    check_thresholds   >> [send_quality_alert, log_pipeline_healthy]
    [send_quality_alert, log_pipeline_healthy] >> update_quality_log
    update_quality_log >> run_health_check