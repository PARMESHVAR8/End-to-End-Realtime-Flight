# airflow/dags/dag_utils.py
"""
Shared utilities for all Flight Pipeline DAGs.
==============================================
Every DAG imports from here — change once, affects all DAGs.

WHY A SHARED UTILS MODULE:
  Without this, every DAG file has its own copy of:
  - default_args dict (retry settings, owner, email)
  - Snowflake connection logic
  - PostgreSQL helper functions
  - Slack/email alert functions
  If retry policy changes from 3 to 5 retries, you'd edit 3 files.
  With this module, edit one line here and all DAGs update instantly.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


# ─── Default arguments applied to EVERY task in EVERY DAG ────────────────────
# These are the "house rules" — applied unless a task overrides them.
#
# retries=3 means: if a task fails, Airflow waits retry_delay then tries again.
# After 3 attempts it marks the task as FAILED and sends an alert.
# retry_delay=timedelta(minutes=2) means wait 2 minutes between attempts.
#
# execution_timeout: if a task hangs for more than 30 minutes, kill it.
# Without this, a stuck task blocks the entire DAG forever.
DEFAULT_ARGS = {
    "owner"            : "flight_pipeline",       # Shows in Airflow UI
    "depends_on_past"  : False,                   # Don't wait for yesterday's run
    "start_date"       : datetime(2024, 1, 1, tzinfo=timezone.utc),
    "email"            : [os.getenv("ALERT_EMAIL", "admin@flightpipeline.com")],
    "email_on_failure" : False,                   # Set True in production
    "email_on_retry"   : False,
    "retries"          : 3,
    "retry_delay"      : timedelta(minutes=2),
    "retry_exponential_backoff": True,            # Wait 2m, 4m, 8m between retries
    "max_retry_delay"  : timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}

# ─── Airflow connection IDs ───────────────────────────────────────────────────
# These match connection names you'll create in Airflow UI (Admin → Connections)
# Operators use these IDs to look up credentials — never hardcode passwords here
SNOWFLAKE_CONN_ID = "snowflake_flight"
POSTGRES_CONN_ID  = "postgres_flight"

# ─── Snowflake config ─────────────────────────────────────────────────────────
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "FLIGHT_DB")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "FLIGHT_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_SCHEMA_RAW       = "RAW"
SNOWFLAKE_SCHEMA_CLEAN     = "CLEAN"
SNOWFLAKE_SCHEMA_ANALYTICS = "ANALYTICS"

# ─── Quality thresholds ───────────────────────────────────────────────────────
# If more than this % of records fail validation → trigger alert
QUALITY_FAIL_THRESHOLD = 0.05   # 5%
MIN_EXPECTED_RECORDS   = 1      # At least 1 record per ingestion run


def get_snowflake_connection():
    """
    Returns a live Snowflake connector connection.
    Used in PythonOperator tasks that need to run custom SQL.

    For SnowflakeOperator tasks, pass snowflake_conn_id=SNOWFLAKE_CONN_ID
    and Airflow handles the connection automatically.
    """
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        role      = os.getenv("SNOWFLAKE_ROLE"),
    )


def get_postgres_connection():
    """Returns a psycopg2 connection to the staging PostgreSQL database."""
    import psycopg2
    return psycopg2.connect(
        host    = os.getenv("POSTGRES_HOST", "postgres"),
        port    = int(os.getenv("POSTGRES_PORT", "5433")),
        user    = os.getenv("POSTGRES_USER", "airflow"),
        password= os.getenv("POSTGRES_PASSWORD", "airflow"),
        dbname  = os.getenv("POSTGRES_DB", "airflow"),
    )


def alert_on_failure(context: dict) -> None:
    """
    Called automatically by Airflow when any task fails.
    'context' contains everything about the failed run:
    dag_id, task_id, execution_date, exception, log_url, etc.

    In production, this sends a Slack message or PagerDuty alert.
    For now, we log a structured error message.
    """
    dag_id    = context.get("dag").dag_id
    task_id   = context.get("task_instance").task_id
    exec_date = context.get("execution_date")
    exception = context.get("exception")
    log_url   = context.get("task_instance").log_url

    logger.error(
        "TASK FAILED | dag=%s | task=%s | exec_date=%s | error=%s | logs=%s",
        dag_id, task_id, exec_date, exception, log_url
    )
    # TODO Phase 8: replace with Slack webhook call
    # slack_alert(dag_id, task_id, str(exception), log_url)


def log_pipeline_run(
    pg_conn,
    run_id       : str,
    dag_id       : str,
    start_time   : datetime,
    end_time     : datetime,
    records_read : int,
    records_written: int,
    status       : str,
    error_message: Optional[str] = None,
) -> None:
    """
    Writes a pipeline run record to staging.pipeline_runs.
    This gives you a history of every run — useful for debugging and SLA tracking.
    """
    try:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            INSERT INTO staging.pipeline_runs
                (run_id, dag_id, start_time, end_time,
                 records_read, records_written, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (run_id, dag_id, start_time, end_time,
             records_read, records_written, status, error_message)
        )
        pg_conn.commit()
        cursor.close()
    except Exception as e:
        logger.warning("Could not log pipeline run: %s", e)