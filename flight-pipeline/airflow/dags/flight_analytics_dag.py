# airflow/dags/flight_analytics_dag.py
"""
DAG 4 — Business Analytics Refresh
=====================================
Schedule : Every 30 minutes
Purpose  : Refresh all 10 analytics views + compute KPI summary

This DAG is the "last mile" — it ensures analytics views are
always fresh when the dashboard loads.
"""

import json
import logging
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from dag_utils import (
    DEFAULT_ARGS, SNOWFLAKE_CONN_ID,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE,
    alert_on_failure,
)

logger = logging.getLogger(__name__)

with DAG(
    dag_id          = "flight_analytics_dag",
    description     = "Refresh all 10 business analytics views every 30 minutes",
    default_args    = {**DEFAULT_ARGS, "on_failure_callback": alert_on_failure},
    schedule        = "*/30 * * * *",
    catchup         = False,
    max_active_runs = 1,
    tags            = ["analytics", "business", "kpi"],
) as dag:

    # ── TASK 1: Refresh hourly summary table ──────────────────────────────────
    refresh_hourly_summary = SnowflakeOperator(
        task_id           = "refresh_hourly_summary",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse         = SNOWFLAKE_WAREHOUSE,
        database          = SNOWFLAKE_DATABASE,
        schema            = "ANALYTICS",
        sql               = "CALL FLIGHT_DB.ANALYTICS.REFRESH_HOURLY_SUMMARY();",
    )

    # ── TASK 2: Refresh route performance table ───────────────────────────────
    refresh_route_performance = SnowflakeOperator(
        task_id           = "refresh_route_performance",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse         = SNOWFLAKE_WAREHOUSE,
        database          = SNOWFLAKE_DATABASE,
        schema            = "ANALYTICS",
        sql               = """
            MERGE INTO FLIGHT_DB.ANALYTICS.ROUTE_PERFORMANCE AS tgt
            USING (
                SELECT
                    CURRENT_DATE()                              AS event_date,
                    source_airport,
                    dest_airport,
                    source_airport || '→' || dest_airport      AS route_key,
                    COUNT(DISTINCT flight_id)                  AS total_flights,
                    COUNT(*)                                   AS total_events,
                    ROUND(AVG(delay_minutes), 2)               AS avg_delay_minutes,
                    MAX(delay_minutes)                         AS max_delay_minutes,
                    ROUND(SUM(IFF(delay_bucket='on_time',1,0))::FLOAT/NULLIF(COUNT(*),0),4) AS pct_on_time,
                    ROUND(SUM(IFF(delay_bucket!='on_time',1,0))::FLOAT/NULLIF(COUNT(*),0),4) AS pct_delayed,
                    ROUND(AVG(speed),2)                        AS avg_speed,
                    ROUND(AVG(IFF(flight_phase='cruise',altitude,NULL)),0) AS avg_cruise_altitude,
                    MAX(is_international)                      AS is_international,
                    CURRENT_TIMESTAMP()                        AS summary_created_at
                FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
                WHERE event_date = CURRENT_DATE()
                  AND data_quality_flag = FALSE
                  AND source_airport != dest_airport
                GROUP BY source_airport, dest_airport
                HAVING COUNT(DISTINCT flight_id) >= 1
            ) AS src
            ON  tgt.event_date    = src.event_date
            AND tgt.source_airport = src.source_airport
            AND tgt.dest_airport   = src.dest_airport
            WHEN MATCHED THEN UPDATE SET
                total_flights     = src.total_flights,
                total_events      = src.total_events,
                avg_delay_minutes = src.avg_delay_minutes,
                max_delay_minutes = src.max_delay_minutes,
                pct_on_time       = src.pct_on_time,
                pct_delayed       = src.pct_delayed,
                avg_speed         = src.avg_speed,
                avg_cruise_altitude = src.avg_cruise_altitude,
                summary_created_at = src.summary_created_at
            WHEN NOT MATCHED THEN INSERT (
                event_date, source_airport, dest_airport, route_key,
                total_flights, total_events, avg_delay_minutes, max_delay_minutes,
                pct_on_time, pct_delayed, avg_speed, avg_cruise_altitude,
                is_international, summary_created_at
            ) VALUES (
                src.event_date, src.source_airport, src.dest_airport, src.route_key,
                src.total_flights, src.total_events, src.avg_delay_minutes, src.max_delay_minutes,
                src.pct_on_time, src.pct_delayed, src.avg_speed, src.avg_cruise_altitude,
                src.is_international, src.summary_created_at
            );
        """,
    )

    # ── TASK 3: Compute and log KPI summary ───────────────────────────────────
    def compute_kpi_summary_fn(**context) -> dict:
        """
        Pull KPIs from all 10 views and push to XCom.
        Streamlit dashboard reads these for headline cards.
        """
        import sys, os
        sys.path.insert(0, "/opt/airflow/project")
        from transformation.analytics_runner import AnalyticsRunner

        runner = AnalyticsRunner()
        kpis   = runner.get_kpi_summary()

        logger.info("KPI summary computed: %s", json.dumps(kpis, default=str))
        context["ti"].xcom_push(key="kpi_summary", value=kpis)
        return kpis

    compute_kpis = PythonOperator(
        task_id         = "compute_kpi_summary",
        python_callable = compute_kpi_summary_fn,
    )

    # ── TASK 4: Log analytics run ─────────────────────────────────────────────
    def log_analytics_run_fn(**context) -> None:
        ti   = context["ti"]
        kpis = ti.xcom_pull(task_ids="compute_kpi_summary", key="kpi_summary")
        logger.info(
            "Analytics refresh complete | "
            "active_flights=%s | fleet_otp=%s%% | pipeline=%s",
            kpis.get("active_flights_now", "?"),
            kpis.get("fleet_otp_rate_pct", "?"),
            kpis.get("pipeline", {}).get("health", "?"),
        )

    log_run = PythonOperator(
        task_id         = "log_analytics_run",
        python_callable = log_analytics_run_fn,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    [refresh_hourly_summary, refresh_route_performance] >> compute_kpis >> log_run