# airflow/dags/flight_ingestion_dag.py
"""
DAG 1 — Flight Ingestion Pipeline
===================================
Schedule : Every 5 minutes
Purpose  : Move validated flight records from PostgreSQL staging → Snowflake RAW layer

Task Flow:
  check_new_data      → Sensor: waits until staging has unprocessed rows
        ↓
  validate_schema     → Python: checks every record matches our JSON schema
        ↓
  quality_gate        → Branch: if pass rate ≥ 95% → load; else → skip_load
        ↓                         ↓
  load_raw_snowflake  →         skip_load_notify
        ↓
  mark_processed      → Python: sets processed=TRUE in staging so we don't re-load

WHY THIS DESIGN:
  The SqlSensor at the start means this DAG only does real work when
  there is actually data to process. Without the sensor, every 5-minute
  run would query PostgreSQL, find nothing, and waste resources.
  The BranchOperator quality gate prevents bad data from ever
  reaching Snowflake — it's a quality firewall.
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

# ── Airflow core ──────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator   # renamed from DummyOperator in 2.4+
from airflow.providers.postgres.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import TaskInstance

# ── Our shared utilities ──────────────────────────────────────────────────────
from dag_utils import (
    DEFAULT_ARGS, SNOWFLAKE_CONN_ID, POSTGRES_CONN_ID,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_SCHEMA_RAW,
    QUALITY_FAIL_THRESHOLD, MIN_EXPECTED_RECORDS,
    get_postgres_connection, alert_on_failure, log_pipeline_run,
)

logger = logging.getLogger(__name__)

# ─── DAG definition ───────────────────────────────────────────────────────────
# The 'with DAG(...)' block is the modern way to define DAGs.
# Everything indented inside becomes part of this DAG automatically.
with DAG(
    dag_id            = "flight_ingestion_dag",
    description       = "Ingest validated flight data from PostgreSQL → Snowflake RAW",
    default_args      = {
        **DEFAULT_ARGS,
        # Override: if THIS specific DAG's task fails, call our alert function
        "on_failure_callback": alert_on_failure,
    },
    schedule          = "*/5 * * * *",   # Every 5 minutes (cron syntax)
    catchup           = False,            # Don't run all missed intervals on startup
    max_active_runs   = 1,               # Only 1 simultaneous run (prevent overlap)
    tags              = ["ingestion", "kafka", "snowflake", "phase3"],
) as dag:

    # ── TASK 1: SqlSensor ─────────────────────────────────────────────────────
    # Keeps polling PostgreSQL every 30 seconds.
    # Only proceeds when the SQL query returns at least one row.
    # poke_interval=30 → checks every 30 seconds
    # timeout=300       → gives up after 5 minutes (one full schedule interval)
    # mode="reschedule" → releases the worker slot while waiting (efficient)
    #                     vs mode="poke" which holds the worker hostage
    check_new_data = SqlSensor(
        task_id        = "check_new_data",
        conn_id        = POSTGRES_CONN_ID,
        sql            = """
            SELECT COUNT(*)
            FROM staging.flights_raw
            WHERE processed = FALSE
            AND ingested_at >= NOW() - INTERVAL '10 minutes'
        """,
        # The sensor considers the condition MET when this returns a truthy value.
        # COUNT(*) > 0 → truthy. COUNT(*) = 0 → falsy → keep waiting.
        success        = lambda count: count > 0,
        poke_interval  = 30,         # Check every 30 seconds
        timeout        = 300,        # Give up after 5 minutes
        mode           = "reschedule",
        soft_fail      = True,       # If timeout → mark SKIPPED not FAILED
    )

    # ── TASK 2: Validate Schema ───────────────────────────────────────────────
    def validate_schema_fn(**context) -> dict:
        """
        Fetch unprocessed records from staging and validate each one.

        Returns a validation report dict pushed to XCom.
        The next task (quality_gate) pulls this to decide whether to load.

        What we validate:
          - Required fields are present and not null
          - Numeric ranges (altitude 0–60000, speed 0–1200, lat/lon bounds)
          - Status is one of our allowed enum values
          - Timestamp is parseable
        """
        start_time = datetime.now(timezone.utc)
        pg = get_postgres_connection()
        cursor = pg.cursor()

        # Fetch all unprocessed records from staging
        cursor.execute("""
            SELECT id, flight_id, airline, flight_number,
                   source_airport, dest_airport,
                   altitude, speed, latitude, longitude,
                   status, raw_payload, ingested_at
            FROM staging.flights_raw
            WHERE processed = FALSE
            AND ingested_at >= NOW() - INTERVAL '10 minutes'
            ORDER BY ingested_at ASC
            LIMIT 5000
        """)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns, row)) for row in rows]

        logger.info("Fetched %d unprocessed records for validation", len(records))

        # ── Validation rules ──────────────────────────────────────────────────
        REQUIRED_FIELDS   = ["flight_id", "source_airport", "dest_airport",
                             "latitude", "longitude", "status"]
        VALID_STATUSES    = {"active", "scheduled", "landed", "cancelled",
                             "diverted", "unknown"}
        ALTITUDE_RANGE    = (0, 60000)
        SPEED_RANGE       = (0, 1200)
        LAT_RANGE         = (-90, 90)
        LON_RANGE         = (-180, 180)

        valid_ids   = []
        invalid_ids = []
        error_counts = {}

        for rec in records:
            errors = []
            payload = rec.get("raw_payload") or {}

            # 1. Required fields
            for field in REQUIRED_FIELDS:
                if rec.get(field) is None or rec.get(field) == "":
                    errors.append(f"missing_{field}")

            # 2. Altitude range
            alt = rec.get("altitude")
            if alt is not None and not (ALTITUDE_RANGE[0] <= alt <= ALTITUDE_RANGE[1]):
                errors.append("altitude_out_of_range")

            # 3. Speed range
            spd = rec.get("speed")
            if spd is not None and not (SPEED_RANGE[0] <= spd <= SPEED_RANGE[1]):
                errors.append("speed_out_of_range")

            # 4. Coordinate validation
            lat = rec.get("latitude")
            lon = rec.get("longitude")
            if lat is not None and not (LAT_RANGE[0] <= lat <= LAT_RANGE[1]):
                errors.append("latitude_out_of_range")
            if lon is not None and not (LON_RANGE[0] <= lon <= LON_RANGE[1]):
                errors.append("longitude_out_of_range")

            # 5. Status enum check
            status = rec.get("status", "")
            if status not in VALID_STATUSES:
                errors.append("invalid_status")

            # Collect results
            if errors:
                invalid_ids.append(rec["id"])
                for err in errors:
                    error_counts[err] = error_counts.get(err, 0) + 1
            else:
                valid_ids.append(rec["id"])

        total      = len(records)
        valid_count= len(valid_ids)
        fail_rate  = (len(invalid_ids) / total) if total > 0 else 0

        report = {
            "total_records"  : total,
            "valid_count"    : valid_count,
            "invalid_count"  : len(invalid_ids),
            "fail_rate"      : round(fail_rate, 4),
            "error_breakdown": error_counts,
            "valid_ids"      : valid_ids,     # PostgreSQL row IDs to load
            "invalid_ids"    : invalid_ids,
            "validated_at"   : datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "Validation complete | total=%d | valid=%d | fail_rate=%.2f%%",
            total, valid_count, fail_rate * 100
        )

        # Push report to XCom so downstream tasks can read it
        # XCom key = "validation_report", task_id = "validate_schema"
        context["ti"].xcom_push(key="validation_report", value=report)

        cursor.close()
        pg.close()
        return report

    validate_schema = PythonOperator(
        task_id         = "validate_schema",
        python_callable = validate_schema_fn,
    )

    # ── TASK 3: Quality Gate (Branch) ─────────────────────────────────────────
    # BranchPythonOperator returns the task_id of the NEXT task to run.
    # ALL OTHER branches are automatically skipped.
    def quality_gate_fn(**context) -> str:
        """
        Decide whether to proceed with loading or skip due to poor data quality.

        Returns:
            "load_raw_snowflake"   if quality is acceptable
            "skip_load_notify"     if too many bad records
        """
        ti = context["ti"]
        # Pull the report that validate_schema pushed to XCom
        report = ti.xcom_pull(task_ids="validate_schema", key="validation_report")

        total     = report.get("total_records", 0)
        fail_rate = report.get("fail_rate", 1.0)

        logger.info(
            "Quality gate | total=%d | fail_rate=%.2f%% | threshold=%.2f%%",
            total, fail_rate * 100, QUALITY_FAIL_THRESHOLD * 100
        )

        # Gate condition: enough records AND acceptable quality
        if total >= MIN_EXPECTED_RECORDS and fail_rate <= QUALITY_FAIL_THRESHOLD:
            logger.info("✓ Quality gate PASSED → proceeding to load")
            return "load_raw_snowflake"
        else:
            logger.warning(
                "✗ Quality gate FAILED | fail_rate=%.2f%% > threshold=%.2f%%",
                fail_rate * 100, QUALITY_FAIL_THRESHOLD * 100
            )
            return "skip_load_notify"

    quality_gate = BranchPythonOperator(
        task_id         = "quality_gate",
        python_callable = quality_gate_fn,
    )

    # ── TASK 4a: Load to Snowflake RAW ───────────────────────────────────────
    # SnowflakeOperator executes SQL directly on Snowflake.
    # It reads credentials from the Airflow connection (SNOWFLAKE_CONN_ID).
    # We use a staging table + MERGE to avoid duplicates.
    load_raw_snowflake = SnowflakeOperator(
        task_id           = "load_raw_snowflake",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse         = SNOWFLAKE_WAREHOUSE,
        database          = SNOWFLAKE_DATABASE,
        schema            = SNOWFLAKE_SCHEMA_RAW,
        sql               = """
            -- MERGE prevents duplicate loads if this DAG runs twice
            -- (idempotent — safe to re-run)
            MERGE INTO FLIGHT_DB.RAW.FLIGHTS_RAW AS target
            USING (
                SELECT
                    raw_payload:event_id::VARCHAR       AS event_id,
                    raw_payload:flight_id::VARCHAR      AS flight_id,
                    raw_payload:airline::VARCHAR        AS airline,
                    raw_payload:airline_iata::VARCHAR   AS airline_iata,
                    raw_payload:flight_number::VARCHAR  AS flight_number,
                    raw_payload:source_airport::VARCHAR AS source_airport,
                    raw_payload:dest_airport::VARCHAR   AS dest_airport,
                    raw_payload:source_city::VARCHAR    AS source_city,
                    raw_payload:dest_city::VARCHAR      AS dest_city,
                    raw_payload:latitude::FLOAT         AS latitude,
                    raw_payload:longitude::FLOAT        AS longitude,
                    raw_payload:altitude::INTEGER       AS altitude,
                    raw_payload:speed::FLOAT            AS speed,
                    raw_payload:heading::FLOAT          AS heading,
                    raw_payload:status::VARCHAR         AS status,
                    raw_payload:departure_time::TIMESTAMP_TZ AS departure_time,
                    raw_payload:arrival_time::TIMESTAMP_TZ   AS arrival_time,
                    raw_payload:delay_minutes::INTEGER  AS delay_minutes,
                    raw_payload:aircraft_type::VARCHAR  AS aircraft_type,
                    raw_payload:timestamp::TIMESTAMP_TZ AS event_timestamp,
                    raw_payload:source::VARCHAR         AS source,
                    raw_payload                         AS raw_json,
                    CURRENT_TIMESTAMP()                 AS loaded_at
                FROM FLIGHT_DB.RAW.FLIGHTS_STAGING
                WHERE processed_by_airflow = FALSE
            ) AS source
            ON target.event_id = source.event_id
            WHEN NOT MATCHED THEN
                INSERT (event_id, flight_id, airline, airline_iata, flight_number,
                        source_airport, dest_airport, source_city, dest_city,
                        latitude, longitude, altitude, speed, heading,
                        status, departure_time, arrival_time, delay_minutes,
                        aircraft_type, event_timestamp, source, raw_json, loaded_at)
                VALUES (source.event_id, source.flight_id, source.airline,
                        source.airline_iata, source.flight_number,
                        source.source_airport, source.dest_airport,
                        source.source_city, source.dest_city,
                        source.latitude, source.longitude, source.altitude,
                        source.speed, source.heading, source.status,
                        source.departure_time, source.arrival_time,
                        source.delay_minutes, source.aircraft_type,
                        source.event_timestamp, source.source,
                        source.raw_json, source.loaded_at);
        """,
    )

    # ── TASK 4b: Skip Load Notify ─────────────────────────────────────────────
    # This task runs when quality gate FAILS instead of loading.
    def skip_load_notify_fn(**context) -> None:
        """Log a warning when data quality is too poor to load."""
        ti = context["ti"]
        report = ti.xcom_pull(task_ids="validate_schema", key="validation_report")
        logger.warning(
            "LOAD SKIPPED due to data quality | "
            "fail_rate=%.2f%% | error_breakdown=%s",
            report.get("fail_rate", 0) * 100,
            report.get("error_breakdown", {})
        )
        # Phase 8 will add: send Slack alert here

    skip_load_notify = PythonOperator(
        task_id         = "skip_load_notify",
        python_callable = skip_load_notify_fn,
    )

    # ── TASK 5: Mark Processed ────────────────────────────────────────────────
    # After successfully loading to Snowflake, mark those rows in PostgreSQL.
    # This prevents the same records from being loaded again next run.
    # trigger_rule="none_failed_min_one_success" means:
    #   "run if at least one upstream task succeeded, and none failed"
    #   This makes mark_processed run after BOTH branches — 
    #   whether we loaded or skipped, we still mark the staging records.
    def mark_processed_fn(**context) -> None:
        """Mark all validated records in staging as processed."""
        ti = context["ti"]
        report = ti.xcom_pull(task_ids="validate_schema", key="validation_report")

        if not report:
            logger.warning("No validation report found in XCom — skipping mark")
            return

        # Mark ALL records (valid + invalid) as processed
        # Invalid ones stay in staging for inspection but won't be re-processed
        all_ids = report.get("valid_ids", []) + report.get("invalid_ids", [])

        if not all_ids:
            logger.info("No record IDs to mark — nothing to do")
            return

        pg = get_postgres_connection()
        cursor = pg.cursor()

        cursor.execute(
            """
            UPDATE staging.flights_raw
            SET processed = TRUE
            WHERE id = ANY(%s)
            """,
            (all_ids,)
        )
        updated = cursor.rowcount
        pg.commit()
        cursor.close()
        pg.close()

        logger.info("Marked %d records as processed in staging", updated)

        # Log this pipeline run to our audit table
        run_id = context["run_id"]
        log_pipeline_run(
            pg_conn        = get_postgres_connection(),
            run_id         = run_id,
            dag_id         = "flight_ingestion_dag",
            start_time     = context["data_interval_start"],
            end_time       = datetime.now(timezone.utc),
            records_read   = report.get("total_records", 0),
            records_written= report.get("valid_count", 0),
            status         = "success",
        )

    mark_processed = PythonOperator(
        task_id         = "mark_processed",
        python_callable = mark_processed_fn,
        trigger_rule    = "none_failed_min_one_success",
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    # The >> operator means "then" — left task must succeed before right starts.
    # This is how Airflow builds the DAG graph.
    #
    # Visually:
    #   check_new_data >> validate_schema >> quality_gate >> load_raw_snowflake >> mark_processed
    #                                                     >> skip_load_notify   >> mark_processed
    #
    # The [load_raw_snowflake, skip_load_notify] list means:
    # "mark_processed waits for EITHER of these to finish"
    check_new_data >> validate_schema >> quality_gate
    quality_gate >> [load_raw_snowflake, skip_load_notify]
    [load_raw_snowflake, skip_load_notify] >> mark_processed