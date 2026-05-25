# airflow/dags/flight_transform_dag.py
"""
DAG 2 — Flight Transformation Pipeline
========================================
Schedule : Every 15 minutes
Purpose  : Move data through RAW → CLEAN → ANALYTICS layers in Snowflake

Task Flow:
  check_raw_data      → Sensor: waits for new RAW records
        ↓
  clean_transform     → Python: Pandas cleaning + enrichment
        ↓
  load_clean          → Snowflake: write to CLEAN schema
        ↓
  build_fact_tables   → Snowflake: build fact_flights in ANALYTICS
        ↓
  build_dim_tables    → Snowflake: build dim_airlines, dim_airports
        ↓
  build_aggregates    → Snowflake: hourly + daily summaries
        ↓
  log_run_stats       → Python: record metrics, push to XCom

WHY THREE ANALYTICS TASKS:
  In real companies, the analytics layer has many independent tables.
  Running them in sequence (fact → dim → agg) ensures foreign keys
  are satisfied. In production these can be parallelised where dependencies
  allow — that's exactly what dbt handles (Phase 6).
"""

import json
import logging
import pandas as pd
from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.postgres.sensors.sql import SqlSensor

from dag_utils import (
    DEFAULT_ARGS, SNOWFLAKE_CONN_ID, POSTGRES_CONN_ID,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_SCHEMA_RAW, SNOWFLAKE_SCHEMA_CLEAN, SNOWFLAKE_SCHEMA_ANALYTICS,
    get_snowflake_connection, alert_on_failure, log_pipeline_run,
)

logger = logging.getLogger(__name__)

with DAG(
    dag_id          = "flight_transform_dag",
    description     = "Transform flight data: RAW → CLEAN → ANALYTICS in Snowflake",
    default_args    = {**DEFAULT_ARGS, "on_failure_callback": alert_on_failure},
    schedule        = "*/15 * * * *",    # Every 15 minutes
    catchup         = False,
    max_active_runs = 1,
    tags            = ["transformation", "snowflake", "dbt-ready"],
) as dag:

    # ── TASK 1: Wait for new RAW data ─────────────────────────────────────────
    check_raw_data = SqlSensor(
        task_id       = "check_raw_data",
        conn_id       = SNOWFLAKE_CONN_ID,
        sql           = """
            SELECT COUNT(*)
            FROM FLIGHT_DB.RAW.FLIGHTS_RAW
            WHERE loaded_at >= DATEADD(minute, -20, CURRENT_TIMESTAMP())
            AND is_transformed = FALSE
        """,
        success       = lambda n: n > 0,
        poke_interval = 60,
        timeout       = 900,
        mode          = "reschedule",
        soft_fail     = True,
    )

    # ── TASK 2: Clean & Transform ─────────────────────────────────────────────
    def clean_transform_fn(**context) -> dict:
        """
        Pull raw records from Snowflake, clean them with Pandas,
        and write back to the CLEAN schema.

        Transformations applied:
          1. Standardise airline names (trim whitespace, title-case)
          2. Normalise airport codes (uppercase)
          3. Impute missing altitude (set to 0 for grounded flights)
          4. Cap extreme speeds at physical maximum (1200 km/h)
          5. Derive new columns: is_international, delay_bucket, region
          6. Parse timestamps to consistent UTC timezone
          7. Remove exact duplicates (same event_id appearing twice)
          8. Flag records with any remaining anomalies
        """
        start_time = datetime.now(timezone.utc)

        snow = get_snowflake_connection()
        cursor = snow.cursor()

        # Fetch untransformed raw records
        cursor.execute("""
            SELECT
                event_id, flight_id, airline, airline_iata, flight_number,
                source_airport, dest_airport, source_city, dest_city,
                latitude, longitude, altitude, speed, heading, status,
                departure_time, arrival_time, delay_minutes, aircraft_type,
                event_timestamp, source, loaded_at
            FROM FLIGHT_DB.RAW.FLIGHTS_RAW
            WHERE is_transformed = FALSE
            AND loaded_at >= DATEADD(minute, -20, CURRENT_TIMESTAMP())
            ORDER BY event_timestamp ASC
            LIMIT 10000
        """)
        rows    = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        if not rows:
            logger.info("No new raw records to transform")
            snow.close()
            return {"records_transformed": 0}

        # ── Load into Pandas ──────────────────────────────────────────────────
        df = pd.DataFrame(rows, columns=[c.lower() for c in columns])
        original_count = len(df)
        logger.info("Loaded %d raw records into Pandas", original_count)

        # ── Transformation 1: Deduplication ───────────────────────────────────
        # Remove exact duplicate event_ids — these come from Kafka at-least-once
        df = df.drop_duplicates(subset=["event_id"], keep="first")
        logger.info("After dedup: %d records (removed %d)",
                    len(df), original_count - len(df))

        # ── Transformation 2: String normalisation ────────────────────────────
        df["airline"]        = df["airline"].str.strip().str.title()
        df["airline_iata"]   = df["airline_iata"].str.strip().str.upper()
        df["source_airport"] = df["source_airport"].str.strip().str.upper()
        df["dest_airport"]   = df["dest_airport"].str.strip().str.upper()
        df["status"]         = df["status"].str.strip().str.lower()

        # ── Transformation 3: Numeric imputation & capping ────────────────────
        # Grounded/taxi aircraft: altitude null → 0
        df["altitude"] = df["altitude"].fillna(0).clip(lower=0, upper=60000)
        # Speed: cap at 1200 km/h (physically impossible to exceed)
        df["speed"]    = df["speed"].fillna(0).clip(lower=0, upper=1200)
        # Delay: negative delay is meaningless → 0
        df["delay_minutes"] = df["delay_minutes"].fillna(0).clip(lower=0)

        # ── Transformation 4: Derived columns ─────────────────────────────────
        # is_international: source and destination in different countries
        INDIA_AIRPORTS = {"DEL","BOM","BLR","MAA","CCU","HYD","AMD","COK","GOI","JAI"}
        df["is_international"] = ~(
            df["source_airport"].isin(INDIA_AIRPORTS) &
            df["dest_airport"].isin(INDIA_AIRPORTS)
        )

        # delay_bucket: categorise delay for analytics
        def categorise_delay(mins):
            if mins == 0:        return "on_time"
            elif mins <= 15:     return "minor_delay"     # ≤15 min
            elif mins <= 60:     return "moderate_delay"  # 15–60 min
            elif mins <= 180:    return "major_delay"     # 1–3 hours
            else:                return "severe_delay"    # >3 hours
        df["delay_bucket"] = df["delay_minutes"].apply(categorise_delay)

        # flight_phase: what part of the journey based on altitude
        def flight_phase(alt):
            if alt < 1000:  return "ground"
            elif alt < 15000: return "climbing"
            elif alt < 32000: return "mid_altitude"
            else:             return "cruise"
        df["flight_phase"] = df["altitude"].apply(flight_phase)

        # region: rough geographic grouping for dashboard filters
        def get_region(lat, lon):
            if 8 <= lat <= 37 and 68 <= lon <= 97:  return "India"
            elif lat >= 35:                          return "Europe_Asia"
            elif lat <= 0:                           return "Southern"
            else:                                    return "Middle_East_SE_Asia"
        df["region"] = df.apply(
            lambda r: get_region(r["latitude"], r["longitude"]), axis=1
        )

        # data_quality_flag: TRUE if this record has any remaining anomaly
        df["data_quality_flag"] = (
            df["latitude"].isna() |
            df["longitude"].isna() |
            (df["speed"] == 0) & (df["status"] == "active") |
            df["source_airport"].eq("???")
        )

        # transformed_at timestamp
        df["transformed_at"] = datetime.now(timezone.utc).isoformat()

        # ── Write back to Snowflake CLEAN schema ──────────────────────────────
        # We use executemany for bulk insert efficiency
        clean_cursor = snow.cursor()
        records_to_insert = df.values.tolist()

        # Build column list (must match CLEAN table definition exactly)
        clean_columns = [
            "event_id","flight_id","airline","airline_iata","flight_number",
            "source_airport","dest_airport","source_city","dest_city",
            "latitude","longitude","altitude","speed","heading","status",
            "departure_time","arrival_time","delay_minutes","aircraft_type",
            "event_timestamp","source","loaded_at",
            "is_international","delay_bucket","flight_phase","region",
            "data_quality_flag","transformed_at"
        ]

        placeholders = ", ".join(["%s"] * len(clean_columns))
        col_list     = ", ".join(clean_columns)

        clean_cursor.executemany(
            f"""
            INSERT INTO FLIGHT_DB.CLEAN.FLIGHTS_CLEAN ({col_list})
            SELECT {placeholders}
            WHERE NOT EXISTS (
                SELECT 1 FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                WHERE event_id = %s
            )
            """,
            [row + [row[0]] for row in records_to_insert]  # append event_id for WHERE
        )

        # Mark raw records as transformed so we don't reprocess them
        event_ids = df["event_id"].tolist()
        placeholders_ids = ", ".join([f"'{eid}'" for eid in event_ids])
        clean_cursor.execute(f"""
            UPDATE FLIGHT_DB.RAW.FLIGHTS_RAW
            SET is_transformed = TRUE,
                transformed_at = CURRENT_TIMESTAMP()
            WHERE event_id IN ({placeholders_ids})
        """)

        snow.commit()
        clean_cursor.close()
        snow.close()

        result = {
            "records_input"      : original_count,
            "records_transformed": len(df),
            "duplicates_removed" : original_count - len(df),
            "quality_flagged"    : int(df["data_quality_flag"].sum()),
        }
        logger.info("Transformation complete: %s", result)
        context["ti"].xcom_push(key="transform_result", value=result)
        return result

    clean_transform = PythonOperator(
        task_id         = "clean_transform",
        python_callable = clean_transform_fn,
    )

    # ── TASK 3: Build Fact Table ──────────────────────────────────────────────
    # Runs SQL directly on Snowflake to build the central fact table.
    # ANALYTICS layer tables are the ones the dashboard queries.
    build_fact_flights = SnowflakeOperator(
        task_id           = "build_fact_flights",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse         = SNOWFLAKE_WAREHOUSE,
        database          = SNOWFLAKE_DATABASE,
        schema            = SNOWFLAKE_SCHEMA_ANALYTICS,
        sql               = """
            -- Fact table: one row per flight event (grain = one position update)
            -- Uses MERGE to be idempotent (safe to re-run)
            MERGE INTO FLIGHT_DB.ANALYTICS.FACT_FLIGHTS AS tgt
            USING (
                SELECT
                    c.event_id,
                    c.flight_id,
                    c.airline_iata,
                    c.source_airport,
                    c.dest_airport,
                    c.latitude,
                    c.longitude,
                    c.altitude,
                    c.speed,
                    c.status,
                    c.delay_minutes,
                    c.delay_bucket,
                    c.flight_phase,
                    c.is_international,
                    c.region,
                    c.data_quality_flag,
                    DATE(c.event_timestamp)            AS event_date,
                    HOUR(c.event_timestamp)            AS event_hour,
                    DAYOFWEEK(c.event_timestamp)       AS day_of_week,
                    c.event_timestamp,
                    c.transformed_at
                FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN c
                WHERE c.transformed_at >= DATEADD(minute, -20, CURRENT_TIMESTAMP())
                AND c.data_quality_flag = FALSE
            ) AS src
            ON tgt.event_id = src.event_id
            WHEN NOT MATCHED THEN INSERT
                (event_id, flight_id, airline_iata, source_airport, dest_airport,
                 latitude, longitude, altitude, speed, status, delay_minutes,
                 delay_bucket, flight_phase, is_international, region,
                 data_quality_flag, event_date, event_hour, day_of_week,
                 event_timestamp, transformed_at)
            VALUES
                (src.event_id, src.flight_id, src.airline_iata, src.source_airport,
                 src.dest_airport, src.latitude, src.longitude, src.altitude,
                 src.speed, src.status, src.delay_minutes, src.delay_bucket,
                 src.flight_phase, src.is_international, src.region,
                 src.data_quality_flag, src.event_date, src.event_hour,
                 src.day_of_week, src.event_timestamp, src.transformed_at);
        """,
    )

    # ── TASK 4: Build Dimension Tables ────────────────────────────────────────
    build_dim_tables = SnowflakeOperator(
        task_id           = "build_dim_tables",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse         = SNOWFLAKE_WAREHOUSE,
        database          = SNOWFLAKE_DATABASE,
        schema            = SNOWFLAKE_SCHEMA_ANALYTICS,
        sql               = """
            -- Dimension: Airlines
            -- Upsert: if airline already exists update stats, else insert
            MERGE INTO FLIGHT_DB.ANALYTICS.DIM_AIRLINES AS tgt
            USING (
                SELECT
                    airline_iata                          AS airline_code,
                    MAX(airline)                          AS airline_name,
                    COUNT(DISTINCT flight_id)             AS total_flights,
                    AVG(delay_minutes)                    AS avg_delay_minutes,
                    SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) AS active_flights,
                    MAX(transformed_at)                   AS last_seen_at
                FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                GROUP BY airline_iata
            ) AS src
            ON tgt.airline_code = src.airline_code
            WHEN MATCHED THEN UPDATE SET
                airline_name      = src.airline_name,
                total_flights     = src.total_flights,
                avg_delay_minutes = src.avg_delay_minutes,
                active_flights    = src.active_flights,
                last_seen_at      = src.last_seen_at
            WHEN NOT MATCHED THEN INSERT
                (airline_code, airline_name, total_flights,
                 avg_delay_minutes, active_flights, last_seen_at)
            VALUES
                (src.airline_code, src.airline_name, src.total_flights,
                 src.avg_delay_minutes, src.active_flights, src.last_seen_at);

            -- Dimension: Airports
            MERGE INTO FLIGHT_DB.ANALYTICS.DIM_AIRPORTS AS tgt
            USING (
                SELECT
                    airport_code,
                    MAX(city)              AS city,
                    SUM(departures)        AS total_departures,
                    SUM(arrivals)          AS total_arrivals,
                    MAX(last_activity)     AS last_activity
                FROM (
                    SELECT
                        source_airport  AS airport_code,
                        MAX(source_city) AS city,
                        COUNT(*)         AS departures,
                        0                AS arrivals,
                        MAX(transformed_at) AS last_activity
                    FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                    GROUP BY source_airport
                    UNION ALL
                    SELECT
                        dest_airport,
                        MAX(dest_city),
                        0,
                        COUNT(*),
                        MAX(transformed_at)
                    FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
                    GROUP BY dest_airport
                )
                GROUP BY airport_code
            ) AS src
            ON tgt.airport_code = src.airport_code
            WHEN MATCHED THEN UPDATE SET
                city             = src.city,
                total_departures = src.total_departures,
                total_arrivals   = src.total_arrivals,
                last_activity    = src.last_activity
            WHEN NOT MATCHED THEN INSERT
                (airport_code, city, total_departures, total_arrivals, last_activity)
            VALUES
                (src.airport_code, src.city, src.total_departures,
                 src.total_arrivals, src.last_activity);
        """,
    )

    # ── TASK 5: Build Aggregate Tables ───────────────────────────────────────
    build_aggregates = SnowflakeOperator(
        task_id           = "build_aggregates",
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
        warehouse         = SNOWFLAKE_WAREHOUSE,
        database          = SNOWFLAKE_DATABASE,
        schema            = SNOWFLAKE_SCHEMA_ANALYTICS,
        sql               = """
            -- Hourly summary: one row per airline per hour
            -- This is what the dashboard charts use
            MERGE INTO FLIGHT_DB.ANALYTICS.HOURLY_FLIGHT_SUMMARY AS tgt
            USING (
                SELECT
                    event_date,
                    event_hour,
                    airline_iata,
                    COUNT(DISTINCT flight_id)        AS unique_flights,
                    COUNT(*)                         AS total_events,
                    AVG(altitude)                    AS avg_altitude,
                    AVG(speed)                       AS avg_speed,
                    AVG(delay_minutes)               AS avg_delay,
                    MAX(delay_minutes)               AS max_delay,
                    SUM(CASE WHEN status='active'    THEN 1 ELSE 0 END) AS active_count,
                    SUM(CASE WHEN status='landed'    THEN 1 ELSE 0 END) AS landed_count,
                    SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                    SUM(CASE WHEN is_international   THEN 1 ELSE 0 END) AS intl_flights,
                    CURRENT_TIMESTAMP()              AS summary_created_at
                FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
                WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
                GROUP BY event_date, event_hour, airline_iata
            ) AS src
            ON  tgt.event_date  = src.event_date
            AND tgt.event_hour  = src.event_hour
            AND tgt.airline_iata = src.airline_iata
            WHEN MATCHED THEN UPDATE SET
                unique_flights = src.unique_flights,
                total_events   = src.total_events,
                avg_altitude   = src.avg_altitude,
                avg_speed      = src.avg_speed,
                avg_delay      = src.avg_delay,
                max_delay      = src.max_delay,
                active_count   = src.active_count,
                landed_count   = src.landed_count,
                cancelled_count= src.cancelled_count,
                intl_flights   = src.intl_flights,
                summary_created_at = src.summary_created_at
            WHEN NOT MATCHED THEN INSERT
                (event_date, event_hour, airline_iata, unique_flights, total_events,
                 avg_altitude, avg_speed, avg_delay, max_delay, active_count,
                 landed_count, cancelled_count, intl_flights, summary_created_at)
            VALUES
                (src.event_date, src.event_hour, src.airline_iata, src.unique_flights,
                 src.total_events, src.avg_altitude, src.avg_speed, src.avg_delay,
                 src.max_delay, src.active_count, src.landed_count, src.cancelled_count,
                 src.intl_flights, src.summary_created_at);
        """,
    )

    # ── TASK 6: Log Run Stats ─────────────────────────────────────────────────
    def log_run_stats_fn(**context) -> None:
        """Pull XCom stats and log a final summary for this DAG run."""
        ti     = context["ti"]
        result = ti.xcom_pull(task_ids="clean_transform", key="transform_result")
        run_id = context["run_id"]

        logger.info(
            "DAG run complete | run_id=%s | input=%d | transformed=%d | "
            "deduped=%d | quality_flagged=%d",
            run_id,
            result.get("records_input", 0),
            result.get("records_transformed", 0),
            result.get("duplicates_removed", 0),
            result.get("quality_flagged", 0),
        )

    log_run_stats = PythonOperator(
        task_id         = "log_run_stats",
        python_callable = log_run_stats_fn,
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    (
        check_raw_data
        >> clean_transform
        >> build_fact_flights
        >> build_dim_tables
        >> build_aggregates
        >> log_run_stats
    )