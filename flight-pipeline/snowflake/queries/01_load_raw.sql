-- snowflake/queries/01_load_raw.sql
-- ============================================================
-- Loads from FLIGHTS_STAGING → FLIGHTS_RAW using MERGE.
-- MERGE is idempotent: re-running this never creates duplicates.
-- Called by: Airflow flight_ingestion_dag → load_raw_snowflake task
-- ============================================================

USE WAREHOUSE FLIGHT_WH;

-- Step 1: MERGE staging into permanent raw table
-- MERGE logic:
--   For each row in source (STAGING):
--     If event_id already exists in target (RAW) → do nothing (WHEN MATCHED → skip)
--     If event_id is new → INSERT it
-- This means: every event_id appears exactly once in FLIGHTS_RAW, always.
MERGE INTO FLIGHT_DB.RAW.FLIGHTS_RAW AS target

-- Source: only unprocessed staging rows
USING (
    SELECT *
    FROM FLIGHT_DB.RAW.FLIGHTS_STAGING
    WHERE processed_by_airflow = FALSE
) AS source

-- Match condition: same event_id = same record
ON target.event_id = source.event_id

-- If it's already in RAW: do nothing
WHEN MATCHED THEN UPDATE SET
    target.ingestion_batch_id = source.ingestion_batch_id  -- just track latest batch

-- If it's new: insert it
WHEN NOT MATCHED THEN INSERT (
    event_id, flight_id, airline, airline_iata, flight_number,
    source_airport, dest_airport, source_city, dest_city,
    latitude, longitude, altitude, speed, heading,
    status, departure_time, arrival_time, delay_minutes,
    aircraft_type, event_timestamp, source, raw_json,
    loaded_at, is_transformed, ingestion_batch_id
)
VALUES (
    source.event_id, source.flight_id, source.airline, source.airline_iata,
    source.flight_number, source.source_airport, source.dest_airport,
    source.source_city, source.dest_city,
    source.latitude, source.longitude, source.altitude, source.speed,
    source.heading, source.status, source.departure_time, source.arrival_time,
    source.delay_minutes, source.aircraft_type, source.event_timestamp,
    source.source, source.raw_json,
    source.loaded_at, FALSE, source.ingestion_batch_id
);

-- Step 2: Mark staging rows as processed
-- After MERGE, these rows will not be picked up again
UPDATE FLIGHT_DB.RAW.FLIGHTS_STAGING
SET processed_by_airflow = TRUE
WHERE processed_by_airflow = FALSE;

-- Step 3: Verify the load
SELECT
    COUNT(*)                                        AS total_raw_records,
    COUNT(CASE WHEN is_transformed = FALSE THEN 1 END) AS pending_transformation,
    MAX(loaded_at)                                  AS latest_record,
    DATEDIFF(minute, MAX(loaded_at), CURRENT_TIMESTAMP()) AS minutes_since_last_load
FROM FLIGHT_DB.RAW.FLIGHTS_RAW;