-- snowflake/queries/02_transform_to_clean.sql
-- ============================================================
-- Transforms RAW → CLEAN with SQL-level enrichment.
-- This runs AFTER the Pandas transformation in Airflow.
-- Some transformations are easier in SQL (window functions,
-- joins to reference data) than in Pandas.
-- ============================================================

USE WAREHOUSE FLIGHT_WH;

-- ─── STEP 1: Insert into CLEAN from RAW ──────────────────────────────────────
-- Uses ROW_NUMBER() window function to deduplicate:
-- If the same event_id appears in RAW twice (Kafka at-least-once),
-- ROW_NUMBER() = 1 picks only the first occurrence.
MERGE INTO FLIGHT_DB.CLEAN.FLIGHTS_CLEAN AS target
USING (
    -- CTE (Common Table Expression): named subquery for readability
    -- WHY CTE vs subquery? CTEs are easier to debug —
    -- you can run each CTE block independently to inspect intermediate results.
    WITH ranked_raw AS (
        SELECT
            *,
            -- Window function: number rows by event_id, ordered by loaded_at
            -- ROW_NUMBER() = 1 means "first time we saw this event_id"
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY loaded_at ASC
            ) AS row_num
        FROM FLIGHT_DB.RAW.FLIGHTS_RAW
        WHERE is_transformed = FALSE
    ),

    deduplicated AS (
        -- Only keep first occurrence of each event_id
        SELECT * FROM ranked_raw WHERE row_num = 1
    ),

    enriched AS (
        SELECT
            d.event_id,
            d.flight_id,

            -- TRIM removes leading/trailing whitespace
            -- INITCAP title-cases: "air india" → "Air India"
            TRIM(INITCAP(d.airline))    AS airline,
            UPPER(TRIM(d.airline_iata)) AS airline_iata,
            d.flight_number,
            UPPER(TRIM(d.source_airport)) AS source_airport,
            UPPER(TRIM(d.dest_airport))   AS dest_airport,
            d.source_city,
            d.dest_city,

            -- COALESCE: use first non-null value
            -- If latitude is null → default to 0 (safer than crashing)
            COALESCE(d.latitude,  0.0)  AS latitude,
            COALESCE(d.longitude, 0.0)  AS longitude,

            -- GREATEST/LEAST: clip values to range
            -- GREATEST(altitude, 0) ensures no negative altitude
            -- LEAST(..., 60000) caps at max aircraft ceiling
            GREATEST(COALESCE(d.altitude, 0), 0)            AS altitude,
            GREATEST(LEAST(COALESCE(d.speed, 0), 1200), 0)  AS speed,
            d.heading,

            -- Normalise status to lowercase
            LOWER(COALESCE(d.status, 'unknown')) AS status,

            d.departure_time,
            d.arrival_time,
            GREATEST(COALESCE(d.delay_minutes, 0), 0) AS delay_minutes,
            d.aircraft_type,
            d.event_timestamp,
            d.source,

            -- ── Derived columns ───────────────────────────────────────────
            -- is_international: neither airport is in our India set
            CASE
                WHEN UPPER(d.source_airport) IN (
                    'DEL','BOM','BLR','MAA','CCU','HYD','AMD','COK','GOI','JAI'
                ) AND UPPER(d.dest_airport) IN (
                    'DEL','BOM','BLR','MAA','CCU','HYD','AMD','COK','GOI','JAI'
                ) THEN FALSE
                ELSE TRUE
            END AS is_international,

            -- delay_bucket: CASE statement is SQL's if/elif/else
            CASE
                WHEN COALESCE(d.delay_minutes, 0) = 0   THEN 'on_time'
                WHEN d.delay_minutes <= 15               THEN 'minor_delay'
                WHEN d.delay_minutes <= 60               THEN 'moderate_delay'
                WHEN d.delay_minutes <= 180              THEN 'major_delay'
                ELSE                                          'severe_delay'
            END AS delay_bucket,

            -- flight_phase based on altitude
            CASE
                WHEN COALESCE(d.altitude, 0) < 1000     THEN 'ground'
                WHEN d.altitude < 15000                  THEN 'climbing'
                WHEN d.altitude < 32000                  THEN 'mid_altitude'
                ELSE                                          'cruise'
            END AS flight_phase,

            -- region: lat/lon bounding boxes
            CASE
                WHEN d.latitude BETWEEN 8 AND 37
                 AND d.longitude BETWEEN 68 AND 97      THEN 'India'
                WHEN d.latitude >= 35                    THEN 'Europe_Asia'
                WHEN d.latitude <= 0                     THEN 'Southern'
                ELSE                                          'Middle_East_SE_Asia'
            END AS region,

            -- data_quality_flag: TRUE if any anomaly detected
            CASE
                WHEN d.latitude IS NULL OR d.longitude IS NULL  THEN TRUE
                WHEN d.source_airport IN ('???','')             THEN TRUE
                WHEN d.altitude < 0                             THEN TRUE
                WHEN d.speed < 0                                THEN TRUE
                ELSE FALSE
            END AS data_quality_flag,

            d.loaded_at

        FROM deduplicated d
        -- Only load records with minimum required fields
        WHERE d.flight_id IS NOT NULL
        AND   d.event_timestamp IS NOT NULL
    )
    SELECT * FROM enriched
) AS source

ON target.event_id = source.event_id

WHEN NOT MATCHED THEN INSERT (
    event_id, flight_id, airline, airline_iata, flight_number,
    source_airport, dest_airport, source_city, dest_city,
    latitude, longitude, altitude, speed, heading, status,
    departure_time, arrival_time, delay_minutes, aircraft_type,
    event_timestamp, source,
    is_international, delay_bucket, flight_phase, region,
    data_quality_flag, loaded_at, transformed_at
)
VALUES (
    source.event_id, source.flight_id, source.airline, source.airline_iata,
    source.flight_number, source.source_airport, source.dest_airport,
    source.source_city, source.dest_city,
    source.latitude, source.longitude, source.altitude, source.speed,
    source.heading, source.status, source.departure_time, source.arrival_time,
    source.delay_minutes, source.aircraft_type, source.event_timestamp, source.source,
    source.is_international, source.delay_bucket, source.flight_phase, source.region,
    source.data_quality_flag, source.loaded_at, CURRENT_TIMESTAMP()
);

-- Mark raw records as transformed
UPDATE FLIGHT_DB.RAW.FLIGHTS_RAW
SET
    is_transformed = TRUE,
    transformed_at = CURRENT_TIMESTAMP()
WHERE is_transformed = FALSE
AND event_id IN (
    SELECT event_id FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
    WHERE transformed_at >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
);