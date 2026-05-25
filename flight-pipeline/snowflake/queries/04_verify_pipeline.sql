-- snowflake/queries/04_verify_pipeline.sql
-- ============================================================
-- Verification queries — run these after pipeline starts.
-- These are the queries your team lead will ask about.
-- ============================================================

USE DATABASE FLIGHT_DB;
USE WAREHOUSE FLIGHT_WH;

-- ─── 1. How much data do we have? ─────────────────────────────────────────────
SELECT
    'RAW'       AS layer,
    COUNT(*)    AS total_rows,
    COUNT(DISTINCT flight_id) AS unique_flights,
    MIN(event_timestamp) AS oldest_record,
    MAX(event_timestamp) AS newest_record,
    DATEDIFF(minute, MAX(event_timestamp), CURRENT_TIMESTAMP()) AS minutes_since_last_record
FROM FLIGHT_DB.RAW.FLIGHTS_RAW

UNION ALL

SELECT
    'CLEAN', COUNT(*), COUNT(DISTINCT flight_id),
    MIN(event_timestamp), MAX(event_timestamp),
    DATEDIFF(minute, MAX(event_timestamp), CURRENT_TIMESTAMP())
FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN

UNION ALL

SELECT
    'FACT', COUNT(*), COUNT(DISTINCT flight_id),
    MIN(event_timestamp), MAX(event_timestamp),
    DATEDIFF(minute, MAX(event_timestamp), CURRENT_TIMESTAMP())
FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS;


-- ─── 2. Transformation funnel ─────────────────────────────────────────────────
-- Shows how many records make it through each layer
SELECT
    raw_count,
    clean_count,
    fact_count,
    ROUND(clean_count::FLOAT / NULLIF(raw_count, 0) * 100, 2) AS raw_to_clean_pct,
    ROUND(fact_count::FLOAT  / NULLIF(clean_count, 0) * 100, 2) AS clean_to_fact_pct
FROM (
    SELECT
        (SELECT COUNT(*) FROM FLIGHT_DB.RAW.FLIGHTS_RAW)        AS raw_count,
        (SELECT COUNT(*) FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN)    AS clean_count,
        (SELECT COUNT(*) FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS) AS fact_count
);


-- ─── 3. Airline performance summary ──────────────────────────────────────────
SELECT
    airline_iata,
    airline_name,
    total_flights,
    ROUND(avg_delay_minutes, 1)  AS avg_delay_mins,
    active_flights,
    last_seen_at
FROM FLIGHT_DB.ANALYTICS.DIM_AIRLINES
ORDER BY total_flights DESC
LIMIT 15;


-- ─── 4. Delay distribution across the fleet ───────────────────────────────────
SELECT
    delay_bucket,
    COUNT(*)                                            AS flights,
    ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER() * 100, 2) AS pct_of_total,
    ROUND(AVG(delay_minutes), 1)                        AS avg_delay_in_bucket
FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
GROUP BY delay_bucket
ORDER BY
    CASE delay_bucket
        WHEN 'on_time'        THEN 1
        WHEN 'minor_delay'    THEN 2
        WHEN 'moderate_delay' THEN 3
        WHEN 'major_delay'    THEN 4
        WHEN 'severe_delay'   THEN 5
    END;


-- ─── 5. Top 10 busiest routes ─────────────────────────────────────────────────
SELECT
    route_key,
    total_flights,
    ROUND(avg_delay_minutes, 1) AS avg_delay_mins,
    ROUND(pct_on_time * 100, 1) AS pct_on_time,
    is_international
FROM FLIGHT_DB.ANALYTICS.ROUTE_PERFORMANCE
WHERE event_date = CURRENT_DATE()
ORDER BY total_flights DESC
LIMIT 10;


-- ─── 6. Hourly traffic pattern (peak hours) ───────────────────────────────────
SELECT
    event_hour,
    SUM(unique_flights)     AS flights_this_hour,
    ROUND(AVG(avg_delay), 1) AS avg_delay_mins,
    SUM(active_count)       AS active_flights,
    SUM(cancelled_count)    AS cancellations
FROM FLIGHT_DB.ANALYTICS.HOURLY_FLIGHT_SUMMARY
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY event_hour
ORDER BY event_hour;


-- ─── 7. Data quality health check ─────────────────────────────────────────────
SELECT
    DATE(transformed_at)     AS check_date,
    COUNT(*)                 AS total_records,
    SUM(CASE WHEN data_quality_flag = TRUE  THEN 1 ELSE 0 END) AS flagged_records,
    ROUND(
        SUM(CASE WHEN data_quality_flag = TRUE THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 2
    )                        AS error_rate_pct
FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
GROUP BY 1
ORDER BY 1 DESC
LIMIT 14;


-- ─── 8. Snowflake credit usage (how much have we spent?) ─────────────────────
SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED)       AS total_credits,
    SUM(CREDITS_USED) * 2   AS approx_cost_usd,   -- $2/credit for X-Small
    COUNT(*)                AS query_count,
    MAX(END_TIME)           AS last_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME
ORDER BY total_credits DESC;