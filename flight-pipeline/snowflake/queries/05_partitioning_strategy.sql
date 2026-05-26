-- snowflake/queries/05_partitioning_strategy.sql
-- ============================================================
-- PARTITIONING & CLUSTERING in Snowflake
-- ============================================================
-- Snowflake doesn't use traditional partitions like Hive/Spark.
-- Instead it uses MICRO-PARTITIONS + CLUSTERING KEYS.
-- The effect is the same: queries skip irrelevant data.
-- ============================================================

USE DATABASE FLIGHT_DB;
USE WAREHOUSE FLIGHT_WH;

-- ─────────────────────────────────────────────────────────────
-- CONCEPT: How clustering works
--
-- Without clustering:
--   A query for event_date = '2024-06-15' must scan ALL micro-partitions
--   because rows for that date could be scattered everywhere.
--
-- With CLUSTER BY (event_date):
--   Snowflake groups rows with the same event_date into the same
--   micro-partitions. The query skips all other dates immediately.
--   This is called "partition pruning."
--
-- HOW TO CHECK IF CLUSTERING IS HELPING:
-- ─────────────────────────────────────────────────────────────

-- Check clustering depth of FACT_FLIGHTS
-- Lower depth = better clustering = faster queries
SELECT SYSTEM$CLUSTERING_DEPTH(
    'FLIGHT_DB.ANALYTICS.FACT_FLIGHTS',
    '(event_date, airline_iata)'
) AS clustering_depth;
-- Target: depth < 3. If > 5, consider reclustering.

-- Check clustering information (what % is already clustered)
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'FLIGHT_DB.ANALYTICS.FACT_FLIGHTS',
    '(event_date, airline_iata)'
);
-- Look for "average_depth" and "average_overlaps"
-- average_overlaps = 0 means perfect clustering (no query skipping needed)


-- ─────────────────────────────────────────────────────────────
-- MANUAL RECLUSTERING
-- Over time, as you INSERT new rows, clustering degrades.
-- Snowflake auto-reclusters in the background BUT you can
-- trigger it manually after large bulk loads.
-- ─────────────────────────────────────────────────────────────
ALTER TABLE FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    RECLUSTER MAX_SIZE = 1073741824;  -- Recluster up to 1GB at a time


-- ─────────────────────────────────────────────────────────────
-- QUERY PERFORMANCE COMPARISON
-- Run both queries and compare "partitions scanned" in query profile
-- ─────────────────────────────────────────────────────────────

-- Slow query (no cluster filter — scans everything)
SELECT COUNT(*), AVG(speed), AVG(delay_minutes)
FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS;

-- Fast query (cluster filter — skips most partitions)
SELECT COUNT(*), AVG(speed), AVG(delay_minutes)
FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
WHERE event_date = CURRENT_DATE()         -- Cluster key filter!
AND   airline_iata = 'AI';               -- Second cluster key!

-- After running: click "Query ID" → "Query Profile" in Snowflake UI
-- Look at "Partitions scanned vs Partitions total"
-- Good: 5 scanned / 200 total = 97.5% skipped
-- Bad:  200 scanned / 200 total = 0% skipped


-- ─────────────────────────────────────────────────────────────
-- PARTITIONED VIEW PATTERN
-- Create views that automatically filter to recent data.
-- Dashboard queries hit the view, not the full table.
-- ─────────────────────────────────────────────────────────────

-- View: last 7 days of fact data (what 90% of dashboard queries need)
CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.FACT_FLIGHTS_RECENT AS
SELECT *
FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
COMMENT = 'Rolling 7-day window of fact_flights — use for dashboard queries';

-- View: today only (for live dashboard KPIs)
CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.FACT_FLIGHTS_TODAY AS
SELECT *
FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
WHERE event_date = CURRENT_DATE()
COMMENT = 'Todays flights only — optimised for live KPI cards';

-- View: clean data only (excludes quality-flagged records)
CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.FACT_FLIGHTS_CLEAN AS
SELECT *
FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
WHERE data_quality_flag = FALSE
COMMENT = 'Quality-verified flights only — safe for analytics';


-- ─────────────────────────────────────────────────────────────
-- INCREMENTAL AGGREGATE REFRESH PATTERN
-- Instead of rebuilding the whole HOURLY_FLIGHT_SUMMARY table,
-- only update today's data.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE FLIGHT_DB.ANALYTICS.REFRESH_HOURLY_SUMMARY(
    REFRESH_DATE DATE DEFAULT CURRENT_DATE()
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Delete only today's data (yesterday+ is already correct)
    DELETE FROM FLIGHT_DB.ANALYTICS.HOURLY_FLIGHT_SUMMARY
    WHERE event_date = :REFRESH_DATE;

    -- Re-insert just today's aggregates from FACT table
    INSERT INTO FLIGHT_DB.ANALYTICS.HOURLY_FLIGHT_SUMMARY (
        event_date, event_hour, airline_iata,
        unique_flights, total_events,
        avg_altitude, avg_speed, avg_delay, max_delay,
        active_count, landed_count, cancelled_count,
        intl_flights, domestic_flights, summary_created_at
    )
    SELECT
        event_date,
        event_hour,
        airline_iata,
        COUNT(DISTINCT flight_id)                                   AS unique_flights,
        COUNT(*)                                                    AS total_events,
        ROUND(AVG(altitude), 0)                                     AS avg_altitude,
        ROUND(AVG(speed), 1)                                        AS avg_speed,
        ROUND(AVG(delay_minutes), 1)                                AS avg_delay,
        MAX(delay_minutes)                                          AS max_delay,
        SUM(IFF(status = 'active',    1, 0))                        AS active_count,
        SUM(IFF(status = 'landed',    1, 0))                        AS landed_count,
        SUM(IFF(status = 'cancelled', 1, 0))                        AS cancelled_count,
        SUM(IFF(is_international,     1, 0))                        AS intl_flights,
        SUM(IFF(NOT is_international, 1, 0))                        AS domestic_flights,
        CURRENT_TIMESTAMP()                                         AS summary_created_at
    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE event_date = :REFRESH_DATE
    AND   data_quality_flag = FALSE
    GROUP BY 1, 2, 3;

    RETURN 'Refreshed hourly summary for ' || :REFRESH_DATE::VARCHAR;
END;
$$
COMMENT = 'Incrementally refreshes hourly summary for a given date. Default: today.';

-- Call the procedure:
CALL FLIGHT_DB.ANALYTICS.REFRESH_HOURLY_SUMMARY();
-- Or for a specific date:
CALL FLIGHT_DB.ANALYTICS.REFRESH_HOURLY_SUMMARY('2024-06-15'::DATE);