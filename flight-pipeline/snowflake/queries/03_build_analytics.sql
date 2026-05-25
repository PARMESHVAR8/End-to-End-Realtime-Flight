-- snowflake/queries/03_build_analytics.sql
-- ============================================================
-- Builds ANALYTICS layer tables from CLEAN.
-- Run after transform_to_clean.sql completes.
-- ============================================================

USE WAREHOUSE FLIGHT_WH;

-- ─── ROUTE PERFORMANCE TABLE ──────────────────────────────────────────────────
-- Pre-aggregates route-level metrics for the last 7 days.
-- Dashboard shows this as: "Top 10 Most Delayed Routes"
MERGE INTO FLIGHT_DB.ANALYTICS.ROUTE_PERFORMANCE AS target
USING (
    SELECT
        DATE(event_timestamp)                           AS event_date,
        source_airport,
        dest_airport,
        -- Concatenate source→dest with arrow for display
        source_airport || '→' || dest_airport          AS route_key,

        COUNT(DISTINCT flight_id)                       AS total_flights,
        COUNT(*)                                        AS total_events,
        ROUND(AVG(delay_minutes), 2)                   AS avg_delay_minutes,
        MAX(delay_minutes)                              AS max_delay_minutes,

        -- Percentage calculations using RATIO_TO_REPORT window function
        -- Alternative: COUNT(CASE WHEN ... THEN 1 END) / COUNT(*)
        ROUND(
            SUM(CASE WHEN delay_bucket = 'on_time' THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0), 4
        )                                               AS pct_on_time,
        ROUND(
            SUM(CASE WHEN delay_bucket != 'on_time' THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0), 4
        )                                               AS pct_delayed,

        ROUND(AVG(speed), 2)                           AS avg_speed,

        -- Average altitude only for cruise phase (meaningful cruise altitude)
        ROUND(AVG(
            CASE WHEN flight_phase = 'cruise' THEN altitude ELSE NULL END
        ), 0)                                           AS avg_cruise_altitude,

        MAX(is_international)                           AS is_international,
        CURRENT_TIMESTAMP()                             AS summary_created_at

    FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
    WHERE data_quality_flag = FALSE
    AND event_timestamp >= DATEADD(day, -7, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3, 4   -- GROUP BY position numbers (shorthand for the above)
) AS source

ON  target.event_date    = source.event_date
AND target.source_airport = source.source_airport
AND target.dest_airport   = source.dest_airport

WHEN MATCHED THEN UPDATE SET
    total_flights      = source.total_flights,
    total_events       = source.total_events,
    avg_delay_minutes  = source.avg_delay_minutes,
    max_delay_minutes  = source.max_delay_minutes,
    pct_on_time        = source.pct_on_time,
    pct_delayed        = source.pct_delayed,
    avg_speed          = source.avg_speed,
    avg_cruise_altitude= source.avg_cruise_altitude,
    summary_created_at = source.summary_created_at

WHEN NOT MATCHED THEN INSERT (
    event_date, source_airport, dest_airport, route_key,
    total_flights, total_events, avg_delay_minutes, max_delay_minutes,
    pct_on_time, pct_delayed, avg_speed, avg_cruise_altitude,
    is_international, summary_created_at
) VALUES (
    source.event_date, source.source_airport, source.dest_airport, source.route_key,
    source.total_flights, source.total_events, source.avg_delay_minutes,
    source.max_delay_minutes, source.pct_on_time, source.pct_delayed,
    source.avg_speed, source.avg_cruise_altitude,
    source.is_international, source.summary_created_at
);