-- snowflake/queries/06_business_analytics.sql
-- ================================================================
-- FLIGHT PIPELINE — BUSINESS ANALYTICS QUERIES
-- ================================================================
-- Each query answers one specific business question.
-- Every query is:
--   1. Named with a business-friendly label
--   2. Commented with WHO uses it and WHAT DECISION it enables
--   3. Designed to run in < 2 seconds using pre-aggregated tables
--   4. Idempotent — safe to run repeatedly
--
-- QUERY READING GUIDE:
--   CTE      = named subquery, defined with WITH keyword
--   WINDOW   = calculation across a set of rows (ROW_NUMBER, LAG, RANK)
--   QUALIFY  = Snowflake-specific: filters AFTER window function runs
--              (like WHERE but for window function results)
--   IFF()    = Snowflake shorthand for CASE WHEN x THEN a ELSE b END
--   RATIO_TO_REPORT() = divides each row's value by the group total
-- ================================================================

USE DATABASE FLIGHT_DB;
USE WAREHOUSE FLIGHT_WH;


-- ================================================================
-- QUERY 1: MOST ACTIVE AIRLINES RIGHT NOW
-- ================================================================
-- BUSINESS QUESTION:
--   Which airlines have the most flights in the air at this moment?
--   Used by: Operations team live dashboard
--   Decision: Which airline coordination desk to staff most heavily
--
-- WHY THIS MATTERS:
--   If IndiGo has 40 active flights and Air India has 5,
--   operations allocates radio bandwidth and controller time accordingly.
--   A 15-minute-old answer is useless here — this must be live.
--
-- DESIGN NOTES:
--   We query FACT_FLIGHTS_TODAY view (clustered on today's date)
--   NOT the full FACT_FLIGHTS table.
--   This makes the query scan < 1% of total data.
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_ACTIVE_AIRLINES_NOW AS
WITH latest_per_flight AS (
    -- For each flight currently active, get its most recent position update
    -- ROW_NUMBER() partitioned by flight_id, ordered by event_timestamp DESC
    -- = row 1 is the most recent update for each flight
    SELECT
        flight_id,
        airline_iata,
        status,
        altitude,
        speed,
        latitude,
        longitude,
        delay_minutes,
        delay_bucket,
        source_airport,
        dest_airport,
        route_key,
        event_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY flight_id
            ORDER BY event_timestamp DESC
        ) AS rn
    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE event_date = CURRENT_DATE()
      AND status = 'active'
      AND data_quality_flag = FALSE
      -- Only consider events from the last 30 minutes as "active"
      AND event_timestamp >= DATEADD(minute, -30, CURRENT_TIMESTAMP())
),

current_positions AS (
    -- Keep only the most recent snapshot per flight
    SELECT * FROM latest_per_flight WHERE rn = 1
),

airline_summary AS (
    SELECT
        cp.airline_iata,
        da.airline_name,

        -- Volume metrics
        COUNT(DISTINCT cp.flight_id)                        AS active_flights,
        COUNT(*)                                            AS position_updates,

        -- Performance metrics — aggregate across all active flights
        ROUND(AVG(cp.altitude),  0)                         AS avg_altitude_ft,
        ROUND(AVG(cp.speed),     1)                         AS avg_speed_kmh,
        ROUND(AVG(cp.delay_minutes), 1)                     AS avg_delay_minutes,
        MAX(cp.delay_minutes)                               AS max_delay_minutes,

        -- Delay breakdown
        SUM(IFF(cp.delay_bucket = 'on_time',        1, 0)) AS on_time_count,
        SUM(IFF(cp.delay_bucket != 'on_time',       1, 0)) AS delayed_count,

        -- Percentage of fleet that is delayed right now
        ROUND(
            SUM(IFF(cp.delay_bucket != 'on_time', 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        1)                                                  AS pct_delayed,

        -- Routes this airline is currently flying
        COUNT(DISTINCT cp.route_key)                        AS active_routes,

        -- Most recent update from this airline
        MAX(cp.event_timestamp)                             AS last_seen_at

    FROM current_positions cp
    LEFT JOIN FLIGHT_DB.ANALYTICS.DIM_AIRLINES da
           ON cp.airline_iata = da.airline_code
    GROUP BY cp.airline_iata, da.airline_name
)

SELECT
    -- Rank by active flight count (most active = rank 1)
    RANK() OVER (ORDER BY active_flights DESC)              AS activity_rank,
    airline_iata,
    COALESCE(airline_name, airline_iata)                    AS airline_name,
    active_flights,
    avg_altitude_ft,
    avg_speed_kmh,
    avg_delay_minutes,
    max_delay_minutes,
    on_time_count,
    delayed_count,
    pct_delayed,
    active_routes,
    last_seen_at
FROM airline_summary
ORDER BY active_flights DESC;

-- Test it:
SELECT * FROM FLIGHT_DB.ANALYTICS.V_ACTIVE_AIRLINES_NOW LIMIT 10;


-- ================================================================
-- QUERY 2: DELAY PATTERN ANALYSIS
-- ================================================================
-- BUSINESS QUESTION:
--   When do delays peak? By airline? By route? By hour of day?
--   Used by: Operations + Airport Management
--   Decision: Pre-position staff and aircraft during peak delay windows
--
-- AVIATION CONTEXT:
--   The "delay cascade" is a well-known phenomenon — a 30-minute
--   morning delay causes 3 hours of downstream delays throughout the day
--   because the same aircraft flies multiple sectors.
--   This query helps identify the ORIGIN of cascades.
--
-- KEY SQL TECHNIQUES:
--   LAG() window function: compares each hour to the PREVIOUS hour
--   This reveals acceleration — "delays are getting worse, not just high"
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_DELAY_PATTERNS AS
WITH hourly_delays AS (
    SELECT
        event_date,
        event_hour,
        airline_iata,

        COUNT(DISTINCT flight_id)                           AS flights,
        ROUND(AVG(delay_minutes), 2)                        AS avg_delay,
        ROUND(MEDIAN(delay_minutes), 2)                     AS median_delay,
        MAX(delay_minutes)                                  AS max_delay,
        PERCENTILE_CONT(0.90) WITHIN GROUP (
            ORDER BY delay_minutes
        )                                                   AS p90_delay,

        -- Delay bucket breakdown
        SUM(IFF(delay_bucket = 'on_time',        1, 0))    AS on_time,
        SUM(IFF(delay_bucket = 'minor_delay',    1, 0))    AS minor_delays,
        SUM(IFF(delay_bucket = 'moderate_delay', 1, 0))    AS moderate_delays,
        SUM(IFF(delay_bucket = 'major_delay',    1, 0))    AS major_delays,
        SUM(IFF(delay_bucket = 'severe_delay',   1, 0))    AS severe_delays,

        -- On-time performance rate
        ROUND(
            SUM(IFF(delay_bucket = 'on_time', 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        2)                                                  AS otp_rate_pct

    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE data_quality_flag = FALSE
      AND event_date >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY 1, 2, 3
),

with_trend AS (
    SELECT
        *,
        -- LAG(avg_delay, 1): average delay from the PREVIOUS hour
        -- Positive delta = delays getting worse. Negative = improving.
        LAG(avg_delay, 1) OVER (
            PARTITION BY airline_iata, event_date
            ORDER BY event_hour
        )                                                   AS prev_hour_avg_delay,

        avg_delay - LAG(avg_delay, 1) OVER (
            PARTITION BY airline_iata, event_date
            ORDER BY event_hour
        )                                                   AS delay_delta_vs_prev_hour,

        -- 3-hour rolling average: smooths out spikes
        AVG(avg_delay) OVER (
            PARTITION BY airline_iata, event_date
            ORDER BY event_hour
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                                                   AS rolling_3h_avg_delay

    FROM hourly_delays
)

SELECT
    event_date,
    event_hour,
    airline_iata,
    flights,
    avg_delay,
    median_delay,
    max_delay,
    p90_delay,
    on_time,
    minor_delays,
    moderate_delays,
    major_delays,
    severe_delays,
    otp_rate_pct,
    prev_hour_avg_delay,
    ROUND(delay_delta_vs_prev_hour, 2)                      AS delay_delta_vs_prev_hour,
    ROUND(rolling_3h_avg_delay, 2)                          AS rolling_3h_avg_delay,
    -- Signal: is this hour significantly worse than the rolling average?
    CASE
        WHEN avg_delay > rolling_3h_avg_delay * 1.5
        THEN 'DELAY_SPIKE'
        WHEN avg_delay < rolling_3h_avg_delay * 0.7
        THEN 'IMPROVING'
        ELSE 'NORMAL'
    END                                                     AS delay_signal
FROM with_trend
ORDER BY event_date DESC, event_hour, airline_iata;

-- Useful slice: worst delay hours across all airlines
SELECT
    event_hour,
    ROUND(AVG(avg_delay), 1)    AS avg_delay_all_airlines,
    MAX(max_delay)              AS worst_single_delay,
    SUM(severe_delays)          AS total_severe_delays
FROM FLIGHT_DB.ANALYTICS.V_DELAY_PATTERNS
WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY event_hour
ORDER BY avg_delay_all_airlines DESC;


-- ================================================================
-- QUERY 3: AIRLINE PERFORMANCE LEAGUE TABLE
-- ================================================================
-- BUSINESS QUESTION:
--   Rank all airlines by on-time performance, speed, and reliability.
--   Used by: Management, BI dashboards, press releases
--   Decision: Partnership decisions, slot allocation, premium route assignment
--
-- AVIATION CONTEXT:
--   Airlines are contractually obligated to meet OTP (On-Time Performance)
--   thresholds in codeshare agreements. This table is the source of truth.
--   DGCA (India's aviation regulator) publishes similar monthly reports.
--
-- KEY SQL:
--   NTILE(4): divides airlines into 4 equal quartiles by OTP rate
--   Quartile 1 = best performers, Quartile 4 = worst performers
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_AIRLINE_LEAGUE_TABLE AS
WITH airline_stats AS (
    SELECT
        f.airline_iata,
        COALESCE(da.airline_name, f.airline_iata)           AS airline_name,

        -- Volume
        COUNT(DISTINCT f.flight_id)                         AS total_unique_flights,
        COUNT(*)                                            AS total_events,
        COUNT(DISTINCT f.event_date)                        AS days_of_data,
        COUNT(DISTINCT f.route_key)                         AS unique_routes_flown,

        -- On-Time Performance
        ROUND(
            SUM(IFF(f.delay_bucket = 'on_time', 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        2)                                                  AS otp_rate_pct,

        -- Delay metrics
        ROUND(AVG(f.delay_minutes), 2)                      AS avg_delay_minutes,
        ROUND(MEDIAN(f.delay_minutes), 2)                   AS median_delay_minutes,
        MAX(f.delay_minutes)                                AS worst_single_delay,

        -- Delay severity breakdown (as % of flights)
        ROUND(SUM(IFF(f.delay_bucket = 'minor_delay',    1,0))::FLOAT / NULLIF(COUNT(*),0)*100, 1) AS pct_minor_delay,
        ROUND(SUM(IFF(f.delay_bucket = 'moderate_delay', 1,0))::FLOAT / NULLIF(COUNT(*),0)*100, 1) AS pct_moderate_delay,
        ROUND(SUM(IFF(f.delay_bucket = 'major_delay',    1,0))::FLOAT / NULLIF(COUNT(*),0)*100, 1) AS pct_major_delay,
        ROUND(SUM(IFF(f.delay_bucket = 'severe_delay',   1,0))::FLOAT / NULLIF(COUNT(*),0)*100, 1) AS pct_severe_delay,

        -- Speed (proxy for operational efficiency — slow = fuel-inefficient)
        ROUND(AVG(f.speed), 1)                              AS avg_speed_kmh,
        ROUND(AVG(f.altitude), 0)                           AS avg_altitude_ft,

        -- International mix
        ROUND(
            SUM(IFF(f.is_international, 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        1)                                                  AS pct_international_flights,

        -- Consistency score: low stddev = reliable schedule
        ROUND(STDDEV(f.delay_minutes), 2)                   AS delay_stddev_minutes

    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS f
    LEFT JOIN FLIGHT_DB.ANALYTICS.DIM_AIRLINES da
           ON f.airline_iata = da.airline_code
    WHERE f.data_quality_flag = FALSE
      AND f.event_date >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY f.airline_iata, da.airline_name
    HAVING COUNT(DISTINCT f.flight_id) >= 5   -- Minimum 5 flights to be ranked
)

SELECT
    -- Performance rankings
    RANK()  OVER (ORDER BY otp_rate_pct DESC)               AS otp_rank,
    RANK()  OVER (ORDER BY avg_delay_minutes ASC)           AS delay_rank,
    RANK()  OVER (ORDER BY delay_stddev_minutes ASC)        AS consistency_rank,
    NTILE(4) OVER (ORDER BY otp_rate_pct DESC)              AS performance_quartile,

    airline_iata,
    airline_name,
    total_unique_flights,
    days_of_data,
    unique_routes_flown,
    otp_rate_pct,
    avg_delay_minutes,
    median_delay_minutes,
    worst_single_delay,
    pct_minor_delay,
    pct_moderate_delay,
    pct_major_delay,
    pct_severe_delay,
    avg_speed_kmh,
    avg_altitude_ft,
    pct_international_flights,
    delay_stddev_minutes,

    -- Composite score: weighted combination of OTP and consistency
    -- Higher = better overall performer
    ROUND(
        (otp_rate_pct * 0.6)
        + ((100 - LEAST(avg_delay_minutes, 100)) * 0.3)
        + ((100 - LEAST(delay_stddev_minutes, 100)) * 0.1),
    2)                                                      AS composite_score

FROM airline_stats
ORDER BY composite_score DESC;

-- Quick view: top 5 and bottom 5
(SELECT 'TOP 5' AS tier, * FROM FLIGHT_DB.ANALYTICS.V_AIRLINE_LEAGUE_TABLE LIMIT 5)
UNION ALL
(SELECT 'BOTTOM 5', * FROM FLIGHT_DB.ANALYTICS.V_AIRLINE_LEAGUE_TABLE
 ORDER BY composite_score ASC LIMIT 5);


-- ================================================================
-- QUERY 4: PEAK TRAFFIC HOURS
-- ================================================================
-- BUSINESS QUESTION:
--   Which hours of the day have the most flights?
--   Used by: Airport authority, ATC, infrastructure teams
--   Decision: Staff scheduling, runway slot allocation, gate assignments
--
-- AVIATION CONTEXT:
--   Indian airports have strict slot constraints (max N landings/hour).
--   DGCA approves slots months in advance.
--   Understanding peak hours is critical for capacity planning.
--   This query feeds into the slot allocation model.
--
-- KEY SQL:
--   RATIO_TO_REPORT: each hour's share of total weekly traffic (sums to 1.0)
--   This normalises across different-length weeks so you can compare
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_PEAK_TRAFFIC_HOURS AS
WITH hourly_totals AS (
    SELECT
        event_hour,
        day_of_week,

        COUNT(DISTINCT flight_id)                           AS unique_flights,
        COUNT(*)                                            AS total_events,
        ROUND(AVG(speed), 1)                                AS avg_speed,
        ROUND(AVG(altitude), 0)                             AS avg_altitude,
        ROUND(AVG(delay_minutes), 2)                        AS avg_delay,
        SUM(IFF(status = 'active',    1, 0))                AS active_flights,
        SUM(IFF(status = 'landed',    1, 0))                AS landings,
        SUM(IFF(status = 'cancelled', 1, 0))                AS cancellations

    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE data_quality_flag = FALSE
      AND event_date >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY event_hour, day_of_week
),

hour_aggregated AS (
    -- Roll up across all days of week → one row per hour
    SELECT
        event_hour,
        SUM(unique_flights)                                 AS total_flights,
        SUM(total_events)                                   AS total_events,
        ROUND(AVG(avg_speed), 1)                            AS avg_speed,
        ROUND(AVG(avg_altitude), 0)                         AS avg_altitude,
        ROUND(AVG(avg_delay), 2)                            AS avg_delay_minutes,
        SUM(active_flights)                                 AS active_flights,
        SUM(landings)                                       AS landings,
        SUM(cancellations)                                  AS cancellations
    FROM hourly_totals
    GROUP BY event_hour
)

SELECT
    event_hour,
    -- Convert 24h integer to readable label: 14 → '14:00'
    LPAD(event_hour::VARCHAR, 2, '0') || ':00'              AS hour_label,

    total_flights,
    total_events,

    -- Share of daily traffic this hour carries
    ROUND(
        RATIO_TO_REPORT(total_flights) OVER () * 100,
    2)                                                      AS pct_of_daily_traffic,

    avg_speed,
    avg_altitude,
    avg_delay_minutes,
    active_flights,
    landings,
    cancellations,

    -- Classify into traffic bands for easy colour-coding in dashboard
    CASE
        WHEN total_flights >= PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY total_flights) OVER ()
        THEN 'PEAK'
        WHEN total_flights >= PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_flights) OVER ()
        THEN 'BUSY'
        WHEN total_flights >= PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_flights) OVER ()
        THEN 'MODERATE'
        ELSE 'QUIET'
    END                                                     AS traffic_band,

    -- Rank 1 = busiest hour of the day
    RANK() OVER (ORDER BY total_flights DESC)               AS traffic_rank

FROM hour_aggregated
ORDER BY event_hour;

-- Result: tells you that 7am–9am and 6pm–9pm are peak in Indian aviation
SELECT hour_label, total_flights, pct_of_daily_traffic, traffic_band
FROM FLIGHT_DB.ANALYTICS.V_PEAK_TRAFFIC_HOURS
ORDER BY traffic_rank;


-- ================================================================
-- QUERY 5: AIRPORT CONGESTION ANALYSIS
-- ================================================================
-- BUSINESS QUESTION:
--   Which airports are the busiest? Which are most delayed?
--   Used by: Ground operations, slot coordinators
--   Decision: Gate assignment, turnaround time planning
--
-- KEY SQL:
--   UNION ALL: combines departures and arrivals into one dataset
--   PIVOT concept: different metrics for same airport from two angles
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_AIRPORT_CONGESTION AS
WITH departures AS (
    SELECT
        source_airport                                      AS airport_code,
        'departure'                                         AS movement_type,
        COUNT(DISTINCT flight_id)                           AS flight_count,
        ROUND(AVG(delay_minutes), 2)                        AS avg_delay,
        MAX(delay_minutes)                                  AS max_delay,
        SUM(IFF(delay_bucket != 'on_time', 1, 0))          AS delayed_flights,
        COUNT(*)                                            AS event_count
    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE data_quality_flag = FALSE
      AND event_date >= DATEADD(day, -7, CURRENT_DATE())
      AND source_airport IS NOT NULL
    GROUP BY source_airport
),

arrivals AS (
    SELECT
        dest_airport                                        AS airport_code,
        'arrival'                                           AS movement_type,
        COUNT(DISTINCT flight_id)                           AS flight_count,
        ROUND(AVG(delay_minutes), 2)                        AS avg_delay,
        MAX(delay_minutes)                                  AS max_delay,
        SUM(IFF(delay_bucket != 'on_time', 1, 0))          AS delayed_flights,
        COUNT(*)                                            AS event_count
    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE data_quality_flag = FALSE
      AND event_date >= DATEADD(day, -7, CURRENT_DATE())
      AND dest_airport IS NOT NULL
    GROUP BY dest_airport
),

combined AS (
    SELECT * FROM departures
    UNION ALL
    SELECT * FROM arrivals
),

airport_summary AS (
    SELECT
        c.airport_code,
        COALESCE(da.city, c.airport_code)                  AS city,
        SUM(c.flight_count)                                 AS total_movements,
        SUM(IFF(c.movement_type='departure', c.flight_count, 0)) AS departures,
        SUM(IFF(c.movement_type='arrival',   c.flight_count, 0)) AS arrivals,
        ROUND(AVG(c.avg_delay), 2)                          AS avg_delay_minutes,
        MAX(c.max_delay)                                    AS max_delay_minutes,
        SUM(c.delayed_flights)                              AS total_delayed,
        ROUND(
            SUM(c.delayed_flights)::FLOAT
            / NULLIF(SUM(c.flight_count), 0) * 100,
        2)                                                  AS delay_rate_pct,
        -- Congestion index: high traffic + high delay = most congested
        ROUND(
            (SUM(c.flight_count) * 0.5)
            + (AVG(c.avg_delay)  * 0.5),
        2)                                                  AS congestion_index
    FROM combined c
    LEFT JOIN FLIGHT_DB.ANALYTICS.DIM_AIRPORTS da
           ON c.airport_code = da.airport_code
    GROUP BY c.airport_code, da.city
)

SELECT
    RANK() OVER (ORDER BY total_movements DESC)             AS traffic_rank,
    RANK() OVER (ORDER BY delay_rate_pct DESC)              AS delay_rank,
    RANK() OVER (ORDER BY congestion_index DESC)            AS congestion_rank,
    airport_code,
    city,
    total_movements,
    departures,
    arrivals,
    avg_delay_minutes,
    max_delay_minutes,
    total_delayed,
    delay_rate_pct,
    congestion_index
FROM airport_summary
ORDER BY congestion_index DESC;

SELECT * FROM FLIGHT_DB.ANALYTICS.V_AIRPORT_CONGESTION LIMIT 10;


-- ================================================================
-- QUERY 6: ROUTE EFFICIENCY RANKING
-- ================================================================
-- BUSINESS QUESTION:
--   Which routes are most efficient (fast + on-time + high volume)?
--   Which routes are worst (slow + delayed + cancellation-prone)?
--   Used by: Network planning team
--   Decision: Which routes to expand, retire, or hand to a partner airline
--
-- AVIATION CONTEXT:
--   Route efficiency = (on-time rate × traffic volume) / avg delay
--   High-efficiency routes: DEL-BOM, BOM-BLR (India's trunk routes)
--   Low-efficiency: seasonal routes with poor infrastructure
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_ROUTE_EFFICIENCY AS
WITH route_metrics AS (
    SELECT
        route_key,
        source_airport,
        dest_airport,
        is_international,

        COUNT(DISTINCT flight_id)                           AS total_flights,
        COUNT(DISTINCT event_date)                          AS days_operated,
        COUNT(DISTINCT airline_iata)                        AS airlines_on_route,

        ROUND(AVG(speed), 1)                                AS avg_speed_kmh,
        ROUND(AVG(altitude), 0)                             AS avg_altitude_ft,
        ROUND(AVG(delay_minutes), 2)                        AS avg_delay_minutes,
        ROUND(MEDIAN(delay_minutes), 2)                     AS median_delay_minutes,
        MAX(delay_minutes)                                  AS worst_delay_minutes,

        ROUND(
            SUM(IFF(delay_bucket = 'on_time', 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        2)                                                  AS otp_rate_pct,

        SUM(IFF(status = 'cancelled', 1, 0))               AS cancellations,
        ROUND(
            SUM(IFF(status = 'cancelled', 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        2)                                                  AS cancellation_rate_pct

    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE data_quality_flag = FALSE
      AND route_key NOT LIKE '%???%'              -- Exclude unknown airports
      AND source_airport != dest_airport          -- Exclude self-loops
      AND event_date >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY route_key, source_airport, dest_airport, is_international
    HAVING COUNT(DISTINCT flight_id) >= 3         -- Minimum 3 flights to rank
)

SELECT
    RANK() OVER (ORDER BY otp_rate_pct DESC, avg_delay_minutes ASC) AS efficiency_rank,
    route_key,
    source_airport,
    dest_airport,
    is_international,
    total_flights,
    days_operated,
    airlines_on_route,
    avg_speed_kmh,
    avg_altitude_ft,
    avg_delay_minutes,
    median_delay_minutes,
    worst_delay_minutes,
    otp_rate_pct,
    cancellations,
    cancellation_rate_pct,

    -- Efficiency score: penalises delays and cancellations, rewards OTP and speed
    ROUND(
        (otp_rate_pct * 0.5)
        + (LEAST(avg_speed_kmh / 10, 100) * 0.3)
        + ((100 - cancellation_rate_pct) * 0.2),
    2)                                                      AS efficiency_score,

    -- Route health label for dashboard colour-coding
    CASE
        WHEN otp_rate_pct >= 90 AND cancellation_rate_pct < 2  THEN 'EXCELLENT'
        WHEN otp_rate_pct >= 75 AND cancellation_rate_pct < 5  THEN 'GOOD'
        WHEN otp_rate_pct >= 60                                 THEN 'AVERAGE'
        WHEN otp_rate_pct >= 40                                 THEN 'POOR'
        ELSE                                                         'CRITICAL'
    END                                                     AS route_health

FROM route_metrics
ORDER BY efficiency_score DESC;

-- Best and worst routes at a glance
(SELECT 'BEST'  AS category, route_key, otp_rate_pct, avg_delay_minutes, route_health
 FROM FLIGHT_DB.ANALYTICS.V_ROUTE_EFFICIENCY ORDER BY efficiency_score DESC LIMIT 5)
UNION ALL
(SELECT 'WORST', route_key, otp_rate_pct, avg_delay_minutes, route_health
 FROM FLIGHT_DB.ANALYTICS.V_ROUTE_EFFICIENCY ORDER BY efficiency_score ASC  LIMIT 5);


-- ================================================================
-- QUERY 7: FLEET / AIRCRAFT TYPE ANALYSIS
-- ================================================================
-- BUSINESS QUESTION:
--   Which aircraft types perform best? Fewest delays? Fastest?
--   Used by: Fleet planning, MRO (Maintenance, Repair & Overhaul)
--   Decision: New aircraft orders, retirement schedule, lease returns
--
-- AVIATION CONTEXT:
--   A Boeing 737 MAX may show higher delays due to software reliability issues.
--   An Airbus A320neo may show better fuel efficiency (higher cruise speed).
--   This query gives actual in-service evidence for procurement decisions.
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_AIRCRAFT_PERFORMANCE AS
SELECT
    aircraft_type,
    COUNT(DISTINCT flight_id)                               AS flights_operated,
    COUNT(DISTINCT airline_iata)                            AS airlines_using,

    ROUND(AVG(speed), 1)                                    AS avg_cruise_speed_kmh,
    ROUND(AVG(altitude), 0)                                 AS avg_cruise_altitude_ft,
    ROUND(AVG(delay_minutes), 2)                            AS avg_delay_minutes,
    ROUND(MEDIAN(delay_minutes), 2)                         AS median_delay_minutes,

    ROUND(
        SUM(IFF(delay_bucket = 'on_time', 1, 0))::FLOAT
        / NULLIF(COUNT(*), 0) * 100,
    2)                                                      AS otp_rate_pct,

    ROUND(
        SUM(IFF(is_international, 1, 0))::FLOAT
        / NULLIF(COUNT(*), 0) * 100,
    1)                                                      AS pct_intl_routes,

    -- Reliability index: high OTP, low delay variance
    ROUND(
        otp_rate_pct - STDDEV(delay_minutes),
    2)                                                      AS reliability_index,

    RANK() OVER (ORDER BY otp_rate_pct DESC)                AS otp_rank,
    RANK() OVER (ORDER BY avg_cruise_speed_kmh DESC)        AS speed_rank

FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
WHERE data_quality_flag  = FALSE
  AND aircraft_type IS NOT NULL
  AND aircraft_type NOT IN ('UNKN', 'UNKNOWN', '')
  AND flight_phase        = 'cruise'           -- Only cruise phase for fair comparison
  AND event_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY aircraft_type
HAVING COUNT(DISTINCT flight_id) >= 3
ORDER BY otp_rate_pct DESC;

SELECT * FROM FLIGHT_DB.ANALYTICS.V_AIRCRAFT_PERFORMANCE;


-- ================================================================
-- QUERY 8: WEEK-OVER-WEEK TREND COMPARISON
-- ================================================================
-- BUSINESS QUESTION:
--   Is performance improving or deteriorating vs last week?
--   Used by: Operations director, Executive weekly review
--   Decision: Escalate to leadership? Trigger operational review?
--
-- KEY SQL:
--   LAG(value, 7): value from 7 days ago (same day last week)
--   This is the "year-over-year" pattern but for weekly comparison
--   Change pct = (this_week - last_week) / last_week * 100
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_WEEKLY_TRENDS AS
WITH daily_kpis AS (
    SELECT
        event_date,
        COUNT(DISTINCT flight_id)                           AS daily_flights,
        ROUND(AVG(delay_minutes), 2)                        AS avg_delay,
        ROUND(
            SUM(IFF(delay_bucket = 'on_time', 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        2)                                                  AS otp_rate_pct,
        SUM(IFF(status = 'cancelled', 1, 0))               AS cancellations,
        ROUND(AVG(speed), 1)                                AS avg_speed,
        SUM(IFF(is_international, 1, 0))                   AS intl_flights,
        COUNT(DISTINCT airline_iata)                        AS active_airlines
    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE data_quality_flag = FALSE
      AND event_date >= DATEADD(day, -60, CURRENT_DATE())
    GROUP BY event_date
)

SELECT
    event_date,
    daily_flights,
    avg_delay,
    otp_rate_pct,
    cancellations,
    avg_speed,
    intl_flights,
    active_airlines,

    -- Same metrics from 7 days ago
    LAG(daily_flights,  7) OVER (ORDER BY event_date)      AS flights_last_week,
    LAG(avg_delay,      7) OVER (ORDER BY event_date)      AS avg_delay_last_week,
    LAG(otp_rate_pct,   7) OVER (ORDER BY event_date)      AS otp_last_week,
    LAG(cancellations,  7) OVER (ORDER BY event_date)      AS cancellations_last_week,

    -- Week-over-week change percentages
    ROUND(
        (daily_flights - LAG(daily_flights, 7) OVER (ORDER BY event_date))::FLOAT
        / NULLIF(LAG(daily_flights, 7) OVER (ORDER BY event_date), 0) * 100,
    2)                                                      AS flights_wow_pct,

    ROUND(
        (avg_delay - LAG(avg_delay, 7) OVER (ORDER BY event_date))::FLOAT
        / NULLIF(LAG(avg_delay, 7) OVER (ORDER BY event_date), 0) * 100,
    2)                                                      AS delay_wow_pct,

    ROUND(
        otp_rate_pct - LAG(otp_rate_pct, 7) OVER (ORDER BY event_date),
    2)                                                      AS otp_pp_change,   -- Percentage points

    -- 7-day rolling average for smoothed trend line
    ROUND(AVG(otp_rate_pct) OVER (
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                   AS otp_7day_rolling_avg,

    -- Trend direction signal for dashboard arrow indicators
    CASE
        WHEN otp_rate_pct > LAG(otp_rate_pct, 7) OVER (ORDER BY event_date) + 2
        THEN 'IMPROVING'
        WHEN otp_rate_pct < LAG(otp_rate_pct, 7) OVER (ORDER BY event_date) - 2
        THEN 'DEGRADING'
        ELSE 'STABLE'
    END                                                     AS otp_trend

FROM daily_kpis
ORDER BY event_date DESC;

-- Last 14 days with trend
SELECT
    event_date,
    daily_flights,
    otp_rate_pct,
    otp_last_week,
    otp_pp_change,
    otp_trend,
    avg_delay,
    delay_wow_pct
FROM FLIGHT_DB.ANALYTICS.V_WEEKLY_TRENDS
WHERE event_date >= DATEADD(day, -14, CURRENT_DATE())
ORDER BY event_date DESC;


-- ================================================================
-- QUERY 9: INTERNATIONAL VS DOMESTIC SPLIT
-- ================================================================
-- BUSINESS QUESTION:
--   What percentage of traffic is international vs domestic?
--   How do their delay profiles compare?
--   Used by: Revenue management, regulatory compliance
--   Decision: Route expansion, UDAN scheme compliance (India domestic policy)
--
-- AVIATION CONTEXT:
--   UDAN (Ude Desh ka Aam Nagrik) scheme mandates airlines fly
--   regional domestic routes at capped fares. This query tracks
--   compliance and domestic route health.
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_INTL_VS_DOMESTIC AS
WITH segment_stats AS (
    SELECT
        IFF(is_international, 'International', 'Domestic')  AS segment,
        event_date,

        COUNT(DISTINCT flight_id)                           AS flights,
        COUNT(DISTINCT route_key)                           AS unique_routes,
        COUNT(DISTINCT airline_iata)                        AS airlines,

        ROUND(AVG(delay_minutes), 2)                        AS avg_delay_minutes,
        ROUND(AVG(speed), 1)                                AS avg_speed_kmh,
        ROUND(AVG(altitude), 0)                             AS avg_altitude_ft,

        ROUND(
            SUM(IFF(delay_bucket = 'on_time', 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        2)                                                  AS otp_rate_pct,

        SUM(IFF(delay_bucket = 'severe_delay', 1, 0))      AS severe_delays,
        SUM(IFF(status = 'cancelled', 1, 0))               AS cancellations

    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE data_quality_flag = FALSE
      AND event_date >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY segment, event_date
)

SELECT
    segment,
    COUNT(DISTINCT event_date)                              AS days_of_data,
    SUM(flights)                                            AS total_flights,

    -- Traffic share: what % of all flights is this segment?
    ROUND(
        SUM(flights)::FLOAT
        / SUM(SUM(flights)) OVER () * 100,
    2)                                                      AS traffic_share_pct,

    MAX(unique_routes)                                      AS unique_routes,
    MAX(airlines)                                           AS airlines,

    ROUND(AVG(avg_delay_minutes), 2)                        AS avg_delay_minutes,
    ROUND(AVG(avg_speed_kmh), 1)                            AS avg_speed_kmh,
    ROUND(AVG(avg_altitude_ft), 0)                          AS avg_altitude_ft,
    ROUND(AVG(otp_rate_pct), 2)                             AS avg_otp_rate_pct,
    SUM(severe_delays)                                      AS total_severe_delays,
    SUM(cancellations)                                      AS total_cancellations,

    ROUND(
        SUM(cancellations)::FLOAT / NULLIF(SUM(flights), 0) * 100,
    2)                                                      AS cancellation_rate_pct

FROM segment_stats
GROUP BY segment
ORDER BY total_flights DESC;

SELECT * FROM FLIGHT_DB.ANALYTICS.V_INTL_VS_DOMESTIC;


-- ================================================================
-- QUERY 10: DATA PIPELINE HEALTH SCORECARD
-- ================================================================
-- BUSINESS QUESTION:
--   Is the data pipeline healthy? Are we getting fresh, complete data?
--   Used by: Data engineering team, SLA monitoring
--   Decision: Page on-call engineer? Trigger pipeline retry?
--
-- WHY THIS BELONGS IN ANALYTICS:
--   Data teams are responsible for DATA SLAs, not just system uptime.
--   If the pipeline runs but delivers bad data, that is a failure.
--   This query is shown on the DE team's internal dashboard
--   alongside business metrics — because data quality IS a business metric.
-- ================================================================

CREATE OR REPLACE VIEW FLIGHT_DB.ANALYTICS.V_PIPELINE_HEALTH AS
WITH layer_stats AS (
    -- How much data does each layer have for today?
    SELECT 'RAW'   AS layer, COUNT(*) AS records, MAX(loaded_at)      AS latest_record
    FROM FLIGHT_DB.RAW.FLIGHTS_RAW
    WHERE DATE(loaded_at) = CURRENT_DATE()

    UNION ALL

    SELECT 'CLEAN', COUNT(*), MAX(transformed_at)
    FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
    WHERE DATE(transformed_at) = CURRENT_DATE()

    UNION ALL

    SELECT 'ANALYTICS', COUNT(*), MAX(transformed_at)
    FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
    WHERE event_date = CURRENT_DATE()
),

freshness AS (
    SELECT
        layer,
        records,
        latest_record,
        DATEDIFF(minute, latest_record, CURRENT_TIMESTAMP()) AS minutes_since_last_record,
        CASE
            WHEN DATEDIFF(minute, latest_record, CURRENT_TIMESTAMP()) <= 10  THEN 'FRESH'
            WHEN DATEDIFF(minute, latest_record, CURRENT_TIMESTAMP()) <= 30  THEN 'ACCEPTABLE'
            WHEN DATEDIFF(minute, latest_record, CURRENT_TIMESTAMP()) <= 60  THEN 'STALE'
            ELSE                                                                   'CRITICAL'
        END                                                 AS freshness_status
    FROM layer_stats
),

quality_score AS (
    SELECT
        ROUND(
            SUM(IFF(data_quality_flag = FALSE, 1, 0))::FLOAT
            / NULLIF(COUNT(*), 0) * 100,
        2)                                                  AS clean_record_pct,
        COUNT(*)                                            AS total_clean_records,
        SUM(IFF(data_quality_flag = TRUE, 1, 0))           AS flagged_records
    FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN
    WHERE DATE(transformed_at) = CURRENT_DATE()
),

watermark_status AS (
    SELECT
        process_name,
        last_processed_at,
        last_run_count,
        updated_at,
        DATEDIFF(minute, updated_at, CURRENT_TIMESTAMP())  AS minutes_since_update
    FROM FLIGHT_DB.RAW.PIPELINE_WATERMARKS
)

-- Final scorecard
SELECT
    CURRENT_TIMESTAMP()                                     AS report_generated_at,

    -- Layer record counts and freshness
    MAX(IFF(f.layer='RAW',       f.records,     0))        AS raw_records_today,
    MAX(IFF(f.layer='CLEAN',     f.records,     0))        AS clean_records_today,
    MAX(IFF(f.layer='ANALYTICS', f.records,     0))        AS analytics_records_today,

    MAX(IFF(f.layer='RAW',       f.freshness_status, '')) AS raw_freshness,
    MAX(IFF(f.layer='CLEAN',     f.freshness_status, '')) AS clean_freshness,
    MAX(IFF(f.layer='ANALYTICS', f.freshness_status, '')) AS analytics_freshness,

    MAX(IFF(f.layer='RAW',       f.minutes_since_last_record, 0)) AS raw_lag_minutes,
    MAX(IFF(f.layer='CLEAN',     f.minutes_since_last_record, 0)) AS clean_lag_minutes,

    -- Quality metrics
    (SELECT clean_record_pct  FROM quality_score)           AS clean_record_pct,
    (SELECT flagged_records    FROM quality_score)           AS flagged_records_today,

    -- Throughput ratio: what % of RAW made it to ANALYTICS?
    ROUND(
        MAX(IFF(f.layer='ANALYTICS', f.records, 0))::FLOAT
        / NULLIF(MAX(IFF(f.layer='RAW', f.records, 0)), 0) * 100,
    2)                                                      AS raw_to_analytics_pct,

    -- Overall health: HEALTHY only if all layers are fresh and quality is high
    CASE
        WHEN MAX(IFF(f.layer='RAW', f.freshness_status, 'CRITICAL')) = 'CRITICAL'
          OR MAX(IFF(f.layer='RAW', f.freshness_status, 'CRITICAL')) = 'STALE'
        THEN 'PIPELINE_DEGRADED'
        WHEN (SELECT clean_record_pct FROM quality_score) < 90
        THEN 'QUALITY_DEGRADED'
        ELSE 'HEALTHY'
    END                                                     AS overall_health

FROM freshness f;

SELECT * FROM FLIGHT_DB.ANALYTICS.V_PIPELINE_HEALTH;