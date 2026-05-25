-- snowflake/setup/04_analytics_layer.sql
-- ============================================================
-- ANALYTICS LAYER: Business-ready tables.
-- Built from CLEAN layer using SQL aggregations.
-- These are what the Streamlit dashboard and BI tools query.
-- Star schema: one central FACT table, multiple DIMENSION tables.
-- ============================================================

USE DATABASE FLIGHT_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE FLIGHT_WH;

-- ─────────────────────────────────────────────────────────────
-- FACT TABLE: FACT_FLIGHTS
-- Grain: one row per flight position update event.
-- Contains measurable facts (altitude, speed, delay)
-- and foreign keys to dimension tables.
--
-- WHY A FACT TABLE?
-- In star schema design, the fact table stores MEASUREMENTS.
-- "What happened, when, and how much?"
-- Dimensions store CONTEXT: "Who? Where? Which?"
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.ANALYTICS.FACT_FLIGHTS (
    -- Surrogate key: unique per row in this fact table
    fact_id                 BIGINT          AUTOINCREMENT PRIMARY KEY,

    -- Business key from source system
    event_id                VARCHAR(100)    NOT NULL,

    -- Foreign keys → dimension tables
    -- These link to DIM_AIRLINES.airline_code
    -- and DIM_AIRPORTS.airport_code
    flight_id               VARCHAR(20)     NOT NULL,
    airline_iata            VARCHAR(5)      NOT NULL,    -- FK → DIM_AIRLINES
    source_airport          VARCHAR(10)     NOT NULL,    -- FK → DIM_AIRPORTS
    dest_airport            VARCHAR(10)     NOT NULL,    -- FK → DIM_AIRPORTS

    -- Measurable facts (the numbers analysts aggregate)
    latitude                FLOAT,
    longitude               FLOAT,
    altitude                INTEGER,
    speed                   FLOAT,
    delay_minutes           INTEGER         DEFAULT 0,

    -- Categorical attributes (low cardinality — safe to denormalise into fact)
    status                  VARCHAR(30),
    delay_bucket            VARCHAR(20),
    flight_phase            VARCHAR(20),
    is_international        BOOLEAN,
    region                  VARCHAR(50),
    data_quality_flag       BOOLEAN         DEFAULT FALSE,

    -- Date/time dimensions (extracted for fast grouping)
    -- Storing these pre-computed avoids DATE() / HOUR() function calls on every query
    event_date              DATE            NOT NULL,    -- FK → future DIM_DATE
    event_hour              INTEGER         NOT NULL,    -- 0–23
    day_of_week             INTEGER         NOT NULL,    -- 0=Sun, 6=Sat

    -- Full timestamp for precise queries
    event_timestamp         TIMESTAMP_TZ    NOT NULL,
    transformed_at          TIMESTAMP_TZ
)
-- Clustered by event_date and airline for the most common query patterns:
-- "Show me all flights on date X" and "Show me airline Y's performance"
CLUSTER BY (event_date, airline_iata)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Central fact table: one row per flight position event. Star schema grain.';


-- ─────────────────────────────────────────────────────────────
-- DIMENSION: DIM_AIRLINES
-- One row per airline. Stores descriptive attributes.
-- Updated by build_dim_tables Airflow task using MERGE.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.ANALYTICS.DIM_AIRLINES (
    airline_dim_id          BIGINT          AUTOINCREMENT PRIMARY KEY,
    airline_code            VARCHAR(5)      NOT NULL,   -- Natural key (IATA code)
    airline_name            VARCHAR(100),
    total_flights           INTEGER         DEFAULT 0,
    avg_delay_minutes       FLOAT           DEFAULT 0,
    active_flights          INTEGER         DEFAULT 0,
    last_seen_at            TIMESTAMP_TZ,
    created_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Airline dimension: one row per airline with aggregated stats';


-- ─────────────────────────────────────────────────────────────
-- DIMENSION: DIM_AIRPORTS
-- One row per airport. Stores location and traffic stats.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.ANALYTICS.DIM_AIRPORTS (
    airport_dim_id          BIGINT          AUTOINCREMENT PRIMARY KEY,
    airport_code            VARCHAR(10)     NOT NULL,   -- IATA code (natural key)
    airport_name            VARCHAR(150),
    city                    VARCHAR(100),
    country_code            VARCHAR(5),
    latitude                FLOAT,
    longitude               FLOAT,
    total_departures        INTEGER         DEFAULT 0,
    total_arrivals          INTEGER         DEFAULT 0,
    last_activity           TIMESTAMP_TZ,
    created_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Airport dimension: one row per airport with traffic volume stats';


-- ─────────────────────────────────────────────────────────────
-- AGGREGATE: HOURLY_FLIGHT_SUMMARY
-- Pre-aggregated: one row per airline per hour.
-- Dashboard charts query this — never the raw fact table.
-- Pre-aggregation = dashboards load in milliseconds not seconds.
--
-- WHY PRE-AGGREGATE?
-- A dashboard showing "flights per hour by airline" would need to
-- GROUP BY + COUNT across millions of FACT_FLIGHTS rows every refresh.
-- Instead, Airflow builds this table every 15 minutes,
-- so the dashboard queries ~1000 rows instead of 10 million.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.ANALYTICS.HOURLY_FLIGHT_SUMMARY (
    summary_id              BIGINT          AUTOINCREMENT PRIMARY KEY,
    event_date              DATE            NOT NULL,
    event_hour              INTEGER         NOT NULL,   -- 0–23
    airline_iata            VARCHAR(5)      NOT NULL,

    -- Volume metrics
    unique_flights          INTEGER         DEFAULT 0,  -- distinct flight_ids
    total_events            INTEGER         DEFAULT 0,  -- total position updates

    -- Performance metrics
    avg_altitude            FLOAT,
    avg_speed               FLOAT,
    avg_delay               FLOAT,
    max_delay               INTEGER,

    -- Status breakdown
    active_count            INTEGER         DEFAULT 0,
    landed_count            INTEGER         DEFAULT 0,
    cancelled_count         INTEGER         DEFAULT 0,

    -- Segmentation
    intl_flights            INTEGER         DEFAULT 0,
    domestic_flights        INTEGER         DEFAULT 0,

    summary_created_at      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_date)
COMMENT = 'Pre-aggregated hourly flight metrics by airline. Used by dashboard.';


-- ─────────────────────────────────────────────────────────────
-- AGGREGATE: ROUTE_PERFORMANCE
-- One row per source→destination route per day.
-- Business insight: "Which routes have the most delays?"
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.ANALYTICS.ROUTE_PERFORMANCE (
    route_id                BIGINT          AUTOINCREMENT PRIMARY KEY,
    event_date              DATE            NOT NULL,
    source_airport          VARCHAR(10)     NOT NULL,
    dest_airport            VARCHAR(10)     NOT NULL,
    route_key               VARCHAR(25)     NOT NULL,   -- 'DEL→BOM' format

    -- Traffic
    total_flights           INTEGER         DEFAULT 0,
    total_events            INTEGER         DEFAULT 0,

    -- Delay analysis
    avg_delay_minutes       FLOAT,
    max_delay_minutes       INTEGER,
    pct_on_time             FLOAT,          -- 0.0 to 1.0
    pct_delayed             FLOAT,

    -- Speed & altitude stats
    avg_speed               FLOAT,
    avg_cruise_altitude     FLOAT,

    is_international        BOOLEAN,
    summary_created_at      TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_date, source_airport)
COMMENT = 'Daily route performance metrics — delays, traffic, speed by route';


-- ─────────────────────────────────────────────────────────────
-- DATA QUALITY LOG
-- Written by flight_validation_dag every hour.
-- Tracks pipeline health over time.
-- "Are we getting worse? Were things healthier last week?"
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.ANALYTICS.DATA_QUALITY_LOG (
    log_id                  BIGINT          AUTOINCREMENT PRIMARY KEY,
    check_run_time          TIMESTAMP_TZ    NOT NULL,
    overall_status          VARCHAR(20),    -- 'HEALTHY' or 'DEGRADED'
    checks_passed           INTEGER         DEFAULT 0,
    checks_failed           INTEGER         DEFAULT 0,
    checks_warned           INTEGER         DEFAULT 0,
    created_at              TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Hourly data quality check results — pipeline health history';


-- ─────────────────────────────────────────────────────────────
-- VERIFY ALL TABLES CREATED
-- ─────────────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA FLIGHT_DB.ANALYTICS;

-- Expected output: 6 tables
-- FACT_FLIGHTS, DIM_AIRLINES, DIM_AIRPORTS,
-- HOURLY_FLIGHT_SUMMARY, ROUTE_PERFORMANCE, DATA_QUALITY_LOG