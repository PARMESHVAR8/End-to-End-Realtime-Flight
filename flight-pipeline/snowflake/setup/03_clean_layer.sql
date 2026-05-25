-- snowflake/setup/03_clean_layer.sql
-- ============================================================
-- CLEAN LAYER: Validated, deduplicated, enriched data.
-- Derived from RAW. Has extra columns RAW doesn't have.
-- Data analysts query this layer for ad-hoc analysis.
-- dbt models (Phase 6) will transform FROM this layer.
-- ============================================================

USE DATABASE FLIGHT_DB;
USE SCHEMA CLEAN;
USE WAREHOUSE FLIGHT_WH;

-- ─────────────────────────────────────────────────────────────
-- TABLE: FLIGHTS_CLEAN
-- One row per validated flight event.
-- All nulls imputed, all strings normalised, all types cast.
-- Extra derived columns added by our Pandas transformation.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.CLEAN.FLIGHTS_CLEAN (
    -- Keys
    clean_id                BIGINT          AUTOINCREMENT PRIMARY KEY,
    event_id                VARCHAR(100)    NOT NULL,   -- Links back to RAW

    -- Cleaned core fields
    flight_id               VARCHAR(20)     NOT NULL,
    airline                 VARCHAR(100)    NOT NULL,
    airline_iata            VARCHAR(5)      NOT NULL,
    flight_number           VARCHAR(10),

    -- Validated geography
    source_airport          VARCHAR(10)     NOT NULL,
    dest_airport            VARCHAR(10)     NOT NULL,
    source_city             VARCHAR(100),
    dest_city               VARCHAR(100),

    -- Validated & capped position
    -- NOT NULL because Pandas imputed missing values
    latitude                FLOAT           NOT NULL,
    longitude               FLOAT           NOT NULL,
    altitude                INTEGER         NOT NULL    DEFAULT 0,
    speed                   FLOAT           NOT NULL    DEFAULT 0,
    heading                 FLOAT,

    -- Normalised status (lowercased, validated against enum)
    status                  VARCHAR(30)     NOT NULL,
    departure_time          TIMESTAMP_TZ,
    arrival_time            TIMESTAMP_TZ,
    delay_minutes           INTEGER         NOT NULL    DEFAULT 0,
    aircraft_type           VARCHAR(20),
    event_timestamp         TIMESTAMP_TZ    NOT NULL,
    source                  VARCHAR(50),

    -- ── Derived columns added by transformation ──────────────────
    -- These columns DO NOT EXIST in the raw layer.
    -- They are computed by our Pandas clean_transform task.

    -- TRUE if source and destination are in different countries
    is_international        BOOLEAN         NOT NULL    DEFAULT FALSE,

    -- Categorical delay bucket for easy grouping in dashboards
    -- Values: 'on_time', 'minor_delay', 'moderate_delay',
    --         'major_delay', 'severe_delay'
    delay_bucket            VARCHAR(20)     NOT NULL    DEFAULT 'on_time',

    -- What phase of flight is this? Based on altitude
    -- Values: 'ground', 'climbing', 'mid_altitude', 'cruise'
    flight_phase            VARCHAR(20)     NOT NULL    DEFAULT 'ground',

    -- Rough geographic region for dashboard filters
    -- Values: 'India', 'Europe_Asia', 'Southern', 'Middle_East_SE_Asia'
    region                  VARCHAR(50),

    -- TRUE if this record has any remaining data anomaly
    -- These records appear in the clean layer but are flagged
    -- so analysts know to treat them carefully
    data_quality_flag       BOOLEAN         NOT NULL    DEFAULT FALSE,

    -- Pipeline metadata
    loaded_at               TIMESTAMP_TZ,               -- When it arrived in RAW
    transformed_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP()
)
-- Cluster by event_date (derived from event_timestamp) for fast date queries
-- AND airline_iata for fast per-airline queries
CLUSTER BY (DATE(event_timestamp), airline_iata)
DATA_RETENTION_TIME_IN_DAYS = 14   -- Analysts can query 14 days back in time
COMMENT = 'Clean validated flight events with derived columns. Analyst-facing.';


-- ─────────────────────────────────────────────────────────────
-- TABLE: DUPLICATE_EVENTS_LOG
-- Records we identified as duplicates and did NOT load to CLEAN.
-- Useful for debugging and auditing.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.CLEAN.DUPLICATE_EVENTS_LOG (
    log_id              BIGINT      AUTOINCREMENT PRIMARY KEY,
    event_id            VARCHAR(100),
    flight_id           VARCHAR(20),
    first_seen_at       TIMESTAMP_TZ,
    duplicate_seen_at   TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    source              VARCHAR(50)
)
COMMENT = 'Log of duplicate event_ids caught during transformation';


-- ─────────────────────────────────────────────────────────────
-- VERIFY
-- ─────────────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA FLIGHT_DB.CLEAN;

-- Quick column count check
SELECT COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'CLEAN'
AND TABLE_NAME = 'FLIGHTS_CLEAN';
-- Expected: 29 columns