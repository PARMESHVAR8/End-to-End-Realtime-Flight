-- postgres/init.sql
-- This runs automatically the FIRST TIME PostgreSQL starts
-- It creates a second database for our staging data
-- (Airflow uses the default 'airflow' database; 
--  we use 'flight_staging' for our pipeline data)

CREATE DATABASE flight_staging;

-- Connect to the new database and create schema
\c flight_staging;

CREATE SCHEMA IF NOT EXISTS staging;

-- Staging table for raw flight data before it goes to Snowflake
CREATE TABLE IF NOT EXISTS staging.flights_raw (
    id              SERIAL PRIMARY KEY,
    flight_id       VARCHAR(50),
    airline         VARCHAR(100),
    flight_number   VARCHAR(20),
    source_airport  VARCHAR(10),
    dest_airport    VARCHAR(10),
    altitude        INTEGER,
    speed           FLOAT,
    latitude        FLOAT,
    longitude       FLOAT,
    status          VARCHAR(30),
    raw_payload     JSONB,              -- Store the complete original JSON
    ingested_at     TIMESTAMP DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE
);

-- Index for fast lookups by flight_id and processing status
CREATE INDEX idx_flights_raw_flight_id ON staging.flights_raw(flight_id);
CREATE INDEX idx_flights_raw_processed ON staging.flights_raw(processed);
CREATE INDEX idx_flights_raw_ingested_at ON staging.flights_raw(ingested_at);

-- Log table for pipeline run history
CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(100),
    dag_id          VARCHAR(100),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    records_read    INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    status          VARCHAR(20),        -- 'success', 'failed', 'running'
    error_message   TEXT
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO airflow;