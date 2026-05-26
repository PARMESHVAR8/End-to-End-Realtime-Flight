-- snowflake/setup/05_watermark_control.sql
-- ============================================================
-- Pipeline Control Tables
-- These tables are the "memory" of your pipeline.
-- They track where each process left off so incremental
-- loads know exactly what to pick up next run.
-- ============================================================

USE DATABASE FLIGHT_DB;
USE WAREHOUSE FLIGHT_WH;

-- ─────────────────────────────────────────────────────────────
-- WATERMARK TABLE
-- One row per pipeline process.
-- Updated atomically after each successful run.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FLIGHT_DB.RAW.PIPELINE_WATERMARKS (
    process_name        VARCHAR(100)    PRIMARY KEY,
    -- The timestamp up to which this process has successfully processed data
    last_processed_at   TIMESTAMP_TZ    NOT NULL,
    -- How many records were processed in the last run
    last_run_count      INTEGER         DEFAULT 0,
    -- When this watermark was last updated
    updated_at          TIMESTAMP_TZ    DEFAULT CURRENT_TIMESTAMP(),
    -- Extra metadata for debugging
    last_run_id         VARCHAR(200),
    comment_text        VARCHAR(500)
)
COMMENT = 'Pipeline watermarks: tracks last processed timestamp per process for incremental loads';

-- Seed with initial watermarks (far in the past = process all historical data on first run)
DELETE FROM FLIGHT_DB.RAW.PIPELINE_WATERMARKS WHERE 1=1;

INSERT INTO FLIGHT_DB.RAW.PIPELINE_WATERMARKS (process_name, last_processed_at, comment_text)
VALUES
    ('raw_to_clean',        '2024-01-01 00:00:00+00', 'Initial watermark - processes all RAW data'),
    ('clean_to_analytics',  '2024-01-01 00:00:00+00', 'Initial watermark - processes all CLEAN data'),
    ('route_aggregation',   '2024-01-01 00:00:00+00', 'Initial watermark - builds all route stats'),
    ('hourly_aggregation',  '2024-01-01 00:00:00+00', 'Initial watermark - builds all hourly summaries');

-- Verify
SELECT * FROM FLIGHT_DB.RAW.PIPELINE_WATERMARKS;