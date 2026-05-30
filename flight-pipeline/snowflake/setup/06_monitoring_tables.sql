-- snowflake/setup/06_monitoring_tables.sql

USE DATABASE FLIGHT_DB;
USE WAREHOUSE FLIGHT_WH;

-- Pipeline run metrics history
CREATE TABLE IF NOT EXISTS FLIGHT_DB.RAW.PIPELINE_RUN_METRICS (
    metric_id          BIGINT        AUTOINCREMENT PRIMARY KEY,
    run_id             VARCHAR(200)  NOT NULL,
    dag_id             VARCHAR(100),
    task_id            VARCHAR(100),
    started_at         TIMESTAMP_TZ,
    finished_at        TIMESTAMP_TZ,
    records_read       INTEGER       DEFAULT 0,
    records_written    INTEGER       DEFAULT 0,
    records_failed     INTEGER       DEFAULT 0,
    records_flagged    INTEGER       DEFAULT 0,
    duplicates_removed INTEGER       DEFAULT 0,
    elapsed_seconds    FLOAT,
    throughput_rps     FLOAT,
    error_rate_pct     FLOAT,
    status             VARCHAR(20),
    error_message      VARCHAR(2000),
    created_at         TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE(started_at))
COMMENT = 'Historical metrics for every pipeline run';

-- DLQ table in PostgreSQL (run this via psql or pgAdmin)
-- docker exec -it flight_postgres psql -U airflow -d airflow