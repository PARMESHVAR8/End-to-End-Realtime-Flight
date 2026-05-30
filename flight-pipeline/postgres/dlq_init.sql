-- postgres/dlq_init.sql (run inside PostgreSQL)
\c airflow;

CREATE TABLE IF NOT EXISTS staging.dead_letter_queue (
    id            SERIAL PRIMARY KEY,
    event_id      VARCHAR(100),
    flight_id     VARCHAR(50),
    source        VARCHAR(100),
    dag_id        VARCHAR(100),
    run_id        VARCHAR(200),
    error_message TEXT,
    retry_count   INTEGER   DEFAULT 0,
    raw_payload   JSONB,
    created_at    TIMESTAMP DEFAULT NOW(),
    status        VARCHAR(30) DEFAULT 'pending'
    -- status values: pending / replayed / replay_failed / resolved
);

CREATE INDEX idx_dlq_status     ON staging.dead_letter_queue(status);
CREATE INDEX idx_dlq_created_at ON staging.dead_letter_queue(created_at);
CREATE INDEX idx_dlq_flight_id  ON staging.dead_letter_queue(flight_id);