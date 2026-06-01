# flight_kafka/consumer.py
"""
Kafka Flight Event Consumer
============================
Reads flight events from Kafka → writes to PostgreSQL staging.flights_raw
"""

import os
import sys
import json
import signal
import logging
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from monitoring.logging_config import setup_logging

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "flights_raw")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "flight_consumers")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_USER     = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "airflow")

# Flush buffer when this many messages accumulated OR timeout reached
BATCH_SIZE_MESSAGES = 50
BATCH_TIMEOUT_SECS  = 10


class KafkaFlightConsumer:
    """
    Reads from Kafka topic flights_raw and writes micro-batches to PostgreSQL.
    Uses manual offset commits so no message is ever lost.
    """

    def __init__(self):
        self.pg_conn          = None
        self.consumer         = None
        self.buffer           = []
        self.records_written  = 0
        self.records_failed   = 0
        self._connect_postgres()
        self._connect_kafka()

    # ── Connections ───────────────────────────────────────────────────────────

    def _connect_postgres(self) -> None:
        """Connect to PostgreSQL with retry."""
        for attempt in range(1, 6):
            try:
                self.pg_conn = psycopg2.connect(
                    host     = POSTGRES_HOST,
                    port     = POSTGRES_PORT,
                    user     = POSTGRES_USER,
                    password = POSTGRES_PASSWORD,
                    dbname   = POSTGRES_DB,
                )
                self.pg_conn.autocommit = False
                logger.info("Connected to PostgreSQL on attempt %d", attempt)
                return
            except psycopg2.OperationalError as e:
                wait = 2 ** attempt
                logger.warning(
                    "PostgreSQL connection attempt %d failed: %s", attempt, e
                )
                if attempt < 5:
                    time.sleep(wait)
        raise RuntimeError(
            f"Failed to connect to PostgreSQL after 5 attempts "
            f"({POSTGRES_HOST}:{POSTGRES_PORT})"
        )

    def _connect_kafka(self) -> None:
        """Connect to Kafka as a consumer."""
        for attempt in range(1, 6):
            try:
                self.consumer = KafkaConsumer(
                    KAFKA_TOPIC_RAW,
                    bootstrap_servers  = KAFKA_BOOTSTRAP_SERVERS,
                    group_id           = KAFKA_GROUP_ID,
                    value_deserializer = lambda v: json.loads(v.decode("utf-8")),
                    key_deserializer   = lambda k: k.decode("utf-8") if k else None,
                    auto_offset_reset  = "earliest",
                    enable_auto_commit = False,
                    consumer_timeout_ms= 1000,
                    max_poll_records   = 100,
                )
                logger.info(
                    "Connected to Kafka topic: %s | group: %s",
                    KAFKA_TOPIC_RAW, KAFKA_GROUP_ID
                )
                return
            except KafkaError as e:
                wait = 2 ** attempt
                logger.warning("Kafka connection attempt %d failed: %s", attempt, e)
                if attempt < 5:
                    time.sleep(wait)
        raise RuntimeError("Failed to connect to Kafka after 5 attempts")

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Poll Kafka continuously, buffer messages, flush to PostgreSQL."""
        logger.info(
            "Consumer starting | batch_size=%d | timeout=%ds",
            BATCH_SIZE_MESSAGES, BATCH_TIMEOUT_SECS
        )

        shutdown_requested = False

        def handle_shutdown(signum, frame):
            nonlocal shutdown_requested
            logger.info("Shutdown signal — flushing buffer...")
            shutdown_requested = True

        signal.signal(signal.SIGINT,  handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        batch_start = time.time()

        try:
            while not shutdown_requested:
                # Poll Kafka
                try:
                    for message in self.consumer:
                        if message.value:
                            self.buffer.append(message.value)

                        # Flush when batch is full
                        if len(self.buffer) >= BATCH_SIZE_MESSAGES:
                            self._flush_buffer()
                            batch_start = time.time()

                        if shutdown_requested:
                            break
                except StopIteration:
                    # consumer_timeout_ms elapsed — no new messages
                    pass

                # Flush on timeout even if buffer isn't full
                elapsed = time.time() - batch_start
                if elapsed >= BATCH_TIMEOUT_SECS and self.buffer:
                    logger.debug(
                        "Timeout flush | buffer_size=%d | elapsed=%.1fs",
                        len(self.buffer), elapsed
                    )
                    self._flush_buffer()
                    batch_start = time.time()

        finally:
            if self.buffer:
                logger.info("Final flush: %d records", len(self.buffer))
                self._flush_buffer()
            self.close()

    # ── Batch write ───────────────────────────────────────────────────────────

    def _flush_buffer(self) -> None:
        """
        Insert all buffered events into staging.flights_raw.
        Uses the exact column names from the table we created:
            flight_id, airline, flight_number, source_airport, dest_airport,
            altitude, speed, latitude, longitude, status, raw_payload, ingested_at
        """
        if not self.buffer:
            return

        records = self.buffer.copy()
        self.buffer.clear()

        try:
            cursor = self.pg_conn.cursor()

            # Build rows matching staging.flights_raw columns exactly
            rows = []
            for event in records:
                rows.append((
                    event.get("flight_id"),          # flight_id VARCHAR(50)
                    event.get("airline"),             # airline VARCHAR(100)
                    event.get("flight_number"),       # flight_number VARCHAR(20)
                    event.get("source_airport"),      # source_airport VARCHAR(10)
                    event.get("dest_airport"),        # dest_airport VARCHAR(10)
                    event.get("altitude"),            # altitude INTEGER
                    event.get("speed"),               # speed FLOAT
                    event.get("latitude"),            # latitude FLOAT
                    event.get("longitude"),           # longitude FLOAT
                    event.get("status"),              # status VARCHAR(30)
                    json.dumps(event),                # raw_payload JSONB
                    datetime.now(timezone.utc),       # ingested_at TIMESTAMP
                ))

            # Bulk insert — one network round-trip for all rows
            psycopg2.extras.execute_batch(
                cursor,
                """
                INSERT INTO staging.flights_raw (
                    flight_id, airline, flight_number,
                    source_airport, dest_airport,
                    altitude, speed, latitude, longitude,
                    status, raw_payload, ingested_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s::jsonb, %s
                )
                """,
                rows,
                page_size=100,
            )

            self.pg_conn.commit()
            cursor.close()

            self.records_written += len(records)

            # Commit Kafka offsets ONLY after successful DB write
            self.consumer.commit()

            logger.info(
                "Flushed %d records to PostgreSQL | total_written=%d",
                len(records), self.records_written
            )

        except (psycopg2.Error, Exception) as e:
            logger.error("Database error during flush: %s", e)
            try:
                self.pg_conn.rollback()
            except Exception:
                pass
            self.records_failed += len(records)
            # Do NOT commit Kafka offsets — messages will be re-delivered

    def close(self) -> None:
        """Close all connections cleanly."""
        logger.info(
            "Consumer closing | written=%d | failed=%d",
            self.records_written, self.records_failed
        )
        if self.consumer:
            try:
                self.consumer.close()
            except Exception:
                pass
        if self.pg_conn:
            try:
                self.pg_conn.close()
            except Exception:
                pass


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    consumer = KafkaFlightConsumer()
    consumer.run()