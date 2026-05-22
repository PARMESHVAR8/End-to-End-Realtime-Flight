# kafka/consumer.py
"""
Kafka Flight Event Consumer
============================
Reads flight events from Kafka and writes them to PostgreSQL staging.

Architecture role:
  Kafka Topic: flights_raw
       ↓
  [THIS FILE] KafkaFlightConsumer
       ↓
  PostgreSQL: staging.flights_raw   ← Airflow DAGs pick up from here

WHY WRITE TO POSTGRES FIRST (not directly to Snowflake)?
  1. PostgreSQL is local (fast writes) — Snowflake has network latency
  2. Staging allows us to inspect/debug data before it reaches the DWH
  3. Airflow can pick up batches from Postgres on a schedule
  4. If Snowflake is down, data is safe in Postgres — not lost in Kafka

CONSUMER GROUP:
  All consumers in the same group share the partitions.
  If you run 3 consumers with group_id="flight_consumers",
  each handles 1 partition — perfect horizontal scaling.
"""

import os
import sys
import json
import signal
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg2
import psycopg2.extras    # For execute_batch (bulk insert)
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from monitoring.logging_config import setup_logging

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "flights_raw")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "flight_consumers")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER     = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "airflow")

# Batch: commit to Postgres every N messages OR every M seconds
# Whichever comes first — so we don't wait too long at low traffic
BATCH_SIZE_MESSAGES = 50
BATCH_TIMEOUT_SECS  = 10


class KafkaFlightConsumer:
    """
    Reads from Kafka and writes to PostgreSQL in micro-batches.

    Micro-batching means: collect 50 messages (or 10 seconds of messages),
    then insert them all at once as a single SQL transaction.
    This is 10–50× faster than inserting one row at a time.
    """

    def __init__(self):
        self.pg_conn     = None
        self.consumer    = None
        self.buffer      = []           # In-memory batch accumulator
        self.records_written = 0
        self.records_failed  = 0
        self._connect_postgres()
        self._connect_kafka()

    # ─── Connection setup ─────────────────────────────────────────────────────

    def _connect_postgres(self) -> None:
        """Connect to PostgreSQL with retry logic."""
        for attempt in range(1, 6):
            try:
                self.pg_conn = psycopg2.connect(
                    host    = POSTGRES_HOST,
                    port    = POSTGRES_PORT,
                    user    = POSTGRES_USER,
                    password= POSTGRES_PASSWORD,
                    dbname  = POSTGRES_DB,
                )
                # autocommit=False means we control transactions manually
                # This lets us roll back a bad batch atomically
                self.pg_conn.autocommit = False
                logger.info("✓ Connected to PostgreSQL at %s:%d", POSTGRES_HOST, POSTGRES_PORT)
                return
            except psycopg2.OperationalError as e:
                wait = 2 ** attempt
                logger.warning("PostgreSQL not ready. Retrying in %ds... (%s)", wait, e)
                time.sleep(wait)

        raise RuntimeError("Cannot connect to PostgreSQL after 5 attempts")

    def _connect_kafka(self) -> None:
        """Connect to Kafka as a consumer."""
        for attempt in range(1, 6):
            try:
                self.consumer = KafkaConsumer(
                    KAFKA_TOPIC_RAW,          # Subscribe to this topic

                    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
                    group_id          = KAFKA_GROUP_ID,

                    # Deserialiser: JSON bytes → Python dict (reverse of producer)
                    value_deserializer = lambda v: json.loads(v.decode("utf-8")),
                    key_deserializer   = lambda k: k.decode("utf-8") if k else None,

                    # auto_offset_reset: what to do when this consumer group
                    # reads a topic for the FIRST TIME (no stored offset yet)
                    # "earliest" = read from the very beginning of the topic
                    # "latest"   = only read NEW messages from now on
                    auto_offset_reset = "earliest",

                    # enable_auto_commit=False: WE manually commit offsets
                    # This means: only mark a message as "processed" AFTER
                    # we've successfully written it to PostgreSQL.
                    # If we crash mid-batch, we re-process from the last commit.
                    # This is the "at-least-once" delivery guarantee.
                    enable_auto_commit = False,

                    # Don't wait more than 1 second for messages
                    # (so our timeout-based batching still works)
                    consumer_timeout_ms = 1000,

                    # Each poll() fetches up to 100 messages
                    max_poll_records = 100,
                )
                logger.info(
                    "✓ Connected to Kafka | topic=%s | group=%s",
                    KAFKA_TOPIC_RAW, KAFKA_GROUP_ID
                )
                return
            except KafkaError as e:
                wait = 2 ** attempt
                logger.warning("Kafka not ready. Retrying in %ds... (%s)", wait, e)
                time.sleep(wait)

        raise RuntimeError("Cannot connect to Kafka after 5 attempts")

    # ─── Main consume loop ────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Main polling loop.
        Polls Kafka continuously, accumulates messages in a buffer,
        and flushes the buffer to PostgreSQL when it's full or timed out.
        """
        logger.info(
            "Consumer starting | batch_size=%d | timeout=%ds",
            BATCH_SIZE_MESSAGES, BATCH_TIMEOUT_SECS
        )

        shutdown_requested = False

        def handle_shutdown(signum, frame):
            nonlocal shutdown_requested
            logger.info("Shutdown signal. Flushing buffer...")
            shutdown_requested = True

        signal.signal(signal.SIGINT,  handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        batch_start_time = time.time()

        try:
            while not shutdown_requested:
                # Poll Kafka for new messages (timeout handled inside consumer)
                try:
                    for message in self.consumer:
                        # message.value is already a dict (deserialised above)
                        self.buffer.append(message.value)

                        # Flush if batch is full
                        if len(self.buffer) >= BATCH_SIZE_MESSAGES:
                            self._flush_buffer()
                            batch_start_time = time.time()

                        if shutdown_requested:
                            break

                except StopIteration:
                    # consumer_timeout_ms elapsed with no new messages
                    pass

                # Flush if timeout elapsed (even if buffer isn't full)
                elapsed = time.time() - batch_start_time
                if elapsed >= BATCH_TIMEOUT_SECS and self.buffer:
                    logger.debug(
                        "Timeout flush triggered | buffer_size=%d | elapsed=%.1fs",
                        len(self.buffer), elapsed
                    )
                    self._flush_buffer()
                    batch_start_time = time.time()

        finally:
            # Flush anything remaining in the buffer
            if self.buffer:
                logger.info("Final flush: %d records", len(self.buffer))
                self._flush_buffer()
            self.close()

    # ─── Batch write ──────────────────────────────────────────────────────────

    def _flush_buffer(self) -> None:
        """
        Write all buffered messages to PostgreSQL in one transaction.

        Uses psycopg2.extras.execute_batch which sends multiple
        INSERT statements in a single network round-trip.
        Much faster than looping and calling execute() one by one.
        """
        if not self.buffer:
            return

        records = self.buffer.copy()
        self.buffer.clear()

        try:
            cursor = self.pg_conn.cursor()

            # Prepare rows for bulk insert
            rows = []
            for event in records:
                rows.append((
                    event.get("flight_id"),
                    event.get("airline"),
                    event.get("flight_number"),
                    event.get("source_airport"),
                    event.get("dest_airport"),
                    event.get("altitude"),
                    event.get("speed"),
                    event.get("latitude"),
                    event.get("longitude"),
                    event.get("status"),
                    json.dumps(event),          # Store full JSON as raw_payload
                    datetime.now(timezone.utc), # ingested_at
                ))

            # execute_batch: sends all rows in one SQL call
            psycopg2.extras.execute_batch(
                cursor,
                """
                INSERT INTO staging.flights_raw
                    (flight_id, airline, flight_number,
                     source_airport, dest_airport,
                     altitude, speed, latitude, longitude,
                     status, raw_payload, ingested_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                rows,
                page_size=100   # Send 100 rows per round-trip
            )

            # COMMIT: make all insertions permanent atomically
            # If ANY row failed, we ROLLBACK all — no partial writes
            self.pg_conn.commit()
            cursor.close()

            self.records_written += len(records)

            # Now tell Kafka "we've processed up to this offset"
            # ONLY commit after successful DB write
            self.consumer.commit()

            logger.info(
                "✓ Flushed %d records to PostgreSQL | total_written=%d",
                len(records), self.records_written
            )

        except (psycopg2.Error, Exception) as e:
            logger.error("Failed to write batch to PostgreSQL: %s", e)
            # Roll back the failed transaction
            self.pg_conn.rollback()
            self.records_failed += len(records)
            # NOTE: we do NOT commit Kafka offsets here
            # So these messages will be re-delivered on next poll — at-least-once

    def close(self) -> None:
        """Graceful shutdown."""
        logger.info(
            "Consumer closing | written=%d | failed=%d",
            self.records_written, self.records_failed
        )
        if self.consumer:
            self.consumer.close()
        if self.pg_conn:
            self.pg_conn.close()


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    consumer = KafkaFlightConsumer()
    consumer.run()