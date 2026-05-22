# kafka/producer.py
"""
Kafka Flight Event Producer
============================
Continuously fetches flight data and streams it to Kafka topic: flights_raw

Architecture role:
  Flight Source (API / Simulator)
       ↓
  [THIS FILE] KafkaFlightProducer
       ↓
  Kafka Topic: flights_raw
       ↓
  Consumer (reads and processes)

WHY A PRODUCER CLASS:
  Wraps kafka-python's KafkaProducer with:
  - Auto-reconnection logic
  - Serialisation (dict → JSON bytes — Kafka only stores bytes)
  - Delivery callbacks (know if a message was accepted)
  - Graceful shutdown

USAGE:
  python -m kafka.producer --source simulator --interval 5
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from dotenv import load_dotenv

# Add project root to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.flight_simulator import FlightSimulator
from ingestion.api_client import AviationStackClient
from monitoring.logging_config import setup_logging

load_dotenv()
setup_logging()   # Configure logging for the whole application
logger = logging.getLogger(__name__)

# ─── Configuration (reads from .env) ─────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "flights_raw")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "flight_consumers")


class KafkaFlightProducer:
    """
    Wraps KafkaProducer to stream flight events.

    Key design decisions:
      - Uses flight_id as the message KEY.
        Kafka routes messages with the same key to the same partition.
        This means all updates for flight "AI101" go to partition 0 — always.
        Consumers processing AI101's events always see them in order.

      - value_serializer converts our Python dict to JSON bytes.
        Kafka is a byte transport — it doesn't understand Python objects.

      - acks='all' means the broker waits for ALL replicas to confirm
        before acknowledging. Slower but guarantees no data loss.
        In dev (1 broker) this still works fine.
    """

    def __init__(self, max_retries: int = 5):
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topic             = KAFKA_TOPIC_RAW
        self.producer          = None
        self.messages_sent     = 0
        self.messages_failed   = 0
        self._connect(max_retries)

    def _connect(self, max_retries: int) -> None:
        """
        Attempt to connect to Kafka with exponential backoff.

        WHY EXPONENTIAL BACKOFF:
          If Kafka is starting up, we don't want to hammer it with
          connection attempts every millisecond. We wait longer each
          retry: 2s, 4s, 8s, 16s, 32s — then give up.
          This is a universal pattern for connecting to external services.
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "Connecting to Kafka at %s (attempt %d/%d)...",
                    self.bootstrap_servers, attempt, max_retries
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,

                    # Serialiser: Python dict → JSON string → UTF-8 bytes
                    # Kafka stores BYTES only — this converts automatically
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

                    # Key serialiser: flight_id string → bytes
                    key_serializer=lambda k: k.encode("utf-8") if k else None,

                    # Wait for all replicas to confirm (reliability > speed)
                    acks="all",

                    # Retry failed sends up to 3 times before raising
                    retries=3,

                    # Batch messages within 10ms window for efficiency
                    # Instead of 1 network call per message, batch small groups
                    linger_ms=10,

                    # Max message size: 1MB (our JSON events are ~1KB each)
                    max_request_size=1048576,
                )
                logger.info("✓ Connected to Kafka successfully")
                return

            except NoBrokersAvailable:
                wait_time = 2 ** attempt   # 2, 4, 8, 16, 32 seconds
                logger.warning(
                    "Kafka not available. Retrying in %ds...", wait_time
                )
                time.sleep(wait_time)

        raise RuntimeError(
            f"Could not connect to Kafka at {self.bootstrap_servers} "
            f"after {max_retries} attempts. Is Docker running?"
        )

    def send_event(self, event: dict) -> bool:
        """
        Send one flight event to Kafka.

        Args:
            event: Flight event dict (matches our JSON schema)

        Returns:
            True if message was accepted, False if failed

        The send is ASYNCHRONOUS — it returns immediately.
        The on_success/on_error callbacks fire later when Kafka responds.
        This is much faster than waiting for confirmation on each message.
        """
        try:
            flight_id = event.get("flight_id", "unknown")

            # .send() is non-blocking — returns a Future
            future = self.producer.send(
                topic=self.topic,
                key=flight_id,     # Routes same flight to same partition
                value=event,       # Auto-serialised to JSON bytes
                # Optional: set timestamp explicitly
                timestamp_ms=int(datetime.now(timezone.utc).timestamp() * 1000)
            )

            # Attach callbacks — called when Kafka responds
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)

            self.messages_sent += 1
            return True

        except KafkaError as e:
            logger.error("Kafka send error for flight %s: %s", flight_id, e)
            self.messages_failed += 1
            return False

    def send_batch(self, events: list[dict]) -> dict:
        """
        Send multiple events efficiently.
        After sending all, flush() blocks until Kafka confirms receipt.
        """
        success_count = 0
        for event in events:
            if self.send_event(event):
                success_count += 1

        # Block until all pending messages are delivered (or timeout)
        self.producer.flush(timeout=30)

        logger.info(
            "Batch sent: %d success / %d failed",
            success_count, len(events) - success_count
        )
        return {"success": success_count, "failed": len(events) - success_count}

    def get_stats(self) -> dict:
        """Return running statistics for monitoring."""
        return {
            "messages_sent"  : self.messages_sent,
            "messages_failed": self.messages_failed,
            "success_rate"   : (
                self.messages_sent /
                max(self.messages_sent + self.messages_failed, 1) * 100
            ),
            "topic"          : self.topic,
        }

    def close(self) -> None:
        """Graceful shutdown — flush remaining messages then close."""
        if self.producer:
            logger.info("Flushing remaining messages before shutdown...")
            self.producer.flush(timeout=30)
            self.producer.close()
            logger.info(
                "Producer closed. Stats: %s", self.get_stats()
            )

    # ─── Callbacks ───────────────────────────────────────────────────────────

    @staticmethod
    def _on_send_success(record_metadata) -> None:
        """
        Called by Kafka when a message is successfully stored.
        record_metadata tells us exactly where the message landed.
        """
        logger.debug(
            "✓ Message stored | topic=%s | partition=%d | offset=%d",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset
        )

    @staticmethod
    def _on_send_error(exception) -> None:
        """Called by Kafka when a message could NOT be stored."""
        logger.error("✗ Message delivery failed: %s", exception)


# ─── Streaming loop ───────────────────────────────────────────────────────────

def run_streaming_producer(
    source: str = "simulator",
    interval_seconds: float = 5.0,
    batch_size: int = 10,
    inject_errors: bool = False,
) -> None:
    """
    Main streaming loop.
    Runs forever until Ctrl+C or SIGTERM.

    Args:
        source          : "simulator" or "api"
        interval_seconds: Seconds between each batch
        batch_size      : Events per batch
        inject_errors   : Inject bad data for testing validation
    """
    logger.info(
        "Starting flight producer | source=%s | interval=%.1fs | batch_size=%d",
        source, interval_seconds, batch_size
    )

    # Initialise data source
    if source == "simulator":
        data_source = FlightSimulator(inject_errors=inject_errors)
        logger.info("Using SIMULATOR as data source")
    else:
        data_source = AviationStackClient()
        logger.info("Using AVIATIONSTACK API as data source")

    # Initialise producer
    producer = KafkaFlightProducer()

    # ── Graceful shutdown handler ─────────────────────────────────────────────
    # When you press Ctrl+C or the container gets a SIGTERM signal,
    # this function runs instead of abruptly killing the process.
    # It lets in-flight messages finish before exiting.
    shutdown_requested = False

    def handle_shutdown(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown signal received. Finishing current batch...")
        shutdown_requested = True

    signal.signal(signal.SIGINT,  handle_shutdown)   # Ctrl+C
    signal.signal(signal.SIGTERM, handle_shutdown)   # Docker stop

    # ── Main loop ─────────────────────────────────────────────────────────────
    batch_number = 0
    total_events = 0

    try:
        while not shutdown_requested:
            batch_number += 1
            start_time = time.time()

            # Generate events
            if source == "simulator":
                events = data_source.generate_batch(batch_size)
            else:
                # API: fetch real flights (filter to India routes)
                events = data_source.get_live_flights(
                    limit=batch_size,
                    flight_status="active"
                )
                if not events:
                    logger.warning("API returned 0 events. Falling back to simulator.")
                    fallback = FlightSimulator()
                    events = fallback.generate_batch(batch_size)

            # Send to Kafka
            result = producer.send_batch(events)
            total_events += result["success"]

            elapsed = time.time() - start_time

            # Log a summary every batch
            logger.info(
                "Batch %04d | sent=%d | failed=%d | elapsed=%.2fs | "
                "total_events=%d | stats=%s",
                batch_number,
                result["success"],
                result["failed"],
                elapsed,
                total_events,
                producer.get_stats()
            )

            # Sleep until next interval
            sleep_time = max(0, interval_seconds - elapsed)
            if sleep_time > 0 and not shutdown_requested:
                time.sleep(sleep_time)

    finally:
        # Always runs — even if an exception occurs
        producer.close()
        logger.info(
            "Producer stopped. Total events produced: %d", total_events
        )


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kafka Flight Event Producer"
    )
    parser.add_argument(
        "--source",
        choices=["simulator", "api"],
        default="simulator",
        help="Data source: 'simulator' (default) or 'api' (AviationStack)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between batches (default: 5.0)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Events per batch (default: 10)"
    )
    parser.add_argument(
        "--inject-errors",
        action="store_true",
        help="Inject ~5%% bad records to test validation"
    )

    args = parser.parse_args()

    run_streaming_producer(
        source          = args.source,
        interval_seconds= args.interval,
        batch_size      = args.batch_size,
        inject_errors   = args.inject_errors,
    )