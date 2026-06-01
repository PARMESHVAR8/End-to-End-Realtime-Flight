# flight_kafka/producer.py
"""
Kafka Flight Event Producer
============================
Streams flight events from simulator/API into Kafka topic: flights_raw
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from dotenv import load_dotenv

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.flight_simulator import FlightSimulator
from monitoring.logging_config import setup_logging

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

# ── Config from .env ──────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "flights_raw")


class KafkaFlightProducer:
    """Wraps KafkaProducer to stream flight events."""

    def __init__(self, max_retries: int = 5):
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topic             = KAFKA_TOPIC_RAW
        self.producer          = None
        self.messages_sent     = 0
        self.messages_failed   = 0
        self._connect(max_retries)

    def _connect(self, max_retries: int) -> None:
        """Connect to Kafka with exponential backoff."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "Connecting to Kafka at %s (attempt %d/%d)...",
                    self.bootstrap_servers, attempt, max_retries
                )
                self.producer = KafkaProducer(
                    bootstrap_servers  = self.bootstrap_servers,
                    value_serializer   = lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer     = lambda k: k.encode("utf-8") if k else None,
                    acks               = "all",
                    retries            = 3,
                    linger_ms          = 10,
                    request_timeout_ms = 30000,
                )
                logger.info("Connected to Kafka successfully")
                return
            except (KafkaError, NoBrokersAvailable) as e:
                logger.warning("Kafka connection failed on attempt %d: %s", attempt, e)
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.info("Waiting %d seconds before retry...", wait_time)
                    time.sleep(wait_time)
                else:
                    logger.error("Failed to connect to Kafka after %d attempts", max_retries)
                    raise

    def send_event(self, event: dict) -> bool:
        """Send one flight event to Kafka. Returns True on success."""
        if not self.producer:
            logger.error("Producer not connected")
            return False
        try:
            flight_id = str(event.get("flight_id", "unknown"))
            self.producer.send(
                topic = self.topic,
                key   = flight_id,
                value = event,
            )
            self.messages_sent += 1
            return True
        except Exception as e:
            logger.error("Failed to send event: %s", e)
            self.messages_failed += 1
            return False

    def send_batch(self, events: list) -> dict:
        """Send a list of events then flush to Kafka."""
        success_count = 0
        failed_count  = 0

        for event in events:
            if self.send_event(event):
                success_count += 1
            else:
                failed_count += 1

        # Flush — blocks until Kafka confirms receipt of all messages
        try:
            self.producer.flush(timeout=30)
        except Exception as e:
            logger.error("Flush error: %s", e)

        return {"success": success_count, "failed": failed_count}

    def get_stats(self) -> dict:
        return {
            "messages_sent"  : self.messages_sent,
            "messages_failed": self.messages_failed,
            "topic"          : self.topic,
        }

    def close(self) -> None:
        """Flush remaining messages then close the connection."""
        if self.producer:
            try:
                self.producer.flush(timeout=30)
                self.producer.close()
            except Exception as e:
                logger.warning("Error closing producer: %s", e)
        logger.info(
            "Producer closed | sent=%d | failed=%d",
            self.messages_sent, self.messages_failed
        )


# ── Main streaming function ───────────────────────────────────────────────────

def run_streaming_producer(
    source           : str   = "simulator",
    interval_seconds : float = 5.0,
    batch_size       : int   = 10,
    inject_errors    : bool  = False,
) -> None:
    """
    Main loop: generate flight events and stream to Kafka.
    Runs until Ctrl+C or SIGTERM.
    """
    logger.info(
        "Starting producer | source=%s | interval=%.1fs | batch_size=%d",
        source, interval_seconds, batch_size
    )

    # ── Initialise data source ────────────────────────────────────────────────
    if source == "simulator":
        data_source    = FlightSimulator(inject_errors=inject_errors)
        use_simulator  = True
        logger.info("Data source: FlightSimulator")
    else:
        try:
            from ingestion.api_client import AviationStackClient
            data_source   = AviationStackClient()
            use_simulator = False
            logger.info("Data source: AviationStack API")
        except Exception as e:
            logger.warning("API client failed (%s) — falling back to simulator", e)
            data_source   = FlightSimulator(inject_errors=inject_errors)
            use_simulator = True

    # ── Initialise Kafka producer ─────────────────────────────────────────────
    kafka_producer = KafkaFlightProducer()

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    shutdown_requested = False

    def handle_shutdown(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown signal received — finishing current batch...")
        shutdown_requested = True

    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # ── Main loop ─────────────────────────────────────────────────────────────
    batch_number = 0
    total_events = 0

    try:
        while not shutdown_requested:
            batch_number += 1
            batch_start  = time.time()

            # Generate flight events
            try:
                if use_simulator:
                    # FlightSimulator.generate_batch(n) returns a list of dicts
                    events = data_source.generate_batch(batch_size)
                else:
                    events = data_source.get_live_flights(
                        limit          = batch_size,
                        flight_status  = "active",
                    )
                    # Fall back to simulator if API returns nothing
                    if not events:
                        logger.warning("API returned 0 events — using simulator fallback")
                        events = FlightSimulator().generate_batch(batch_size)

            except Exception as e:
                logger.error("Error generating events: %s", e)
                time.sleep(interval_seconds)
                continue

            # Send batch to Kafka
            result       = kafka_producer.send_batch(events)
            total_events += result["success"]
            elapsed      = time.time() - batch_start

            logger.info(
                "Batch %04d | sent=%d | failed=%d | elapsed=%.2fs | total=%d",
                batch_number,
                result["success"],
                result["failed"],
                elapsed,
                total_events,
            )

            # Wait for next interval
            sleep_for = max(0.0, interval_seconds - elapsed)
            if sleep_for > 0 and not shutdown_requested:
                time.sleep(sleep_for)

    finally:
        kafka_producer.close()
        logger.info("Producer stopped | total_events=%d", total_events)


# ── CLI entry point ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Kafka Flight Event Producer")
    parser.add_argument(
        "--source",
        choices = ["simulator", "api"],
        default = "simulator",
        help    = "Data source: simulator (default) or api"
    )
    parser.add_argument(
        "--interval",
        type    = float,
        default = 5.0,
        help    = "Seconds between batches (default: 5.0)"
    )
    parser.add_argument(
        "--batch-size",
        "--batch_size",
        type    = int,
        default = 10,
        dest    = "batch_size",
        help    = "Events per batch (default: 10)"
    )
    parser.add_argument(
        "--inject-errors",
        action  = "store_true",
        dest    = "inject_errors",
        help    = "Inject ~5%% bad records to test validation"
    )
    args = parser.parse_args()

    run_streaming_producer(
        source           = args.source,
        interval_seconds = args.interval,
        batch_size       = args.batch_size,
        inject_errors    = args.inject_errors,
    )


if __name__ == "__main__":
    main()