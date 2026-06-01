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
	python -m flight_kafka.producer --source simulator --interval 5
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

from flight_kafka import KafkaProducer
from flight_kafka.errors import KafkaError, NoBrokersAvailable
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
										value_serializer=lambda v: json.dumps(v).encode('utf-8'),
										acks='all',
										retries=3,
										compression_type='gzip',
										request_timeout_ms=30000,
										batch_size=1000,
								)
								logger.info("Connected to Kafka successfully")
								return
						except (KafkaError, NoBrokersAvailable) as e:
								logger.warning(
										"Kafka connection failed on attempt %d: %s",
										attempt, e
								)
								if attempt < max_retries:
										wait_time = 2 ** attempt
										logger.info("Waiting %d seconds before retry...", wait_time)
										time.sleep(wait_time)
								else:
									logger.error("Failed to connect to Kafka after %d attempts", max_retries)
									raise

		def send_flight_event(self, flight_data: dict, flight_id: Optional[str] = None) -> bool:
				"""
				Send a single flight event to Kafka.
				
				Args:
					flight_data: Dictionary with flight information
					flight_id: Optional flight ID to use as message key
					
				Returns:
					True if sent successfully, False otherwise
				"""
				if not self.producer:
						logger.error("Producer not connected")
						return False
				
				try:
						key = (flight_id or flight_data.get('flight_id', '')).encode('utf-8')
						future = self.producer.send(self.topic, value=flight_data, key=key)
						record_metadata = future.get(timeout=10)
						
						self.messages_sent += 1
						if self.messages_sent % 100 == 0:
								logger.info(f"Sent {self.messages_sent} messages to {record_metadata.topic}")
						return True
						
				except Exception as e:
						logger.error(f"Failed to send message: {e}")
						self.messages_failed += 1
						return False

		def run(self, source: str = "simulator", interval: int = 5) -> None:
				"""
				Main loop: continuously fetch flight data and send to Kafka.
				
				Args:
					source: 'simulator' or 'api'
					interval: Seconds between fetches
				"""
				if source == "simulator":
						flight_source = FlightSimulator()
				elif source == "api":
						flight_source = AviationStackClient()
				else:
						raise ValueError(f"Unknown source: {source}")
				
				logger.info(f"Starting producer from source: {source}, interval: {interval}s")
				
				try:
						while True:
								try:
										flights = flight_source.fetch_flights()
										for flight in flights:
												self.send_flight_event(flight)
										time.sleep(interval)
								except Exception as e:
										logger.error(f"Error during fetch cycle: {e}")
										time.sleep(interval)
										
				except KeyboardInterrupt:
						logger.info("Producer interrupted by user")
				finally:
						self.shutdown()

		def shutdown(self) -> None:
				"""Gracefully shutdown the producer."""
				if self.producer:
						self.producer.flush()
						self.producer.close()
				logger.info(
						f"Producer shut down. Sent: {self.messages_sent}, Failed: {self.messages_failed}"
				)
		# flight_kafka/producer.py
# Find the run_streaming_producer function and replace the ENTIRE function with this:

def run_streaming_producer(
    source: str = "simulator",
    interval_seconds: float = 5.0,
    batch_size: int = 10,
    inject_errors: bool = False,
) -> None:
    """
    Main streaming loop.
    Runs forever until Ctrl+C or SIGTERM.
    """
    logger.info(
        "Starting flight producer | source=%s | interval=%.1fs | batch_size=%d",
        source, interval_seconds, batch_size
    )

    # Initialise data source
    if source == "simulator":
        from ingestion.flight_simulator import FlightSimulator
        data_source = FlightSimulator(inject_errors=inject_errors)
        logger.info("Using SIMULATOR as data source")
        use_simulator = True
    else:
        from ingestion.api_client import AviationStackClient
        data_source = AviationStackClient()
        logger.info("Using AVIATIONSTACK API as data source")
        use_simulator = False

    # Initialise Kafka producer
    producer = KafkaFlightProducer()

    # Graceful shutdown handler
    shutdown_requested = False

    def handle_shutdown(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown signal received. Finishing current batch...")
        shutdown_requested = True

    import signal
    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Main loop
    batch_number  = 0
    total_events  = 0

    try:
        while not shutdown_requested:
            batch_number += 1
            start_time = time.time()

            # ── Generate events ───────────────────────────────────────────
            try:
                if use_simulator:
                    # FlightSimulator uses generate_batch()
                    events = data_source.generate_batch(batch_size)
                else:
                    # API client uses get_live_flights()
                    events = data_source.get_live_flights(
                        limit=batch_size,
                        flight_status="active"
                    )
                    if not events:
                        logger.warning("API returned 0 events. Falling back to simulator.")
                        from ingestion.flight_simulator import FlightSimulator
                        fallback = FlightSimulator()
                        events = fallback.generate_batch(batch_size)
            except Exception as e:
                logger.error("Error generating events: %s", e)
                time.sleep(interval_seconds)
                continue

            # ── Send to Kafka ─────────────────────────────────────────────
            result = producer.send_batch(events)
            total_events += result["success"]

            elapsed = time.time() - start_time
            logger.info(
                "Batch %04d | sent=%d | failed=%d | elapsed=%.2fs | total=%d",
                batch_number,
                result["success"],
                result["failed"],
                elapsed,
                total_events,
            )

            # Sleep until next interval
            sleep_time = max(0, interval_seconds - elapsed)
            if sleep_time > 0 and not shutdown_requested:
                time.sleep(sleep_time)

    finally:
        producer.close()
        logger.info("Producer stopped. Total events: %d", total_events)

	# flight_kafka/producer.py
# Inside the KafkaFlightProducer class, add this method if it doesn't exist:

    def send_batch(self, events: list) -> dict:
        """Send multiple events and flush."""
        success_count = 0
        failed_count  = 0

        for event in events:
            if self.send_event(event):
                success_count += 1
            else:
                failed_count += 1

        # Flush — block until Kafka confirms receipt
        try:
            self.producer.flush(timeout=30)
        except Exception as e:
            logger.error("Flush error: %s", e)

        return {"success": success_count, "failed": failed_count}


def main():
		parser = argparse.ArgumentParser(description="Kafka Flight Event Producer")
		parser.add_argument(
				"--source",
				choices=["simulator", "api"],
				default="simulator",
				help="Flight data source (default: simulator)"
		)
		parser.add_argument(
				"--interval",
				type=int,
				default=5,
				help="Seconds between data fetches (default: 5)"
		)
		args = parser.parse_args()
		
		producer = KafkaFlightProducer()
		producer.run(source=args.source, interval=args.interval)


if __name__ == "__main__":
		main()