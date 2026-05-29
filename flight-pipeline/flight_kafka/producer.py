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