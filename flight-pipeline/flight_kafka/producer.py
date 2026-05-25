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