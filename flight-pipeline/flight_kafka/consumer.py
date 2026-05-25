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

from flight_kafka import KafkaConsumer
# from flight_kafka.consumer import KafkaConsumer
from flight_kafka.errors import KafkaError
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
				# Use 127.0.0.1 explicitly to avoid IPv6 issues with "localhost"
				host = POSTGRES_HOST.replace("localhost", "127.0.0.1") if POSTGRES_HOST == "localhost" else POSTGRES_HOST
        
				for attempt in range(1, 6):
						try:
								self.pg_conn = psycopg2.connect(
										host    = host,
										port    = POSTGRES_PORT,