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
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5433"))
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
										host=host,
										port=POSTGRES_PORT,
										user=POSTGRES_USER,
										password=POSTGRES_PASSWORD,
										database=POSTGRES_DB,
										connect_timeout=5
								)
								logger.info(f"Connected to PostgreSQL on attempt {attempt}")
								return
						except psycopg2.OperationalError as e:
								logger.warning(f"PostgreSQL connection attempt {attempt} failed: {e}")
								if attempt < 5:
										time.sleep(2 ** attempt)  # Exponential backoff
								else:
										logger.error("Failed to connect to PostgreSQL after 5 attempts")
										raise

		def _connect_kafka(self) -> None:
				"""Initialize Kafka consumer with proper error handling."""
				try:
						self.consumer = KafkaConsumer(
								KAFKA_TOPIC_RAW,
								bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
								group_id=KAFKA_GROUP_ID,
								value_deserializer=lambda x: json.loads(x.decode('utf-8')),
								auto_offset_reset='earliest',
								enable_auto_commit=False,
								session_timeout_ms=30000,
								heartbeat_interval_ms=10000,
								max_poll_records=100,
						)
						logger.info(f"Connected to Kafka topic: {KAFKA_TOPIC_RAW}")
				except KafkaError as e:
						logger.error(f"Failed to connect to Kafka: {e}")
						raise

		def run(self) -> None:
				"""Main consumer loop: read from Kafka, batch insert to Postgres."""
				last_flush = time.time()
				try:
						for message in self.consumer:
								try:
										record = message.value
										self.buffer.append(record)
										
										# Check if we should flush: either batch size or timeout
										should_flush = (
												len(self.buffer) >= BATCH_SIZE_MESSAGES or
												time.time() - last_flush >= BATCH_TIMEOUT_SECS
										)
										
										if should_flush:
												self._flush_batch()
												last_flush = time.time()
												
								except json.JSONDecodeError as e:
										logger.error(f"Failed to decode message: {e}")
										self.records_failed += 1
										
				except KeyboardInterrupt:
						logger.info("Consumer interrupted by user")
						self._flush_batch()
				finally:
						self.shutdown()

		def _flush_batch(self) -> None:
				"""Write buffered records to PostgreSQL in one transaction."""
				if not self.buffer:
						return
				
				try:
						cursor = self.pg_conn.cursor()
						insert_sql = """
								INSERT INTO staging.flights_raw (event_json, received_at)
								VALUES (%s, %s)
						"""
						
						batch_data = [
								(json.dumps(record), datetime.now(timezone.utc))
								for record in self.buffer
						]
						
						psycopg2.extras.execute_batch(cursor, insert_sql, batch_data, page_size=1000)
						self.pg_conn.commit()
						
						self.records_written += len(self.buffer)
						logger.info(f"Flushed {len(self.buffer)} records. Total written: {self.records_written}")
						self.buffer = []
						
				except psycopg2.Error as e:
						logger.error(f"Database error during flush: {e}")
						self.pg_conn.rollback()
						self.records_failed += len(self.buffer)

		def shutdown(self) -> None:
				"""Gracefully shutdown consumer and connections."""
				if self.consumer:
						self.consumer.close()
				if self.pg_conn:
						self.pg_conn.close()
				logger.info(f"Consumer shut down. Records: {self.records_written} written, {self.records_failed} failed")


if __name__ == "__main__":
		consumer = KafkaFlightConsumer()
		consumer.run()