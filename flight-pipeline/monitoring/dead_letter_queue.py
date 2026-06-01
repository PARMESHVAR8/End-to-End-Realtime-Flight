# monitoring/dead_letter_queue.py
"""
Dead Letter Queue (DLQ)
========================
Captures messages that failed processing and stores them
for later inspection and reprocessing.

WHAT IS A DLQ:
  In message queue systems, a Dead Letter Queue is a special
  holding area for messages that could not be processed successfully
  after all retries.

WHY YOU NEED A DLQ:
  Without it: failed messages are silently discarded.
              You lose data and never know why.
  With it:    failed messages are preserved with their error.
              You can inspect, fix the root cause, and replay them.

OUR DLQ DESIGN:
  Failed Kafka message
       ↓
  DLQ (PostgreSQL table: staging.dead_letter_queue)
       ↓
  monitoring/alerts.py → Slack notification
       ↓
  Engineer investigates root cause
       ↓
  dlq.replay(message_ids) → re-injects into pipeline

USAGE:
  from monitoring.dead_letter_queue import DeadLetterQueue
  dlq = DeadLetterQueue()
  dlq.publish(message=event, error="ValueError: altitude is None",
              source="kafka_consumer", dag_id="flight_ingestion")
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """
    Stores failed messages in PostgreSQL for inspection and replay.
    """

    DLQ_TABLE = "staging.dead_letter_queue"

    def publish(
        self,
        message     : dict,
        error       : str,
        source      : str,
        dag_id      : str       = "unknown",
        run_id      : str       = "unknown",
        retry_count : int       = 0,
    ) -> bool:
        """
        Add a failed message to the DLQ.

        Args:
            message    : The original failed record (dict)
            error      : Error description (exception message + type)
            source     : Which component failed ("kafka_consumer", "transformer", etc.)
            dag_id     : Airflow DAG that was running when failure occurred
            run_id     : Airflow run_id for correlation
            retry_count: How many times this message was retried before DLQ

        Returns:
            True if successfully stored in DLQ, False if DLQ itself failed.
        """
        try:
            flight_id  = message.get("flight_id", "unknown")
            event_id   = message.get("event_id",  "unknown")

            logger.warning(
                "Message sent to DLQ",
                extra={
                    "flight_id"  : flight_id,
                    "event_id"   : event_id,
                    "source"     : source,
                    "error"      : error[:200],
                    "retry_count": retry_count,
                }
            )

            pg = self._get_pg_connection()
            cursor = pg.cursor()

            cursor.execute(
                f"""
                INSERT INTO {self.DLQ_TABLE} (
                    event_id, flight_id, source,
                    dag_id, run_id, error_message,
                    retry_count, raw_payload, created_at,
                    status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                """,
                (
                    event_id,
                    flight_id,
                    source,
                    dag_id,
                    run_id,
                    error[:2000],
                    retry_count,
                    json.dumps(message),
                    datetime.now(timezone.utc),
                    "pending",
                )
            )
            pg.commit()
            cursor.close()
            pg.close()

            # Alert if DLQ is accumulating too many messages
            self._check_dlq_volume()
            return True

        except Exception as e:
            logger.error("DLQ write failed: %s | original_error=%s", e, error)
            return False

    def get_pending_messages(self, limit: int = 100) -> list[dict]:
        """
        Fetch messages waiting for replay.
        Used by monitoring dashboard and replay scripts.
        """
        try:
            pg     = self._get_pg_connection()
            cursor = pg.cursor()
            cursor.execute(
                f"""
                SELECT id, event_id, flight_id, source,
                       dag_id, error_message, retry_count,
                       raw_payload, created_at
                FROM {self.DLQ_TABLE}
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (limit,)
            )
            cols    = [d[0] for d in cursor.description]
            rows    = cursor.fetchall()
            cursor.close()
            pg.close()
            return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            logger.error("Could not fetch DLQ messages: %s", e)
            return []

    def replay(self, message_ids: list[int]) -> dict:
        """
        Re-inject DLQ messages back into the pipeline.
        Marks them as 'replaying' so they're not picked up twice.

        Returns:
            {"replayed": N, "failed": M}
        """
        from kafka.producer import KafkaFlightProducer

        if not message_ids:
            return {"replayed": 0, "failed": 0}

        messages = self.get_pending_messages(limit=len(message_ids) * 2)
        to_replay = [m for m in messages if m["id"] in message_ids]

        producer = KafkaFlightProducer()
        replayed = 0
        failed   = 0

        for msg in to_replay:
            try:
                payload = msg["raw_payload"]
                if isinstance(payload, str):
                    payload = json.loads(payload)

                producer.send_event(payload)
                self._mark_status(msg["id"], "replayed")
                replayed += 1
                logger.info(
                    "Replayed DLQ message | id=%s | event_id=%s",
                    msg["id"], msg["event_id"]
                )
            except Exception as e:
                self._mark_status(msg["id"], "replay_failed")
                failed += 1
                logger.error("DLQ replay failed for id=%s: %s", msg["id"], e)

        producer.close()
        logger.info("DLQ replay complete | replayed=%d | failed=%d", replayed, failed)
        return {"replayed": replayed, "failed": failed}

    def get_dlq_stats(self) -> dict:
        """Return DLQ statistics for the monitoring dashboard."""
        try:
            pg     = self._get_pg_connection()
            cursor = pg.cursor()
            cursor.execute(
                f"""
                SELECT
                    status,
                    COUNT(*)               AS count,
                    MAX(created_at)        AS latest,
                    MIN(created_at)        AS oldest
                FROM {self.DLQ_TABLE}
                GROUP BY status
                """
            )
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            pg.close()
            return {
                "by_status": [dict(zip(cols, row)) for row in rows],
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logger.error("DLQ stats failed: %s", e)
            return {}

    def _mark_status(self, dlq_id: int, status: str) -> None:
        """Update status of a DLQ entry."""
        try:
            pg = self._get_pg_connection()
            cursor = pg.cursor()
            cursor.execute(
                f"UPDATE {self.DLQ_TABLE} SET status=%s WHERE id=%s",
                (status, dlq_id)
            )
            pg.commit()
            cursor.close()
            pg.close()
        except Exception as e:
            logger.warning("Could not update DLQ status: %s", e)

    def _check_dlq_volume(self) -> None:
        """Alert if DLQ has accumulated too many messages."""
        threshold = int(os.getenv("DLQ_ALERT_THRESHOLD", "50"))
        stats     = self.get_dlq_stats()
        pending   = next(
            (s["count"] for s in stats.get("by_status", [])
             if s["status"] == "pending"),
            0
        )
        if pending >= threshold:
            from monitoring.alerts import get_alert_manager
            get_alert_manager().send_error(
                f"Dead Letter Queue has {pending} pending messages",
                dlq_pending_count = pending,
                threshold         = threshold,
                action_required   = (
                    f"Run: python -m monitoring.dead_letter_queue replay "
                    f"after investigating root cause. "
                    f"Check staging.dead_letter_queue for error details."
                ),
            )

    @staticmethod
    def _get_pg_connection():
        """Get PostgreSQL connection."""
        import os, psycopg2
        return psycopg2.connect(
            host    = os.getenv("POSTGRES_HOST", "localhost"),
            port    = int(os.getenv("POSTGRES_PORT", "5433")),
            user    = os.getenv("POSTGRES_USER", "airflow"),
            password= os.getenv("POSTGRES_PASSWORD", "airflow"),
            dbname  = os.getenv("POSTGRES_DB", "airflow"),
        )


import os  # needed for _check_dlq_volume