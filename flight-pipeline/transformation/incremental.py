# transformation/incremental.py
"""
Incremental Load Manager
=========================
Manages watermarks so each pipeline run only processes new data.

HOW IT WORKS:
  1. Read current watermark from Snowflake control table
  2. Query source for records newer than watermark
  3. Process those records
  4. On SUCCESS: update watermark to max timestamp of processed batch
  5. On FAILURE: do NOT update watermark → safe retry next run

IDEMPOTENCY:
  If the same batch is processed twice (e.g., retry after failure):
  - The transformer handles duplicates via deduplication
  - The MERGE statement handles duplicates via event_id uniqueness
  - Result: exactly the same final state regardless of how many times run
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from snowflake.connection import SnowflakeConnection

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Reads and writes watermarks to/from Snowflake control table.
    Thread-safe: each instance manages one specific process.
    """

    WATERMARK_TABLE = "FLIGHT_DB.RAW.PIPELINE_WATERMARKS"

    def __init__(self, process_name: str):
        """
        Args:
            process_name: Unique identifier for this pipeline process.
                          Must match a row in PIPELINE_WATERMARKS.
                          e.g., 'raw_to_clean', 'clean_to_analytics'
        """
        self.process_name = process_name
        logger.info("WatermarkManager initialised | process=%s", process_name)

    def get_watermark(self, sf: SnowflakeConnection) -> datetime:
        """
        Read the current watermark (last successfully processed timestamp).

        Returns:
            datetime in UTC. If no watermark exists, returns epoch (process all data).
        """
        results = sf.execute(
            f"""
            SELECT last_processed_at, last_run_count
            FROM {self.WATERMARK_TABLE}
            WHERE process_name = %s
            """,
            params=(self.process_name,)
        )
        if not results:
            logger.warning(
                "No watermark found for process '%s'. Using epoch — will process ALL data.",
                self.process_name
            )
            return datetime(2024, 1, 1, tzinfo=timezone.utc)

        wm = results[0]["LAST_PROCESSED_AT"]
        count = results[0]["LAST_RUN_COUNT"]

        logger.info(
            "Watermark loaded | process=%s | last_processed_at=%s | last_run_count=%d",
            self.process_name, wm, count or 0
        )
        return wm

    def update_watermark(
        self,
        sf          : SnowflakeConnection,
        new_ts      : datetime,
        records_processed: int,
        run_id      : Optional[str] = None,
    ) -> None:
        """
        Update the watermark AFTER a successful run.
        This is the last thing called — only runs if all processing succeeded.

        Args:
            sf                 : Active Snowflake connection
            new_ts             : New watermark = max(event_timestamp) of processed batch
            records_processed  : How many records were processed in this run
            run_id             : Airflow run_id for traceability
        """
        sf.execute(
            f"""
            UPDATE {self.WATERMARK_TABLE}
            SET
                last_processed_at = %s,
                last_run_count    = %s,
                updated_at        = CURRENT_TIMESTAMP(),
                last_run_id       = %s
            WHERE process_name = %s
            """,
            params=(new_ts, records_processed, run_id or "manual", self.process_name)
        )
        logger.info(
            "Watermark updated | process=%s | new_watermark=%s | records=%d",
            self.process_name, new_ts, records_processed
        )

    def get_incremental_window(
        self,
        sf                  : SnowflakeConnection,
        source_table        : str,
        timestamp_column    : str = "loaded_at",
        extra_filter        : str = "",
        limit               : int = 50_000,
    ) -> pd.DataFrame:
        """
        Fetch only records newer than the current watermark.

        This is the core of incremental loading:
          SELECT * FROM source_table
          WHERE timestamp_column > (current watermark)
          AND timestamp_column <= NOW()
          AND extra_filter
          LIMIT limit

        The upper bound (NOW()) is explicit — if new records arrive
        while we're processing, they stay for the next run.
        This prevents "moving target" bugs where we keep chasing
        new data and the run never completes.

        Args:
            sf               : Active Snowflake connection
            source_table     : Fully-qualified table name
            timestamp_column : Column used as the incremental boundary
            extra_filter     : Additional WHERE conditions (e.g., "AND is_transformed=FALSE")
            limit            : Max rows per run (safety valve for huge backlogs)

        Returns:
            DataFrame of records to process in this run.
        """
        watermark = self.get_watermark(sf)
        run_upper_bound = datetime.now(timezone.utc)

        logger.info(
            "Incremental window | process=%s | from=%s | to=%s",
            self.process_name, watermark, run_upper_bound
        )

        sql = f"""
            SELECT *
            FROM {source_table}
            WHERE {timestamp_column} > %(watermark)s
            AND   {timestamp_column} <= %(upper_bound)s
            {extra_filter}
            ORDER BY {timestamp_column} ASC
            LIMIT %(limit)s
        """
        results = sf.execute(
            sql,
            params={
                "watermark"  : watermark,
                "upper_bound": run_upper_bound,
                "limit"      : limit,
            }
        )

        if not results:
            logger.info("No new records in incremental window for process=%s", self.process_name)
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df.columns = [c.lower() for c in df.columns]
        logger.info(
            "Fetched %d records for incremental processing | process=%s",
            len(df), self.process_name
        )
        return df