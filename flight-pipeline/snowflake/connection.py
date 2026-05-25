# snowflake/connection.py
"""
Snowflake Connection Manager
=============================
Centralised connection factory for the flight pipeline.
Used by Airflow DAG tasks and standalone scripts.

WHY A CONTEXT MANAGER:
  Using 'with SnowflakeConnection() as conn:' guarantees the connection
  is ALWAYS closed after the block — even if an exception occurs.
  Without this, failed scripts leave orphan connections open,
  wasting your Snowflake credits (connections keep the warehouse active).

USAGE:
  # Option 1: Context manager (recommended)
  with SnowflakeConnection() as sf:
      results = sf.execute("SELECT COUNT(*) FROM FLIGHT_DB.RAW.FLIGHTS_RAW")

  # Option 2: Direct use
  sf = SnowflakeConnection()
  sf.connect()
  results = sf.execute(sql)
  sf.close()
"""

import os
import logging
from typing import Optional, Any
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class SnowflakeConnection:
    """
    Manages a Snowflake connection with auto-reconnect and context manager support.
    """

    def __init__(
        self,
        database  : Optional[str] = None,
        schema    : Optional[str] = None,
        warehouse : Optional[str] = None,
        role      : Optional[str] = None,
    ):
        """
        All params default to .env values but can be overridden per-instance.
        This lets you do: SnowflakeConnection(schema="ANALYTICS") to target
        a specific schema without changing the .env file.
        """
        self.account   = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user      = os.getenv("SNOWFLAKE_USER")
        self.password  = os.getenv("SNOWFLAKE_PASSWORD")
        self.database  = database  or os.getenv("SNOWFLAKE_DATABASE", "FLIGHT_DB")
        self.schema    = schema    or os.getenv("SNOWFLAKE_SCHEMA", "RAW")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "FLIGHT_WH")
        self.role      = role      or os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        self._conn     = None

        if not all([self.account, self.user, self.password]):
            raise ValueError(
                "Missing Snowflake credentials. "
                "Check SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD in .env"
            )

    def connect(self) -> "SnowflakeConnection":
        """Establish the connection. Returns self for chaining."""
        try:
            logger.info(
                "Connecting to Snowflake | account=%s | db=%s | schema=%s",
                self.account, self.database, self.schema
            )
            self._conn = snowflake.connector.connect(
                account          = self.account,
                user             = self.user,
                password         = self.password,
                database         = self.database,
                schema           = self.schema,
                warehouse        = self.warehouse,
                role             = self.role,
                # Network timeout: don't hang forever if Snowflake is unreachable
                network_timeout  = 30,
                login_timeout    = 15,
                # Session params: set these once on connect
                session_parameters = {
                    "QUERY_TAG"        : "flight_pipeline",  # Shows in query history
                    "TIMEZONE"         : "UTC",
                    "WEEK_START"       : 1,                  # Monday = start of week
                }
            )
            logger.info("✓ Snowflake connection established")
            return self
        except snowflake.connector.errors.DatabaseError as e:
            logger.error("Snowflake connection failed: %s", e)
            raise

    def execute(
        self,
        sql       : str,
        params    : Optional[tuple] = None,
        as_dict   : bool = True,
    ) -> list[dict]:
        """
        Execute a SQL statement and return results.

        Args:
            sql     : SQL string (use %s for parameter placeholders)
            params  : Tuple of values for parameterised queries
                      ALWAYS use params instead of string formatting!
                      String formatting → SQL injection vulnerability.
            as_dict : If True, returns list of dicts (column_name → value)
                      If False, returns list of tuples (faster for large results)

        Returns:
            List of row dicts/tuples for SELECT.
            Empty list for INSERT/UPDATE/DELETE.
        """
        if not self._conn:
            raise RuntimeError("Not connected. Call connect() first.")

        cursor_class = DictCursor if as_dict else None

        with self._conn.cursor(cursor_class) as cursor:
            try:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                # Only fetch rows for SELECT statements
                # DML statements (INSERT/UPDATE/DELETE) return rowcount, not rows
                if cursor.description:
                    return cursor.fetchall()
                return []

            except snowflake.connector.errors.ProgrammingError as e:
                logger.error("SQL execution error: %s\nSQL: %s", e, sql[:200])
                raise

    def execute_many(self, sql: str, data: list[tuple]) -> int:
        """
        Bulk insert using executemany.
        Much faster than looping execute() for large datasets.

        Returns number of rows inserted.
        """
        if not self._conn:
            raise RuntimeError("Not connected. Call connect() first.")

        with self._conn.cursor() as cursor:
            cursor.executemany(sql, data)
            return cursor.rowcount

    def bulk_load_from_stage(
        self,
        stage_name  : str,
        table_name  : str,
        file_pattern: str = ".*\\.json",
    ) -> dict:
        """
        Execute COPY INTO from a named stage into a table.
        This is the fastest way to load large files into Snowflake.
        Snowflake parallelises the load across all micro-partitions.

        Returns:
            Dict with rows_loaded, rows_parsed, errors_seen
        """
        sql = f"""
            COPY INTO {table_name}
            FROM @{stage_name}
            PATTERN = '{file_pattern}'
            FILE_FORMAT = (FORMAT_NAME = 'FLIGHT_DB.RAW.JSON_FORMAT')
            ON_ERROR = 'CONTINUE'      -- Skip bad rows, don't fail the whole load
            PURGE = FALSE              -- Keep files in stage (don't auto-delete)
            FORCE = FALSE              -- Don't re-load files already loaded
        """
        results = self.execute(sql)
        total_loaded = sum(r.get("rows_loaded", 0) for r in results)
        total_errors = sum(r.get("errors_seen", 0) for r in results)
        logger.info(
            "COPY INTO complete | rows_loaded=%d | errors=%d",
            total_loaded, total_errors
        )
        return {"rows_loaded": total_loaded, "errors_seen": total_errors}

    def close(self) -> None:
        """Close the connection, releasing the warehouse from activity."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Snowflake connection closed")

    # ── Context manager protocol ───────────────────────────────────────────
    def __enter__(self) -> "SnowflakeConnection":
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Always close, even if an exception was raised inside the with block."""
        self.close()
        # Return False = don't suppress exceptions
        return False


# ── Convenience functions ──────────────────────────────────────────────────────

def test_connection() -> bool:
    """
    Quick connectivity test. Run this to verify your .env is correct.
    Usage: python -m snowflake.connection
    """
    try:
        with SnowflakeConnection() as sf:
            result = sf.execute("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
            row = result[0]
            print(f"✓ Connected as: {row}")
            return True
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False


if __name__ == "__main__":
    # python snowflake/connection.py
    import sys
    sys.exit(0 if test_connection() else 1)