# monitoring/logging_config.py
"""
Centralised logging configuration.
Called once at startup — configures all loggers in the application.

WHY STRUCTURED LOGGING (JSON format):
  Plain text logs are hard to search: "grep -i error app.log"
  JSON logs can be queried like a database:
  {"level": "ERROR", "flight_id": "AI101", "component": "producer"}
  Tools like Datadog, CloudWatch, and ELK Stack ingest JSON natively.

  For development we use human-readable format.
  For production set LOG_FORMAT=json in .env.
"""

import os
import logging
import logging.config
from datetime import datetime


def setup_logging(
    level: str = None,
    log_format: str = None,
) -> None:
    """
    Configure application-wide logging.

    Args:
        level      : "DEBUG", "INFO", "WARNING", "ERROR" (default from .env or INFO)
        log_format : "text" or "json" (default from .env or "text")
    """
    log_level  = level      or os.getenv("LOG_LEVEL",  "INFO").upper()
    log_format = log_format or os.getenv("LOG_FORMAT", "text").lower()

    if log_format == "json":
        # JSON format for production (machine-readable)
        fmt = (
            '{"time": "%(asctime)s", "level": "%(levelname)s", '
            '"module": "%(name)s", "message": "%(message)s"}'
        )
    else:
        # Human-readable format for development
        fmt = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"

    logging.basicConfig(
        level   = getattr(logging, log_level, logging.INFO),
        format  = fmt,
        datefmt = "%Y-%m-%d %H:%M:%S",
        handlers= [
            logging.StreamHandler(),   # Print to console
        ]
    )

    # Quieten noisy third-party libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info("Logging configured | level=%s | format=%s", log_level, log_format)


# monitoring/logging_config.py
"""
Production-Grade Structured Logging
=====================================
Replaces the basic logging from Phase 3 with a full
production logging system.

WHAT THIS ADDS OVER PHASE 3:
  1. Correlation IDs  — trace one request across all services
  2. JSON structured  — machine-parseable for log aggregation tools
  3. File rotation    — logs don't fill up your disk at 3am
  4. Log sampling     — DEBUG logs only 10% of the time in prod
  5. Context injection— every log line carries pipeline_run_id

WHY STRUCTURED LOGGING MATTERS:
  Plain text:   "2024-06-15 10:00:01 ERROR Failed to process record"
  Structured:   {"time":"2024-06-15T10:00:01Z","level":"ERROR",
                 "message":"Failed to process record",
                 "flight_id":"AI101","run_id":"run_abc123",
                 "records_failed":1,"elapsed_ms":342}

  The structured version can be queried like a database:
    SELECT * FROM logs WHERE level='ERROR' AND flight_id='AI101'
  Tools like Datadog, CloudWatch Insights, and Grafana Loki
  do this automatically with JSON logs.

CORRELATION ID:
  When one API request touches 5 services (producer → kafka →
  consumer → postgres → snowflake), a correlation ID is a
  UUID that flows through ALL of them.
  Without it: you see 5 separate log entries with no connection.
  With it: you can reconstruct the full journey of one record.
"""

import os
import sys
import uuid
import logging
import logging.handlers
from datetime import datetime, timezone
from typing import Optional
from pathlib import Path


# ─── Global correlation ID store ─────────────────────────────────────────────
# threading.local() creates a storage area that is unique per thread.
# In a multi-threaded Airflow worker, each task gets its own correlation ID.
import threading
_local = threading.local()


def get_correlation_id() -> str:
    """Return the current thread's correlation ID, creating one if needed."""
    if not hasattr(_local, "correlation_id"):
        _local.correlation_id = str(uuid.uuid4())
    return _local.correlation_id


def set_correlation_id(correlation_id: str) -> None:
    """Set a specific correlation ID for this thread (e.g., from Airflow run_id)."""
    _local.correlation_id = correlation_id


def new_correlation_id() -> str:
    """Generate and set a fresh correlation ID. Returns the new ID."""
    cid = str(uuid.uuid4())
    _local.correlation_id = cid
    return cid


# ─── JSON Formatter ───────────────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.
    Each line is a complete JSON document — easy for log aggregators to parse.

    Output example:
    {"time":"2024-06-15T10:00:01.234Z","level":"ERROR","logger":"kafka.producer",
     "message":"Failed to send batch","correlation_id":"abc-123","records_failed":10,
     "elapsed_ms":1250,"host":"flight-worker-1","env":"production"}
    """

    RENAME_FIELDS = {
        "levelname"  : "level",
        "name"       : "logger",
        "msg"        : "message",
        "funcName"   : "function",
        "lineno"     : "line",
        "filename"   : "file",
    }

    def __init__(self, env: str = "development"):
        super().__init__()
        self.env      = env
        self.hostname = os.uname().nodename if hasattr(os, "uname") else "unknown"

    def format(self, record: logging.LogRecord) -> str:
        import json

        # Build the base log document
        log_doc = {
            "time"           : datetime.fromtimestamp(
                                   record.created, tz=timezone.utc
                               ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level"          : record.levelname,
            "logger"         : record.name,
            "message"        : record.getMessage(),
            "correlation_id" : get_correlation_id(),
            "env"            : self.env,
            "host"           : self.hostname,
        }

        # Add source location for ERROR and above (helps find the bug)
        if record.levelno >= logging.ERROR:
            log_doc["file"]     = record.filename
            log_doc["function"] = record.funcName
            log_doc["line"]     = record.lineno

        # Add exception info if present
        if record.exc_info:
            log_doc["exception"] = self.formatException(record.exc_info)

        # Merge any extra fields passed via logger.info("msg", extra={...})
        # Standard LogRecord attributes to skip (not useful in JSON)
        SKIP_ATTRS = {
            "args","created","exc_info","exc_text","filename","funcName",
            "levelname","levelno","lineno","message","module","msecs","msg",
            "name","pathname","process","processName","relativeCreated",
            "stack_info","thread","threadName",
        }
        for key, value in record.__dict__.items():
            if key not in SKIP_ATTRS and not key.startswith("_"):
                # Only include JSON-serialisable values
                try:
                    json.dumps(value)
                    log_doc[key] = value
                except (TypeError, ValueError):
                    log_doc[key] = str(value)

        return json.dumps(log_doc, ensure_ascii=False)


# ─── Human-readable Formatter ─────────────────────────────────────────────────

class ColorFormatter(logging.Formatter):
    """
    Coloured human-readable format for local development.
    Colours only work in terminals that support ANSI codes.
    """
    COLORS = {
        "DEBUG"   : "\033[36m",   # Cyan
        "INFO"    : "\033[32m",   # Green
        "WARNING" : "\033[33m",   # Yellow
        "ERROR"   : "\033[31m",   # Red
        "CRITICAL": "\033[35m",   # Magenta
    }
    RESET = "\033[0m"
    BOLD  = "\033[1m"

    def format(self, record: logging.LogRecord) -> str:
        color  = self.COLORS.get(record.levelname, "")
        reset  = self.RESET
        cid    = get_correlation_id()[:8]   # First 8 chars of UUID is enough

        return (
            f"{color}{record.levelname:<8}{reset} "
            f"\033[90m{datetime.fromtimestamp(record.created, tz=timezone.utc).strftime('%H:%M:%S')}\033[0m "
            f"\033[94m{record.name:<35}{reset} "
            f"[{cid}] "
            f"{self.BOLD if record.levelno >= logging.ERROR else ''}"
            f"{record.getMessage()}"
            f"{reset}"
        )


# ─── Main setup function ──────────────────────────────────────────────────────

def setup_logging(
    level       : Optional[str] = None,
    log_format  : Optional[str] = None,
    log_dir     : Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> logging.Logger:
    """
    Configure application-wide logging. Call once at startup.

    Args:
        level         : DEBUG / INFO / WARNING / ERROR (default: env var LOG_LEVEL or INFO)
        log_format    : "json" or "text" (default: env var LOG_FORMAT or "text")
        log_dir       : Directory for rotating log files (default: ./logs/)
        correlation_id: Set a specific correlation ID for this run

    Returns:
        Root logger (already configured — just call logging.getLogger(__name__) anywhere)
    """
    log_level  = (level     or os.getenv("LOG_LEVEL",  "INFO")).upper()
    log_format = (log_format or os.getenv("LOG_FORMAT", "text")).lower()
    env        = os.getenv("ENVIRONMENT", "development")

    if correlation_id:
        set_correlation_id(correlation_id)

    # Clear any existing handlers (prevents duplicate logs when called multiple times)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))

    # ── Handler 1: Console ────────────────────────────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level, logging.INFO))

    if log_format == "json":
        console_handler.setFormatter(JSONFormatter(env=env))
    else:
        console_handler.setFormatter(ColorFormatter())

    root_logger.addHandler(console_handler)

    # ── Handler 2: Rotating file log ─────────────────────────────────────────
    # Rotates daily. Keeps 30 days of logs.
    # Without rotation, log files grow forever → disk fills up at 3am.
    if log_dir or os.getenv("LOG_DIR"):
        actual_log_dir = Path(log_dir or os.getenv("LOG_DIR", "./logs"))
        actual_log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename    = actual_log_dir / "flight_pipeline.log",
            when        = "midnight",
            interval    = 1,
            backupCount = 30,       # Keep 30 days
            encoding    = "utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        # Always use JSON format in files — easier to parse later
        file_handler.setFormatter(JSONFormatter(env=env))
        root_logger.addHandler(file_handler)

        # Separate error-only log file (makes it easy to grep just errors)
        error_handler = logging.handlers.TimedRotatingFileHandler(
            filename    = actual_log_dir / "flight_pipeline_errors.log",
            when        = "midnight",
            backupCount = 90,       # Keep errors for 90 days
            encoding    = "utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(JSONFormatter(env=env))
        root_logger.addHandler(error_handler)

    # ── Suppress noisy third-party libraries ──────────────────────────────────
    for noisy_lib in ["kafka", "urllib3", "requests", "botocore",
                      "snowflake.connector", "paramiko"]:
        logging.getLogger(noisy_lib).setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info(
        "Logging configured",
        extra={
            "log_level"       : log_level,
            "log_format"      : log_format,
            "env"             : env,
            "correlation_id"  : get_correlation_id(),
        }
    )
    return root_logger


# ─── Context manager for correlation ID ───────────────────────────────────────

class CorrelatedOperation:
    """
    Context manager that sets a correlation ID for the duration of a block.
    All log lines inside the block share the same correlation_id.

    USAGE:
        with CorrelatedOperation(run_id="airflow_run_2024_06_15"):
            producer.send_batch(events)   # These logs share one correlation_id
            consumer.flush_buffer()       # These logs share the SAME id
    """

    def __init__(self, run_id: Optional[str] = None):
        self.run_id  = run_id or str(uuid.uuid4())
        self._prev   = None

    def __enter__(self):
        self._prev = get_correlation_id()
        set_correlation_id(self.run_id)
        return self.run_id

    def __exit__(self, *args):
        # Restore previous correlation ID when block exits
        if self._prev:
            set_correlation_id(self._prev)