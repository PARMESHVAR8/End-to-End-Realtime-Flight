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