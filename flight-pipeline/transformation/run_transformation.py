# transformation/run_transformation.py
"""
Transformation Runner
======================
Orchestrates the full transformation pipeline.
Called directly by Airflow PythonOperator tasks.

Can also be run standalone for testing:
  python -m transformation.run_transformation --mode full
  python -m transformation.run_transformation --mode incremental --limit 1000
"""

import sys
import logging
import argparse
import pandas as pd
from datetime import datetime, timezone

from transformation.clean_flights   import FlightTransformer
from transformation.deduplication   import FlightDeduplicator
from transformation.validate        import FlightDataValidator
from transformation.incremental     import WatermarkManager
from snowflake.connection           import SnowflakeConnection
from monitoring.logging_config      import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def run_full_transformation(
    limit: int = 50_000,
    run_id: str = "manual",
    **context
) -> dict:
    """
    Full transformation pipeline:
      RAW → deduplicate → validate → clean → CLEAN → update watermark

    This is what Airflow's clean_transform task calls (Phase 4).
    Adding the watermark logic here makes it truly incremental.

    Returns dict with stats pushed to XCom.
    """
    logger.info("Starting transformation run | run_id=%s | limit=%d", run_id, limit)
    start_time = datetime.now(timezone.utc)

    wm_manager   = WatermarkManager("raw_to_clean")
    deduplicator = FlightDeduplicator(fuzzy_window_seconds=5)
    transformer  = FlightTransformer(strict_mode=False)
    validator    = FlightDataValidator()

    with SnowflakeConnection() as sf:

        # ── Step 1: Fetch incremental window ──────────────────────────────────
        df_raw = wm_manager.get_incremental_window(
            sf               = sf,
            source_table     = "FLIGHT_DB.RAW.FLIGHTS_RAW",
            timestamp_column = "loaded_at",
            extra_filter     = "AND is_transformed = FALSE",
            limit            = limit,
        )

        if df_raw.empty:
            logger.info("No new records to transform — exiting early")
            return {
                "records_in": 0, "records_out": 0,
                "status": "no_data", "run_id": run_id
            }

        # ── Step 2: Validate raw data BEFORE transforming ─────────────────────
        logger.info("Running pre-transformation validation on %d records", len(df_raw))
        pre_validation = validator.validate(df_raw)
        pre_validation.print_report()

        # ── Step 3: Deduplicate ───────────────────────────────────────────────
        df_deduped, dedup_report = deduplicator.deduplicate(df_raw)
        logger.info("Deduplication: %s", dedup_report)

        # ── Step 4: Transform ─────────────────────────────────────────────────
        df_clean, transform_report = transformer.transform(df_deduped)

        # ── Step 5: Validate AFTER transforming ───────────────────────────────
        post_validation = validator.validate(df_clean)
        logger.info(
            "Post-transform validation | healthy=%s | pass_rate=%.1f%%",
            post_validation.is_healthy,
            post_validation.pass_rate * 100
        )

        # ── Step 6: Write to CLEAN layer ──────────────────────────────────────
        records_written = _write_to_clean(sf, df_clean)

        # ── Step 7: Mark RAW records as transformed ───────────────────────────
        event_ids = df_clean["event_id"].dropna().tolist()
        if event_ids:
            _mark_raw_as_transformed(sf, event_ids)

        # ── Step 8: Update watermark — ONLY on success ────────────────────────
        if "loaded_at" in df_clean.columns and not df_clean["loaded_at"].isna().all():
            max_ts = pd.to_datetime(df_clean["loaded_at"], utc=True).max()
            wm_manager.update_watermark(sf, max_ts.to_pydatetime(), records_written, run_id)

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    result  = {
        "run_id"              : run_id,
        "records_in"          : len(df_raw),
        "records_after_dedup" : len(df_deduped),
        "records_out"         : records_written,
        "dedup_removed"       : dedup_report.get("total_removed", 0),
        "quality_flagged"     : transform_report.get("records_flagged", 0),
        "pre_validation_ok"   : pre_validation.is_healthy,
        "post_validation_ok"  : post_validation.is_healthy,
        "elapsed_seconds"     : round(elapsed, 2),
        "status"              : "success",
    }
    logger.info("Transformation run complete | %s", result)

    # Push to XCom if called from Airflow
    if "ti" in context:
        context["ti"].xcom_push(key="transform_result", value=result)

    return result


def _write_to_clean(sf: SnowflakeConnection, df: pd.DataFrame) -> int:
    """
    Bulk insert transformed records into FLIGHT_DB.CLEAN.FLIGHTS_CLEAN.
    Uses MERGE to be idempotent — duplicate event_ids are silently skipped.
    Returns number of records actually inserted.
    """
    if df.empty:
        return 0

    # Select only the columns that exist in FLIGHTS_CLEAN
    clean_cols = [
        "event_id", "flight_id", "airline", "airline_iata", "flight_number",
        "source_airport", "dest_airport", "source_city", "dest_city",
        "latitude", "longitude", "altitude", "speed", "heading", "status",
        "departure_time", "arrival_time", "delay_minutes", "aircraft_type",
        "event_timestamp", "source",
        "is_international", "delay_bucket", "flight_phase", "region",
        "route_key", "data_quality_flag", "transformation_log",
        "loaded_at", "transformed_at",
    ]
    available = [c for c in clean_cols if c in df.columns]
    df_insert  = df[available].copy()

    # Convert timestamps to strings for Snowflake parameterised insert
    for col in ["event_timestamp", "departure_time", "arrival_time",
                "loaded_at", "transformed_at"]:
        if col in df_insert.columns:
            df_insert[col] = pd.to_datetime(
                df_insert[col], utc=True, errors="coerce"
            ).dt.strftime("%Y-%m-%d %H:%M:%S%z")

    # Replace pandas NA/NaT with None (Snowflake-compatible NULL)
    df_insert = df_insert.where(pd.notnull(df_insert), None)

    rows = [tuple(row) for row in df_insert.itertuples(index=False, name=None)]
    col_list    = ", ".join(available)
    placeholders= ", ".join(["%s"] * len(available))

    inserted = sf.execute_many(
        f"""
        INSERT INTO FLIGHT_DB.CLEAN.FLIGHTS_CLEAN ({col_list})
        SELECT {placeholders}
        WHERE NOT EXISTS (
            SELECT 1 FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN c
            WHERE c.event_id = %s
        )
        """,
        # Append event_id again for the WHERE NOT EXISTS check
        [row + (row[0],) for row in rows]
    )
    logger.info("Wrote %d records to FLIGHTS_CLEAN", inserted)
    return inserted


def _mark_raw_as_transformed(sf: SnowflakeConnection, event_ids: list[str]) -> None:
    """Mark raw records as transformed so they aren't reprocessed."""
    if not event_ids:
        return
    ids_str = "', '".join(event_ids)
    sf.execute(
        f"""
        UPDATE FLIGHT_DB.RAW.FLIGHTS_RAW
        SET is_transformed = TRUE,
            transformed_at = CURRENT_TIMESTAMP()
        WHERE event_id IN ('{ids_str}')
        AND is_transformed = FALSE
        """
    )
    logger.debug("Marked %d raw records as transformed", len(event_ids))


# ─── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flight Data Transformation Runner")
    parser.add_argument("--limit", type=int, default=10_000,
                        help="Max records per run (default: 10000)")
    parser.add_argument("--run-id", default="cli_manual",
                        help="Run identifier for watermark tracking")
    args = parser.parse_args()

    result = run_full_transformation(limit=args.limit, run_id=args.run_id)
    print("\nTransformation Result:")
    for k, v in result.items():
        print(f"  {k:<30}: {v}")
    sys.exit(0 if result.get("status") == "success" else 1)