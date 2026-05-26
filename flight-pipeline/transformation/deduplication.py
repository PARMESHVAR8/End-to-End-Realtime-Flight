# transformation/deduplication.py
"""
Deduplication Strategies
=========================
Handles duplicate records that arise from Kafka's at-least-once delivery.

WHY DUPLICATES OCCUR:
  Kafka guarantees "at-least-once" delivery by default.
  If the consumer crashes after writing to PostgreSQL but BEFORE
  committing the Kafka offset, the same messages are re-delivered
  on restart. Result: same event_id appears 2–3 times in staging.

THREE STRATEGIES — choose based on context:

  1. EXACT DUPLICATE:  identical event_id → keep first, drop rest
  2. FUZZY DUPLICATE:  same flight_id within N seconds → keep latest
  3. POSITION UPDATE:  same flight_id, different timestamp → keep ALL
                       (these are valid — a flight moves over time)

Our pipeline uses Strategy 1 (event_id dedup) as the primary guard,
and Strategy 2 as a secondary check for near-duplicates.
"""

import logging
import pandas as pd
from datetime import timedelta

logger = logging.getLogger(__name__)


class FlightDeduplicator:
    """Removes duplicate flight records from a DataFrame."""

    def __init__(self, fuzzy_window_seconds: int = 5):
        """
        Args:
            fuzzy_window_seconds: Records with the same flight_id within this
                                  many seconds of each other are considered
                                  near-duplicates. Default: 5 seconds.
        """
        self.fuzzy_window = timedelta(seconds=fuzzy_window_seconds)

    def deduplicate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """
        Apply full deduplication pipeline.

        Returns:
            (deduped_df, report) where report contains counts of
            how many duplicates each strategy removed.
        """
        original_count = len(df)
        report = {"original": original_count}

        # Strategy 1: Exact deduplication by event_id
        df, exact_removed = self._exact_dedup(df)
        report["exact_duplicates_removed"] = exact_removed

        # Strategy 2: Fuzzy deduplication (near-duplicates)
        df, fuzzy_removed = self._fuzzy_dedup(df)
        report["fuzzy_duplicates_removed"] = fuzzy_removed

        report["final_count"]     = len(df)
        report["total_removed"]   = original_count - len(df)
        report["dedup_rate_pct"]  = round(report["total_removed"] / max(original_count, 1) * 100, 2)

        logger.info(
            "Deduplication | original=%d | exact_removed=%d | "
            "fuzzy_removed=%d | final=%d",
            original_count, exact_removed, fuzzy_removed, len(df)
        )
        return df, report

    def _exact_dedup(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """
        Remove rows with identical event_id.
        Keep the FIRST occurrence (earliest loaded_at).

        pandas drop_duplicates(subset=["event_id"], keep="first"):
          - Sorts are done based on current row order
          - "first" = keep the row that appears first in the DataFrame
          - We sort by loaded_at ASC first so "first" = earliest record
        """
        if "event_id" not in df.columns:
            logger.warning("No event_id column — skipping exact dedup")
            return df, 0

        before = len(df)

        # Sort so "first" occurrence = earliest loaded_at
        if "loaded_at" in df.columns:
            df = df.sort_values("loaded_at", ascending=True)

        df = df.drop_duplicates(subset=["event_id"], keep="first")
        removed = before - len(df)

        if removed > 0:
            logger.debug("Exact dedup removed %d records", removed)

        return df.reset_index(drop=True), removed

    def _fuzzy_dedup(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """
        Remove near-duplicate records: same flight_id within fuzzy_window seconds.

        Example:
          event_id=A, flight_id="AI101", timestamp=14:00:00.000  ← KEEP
          event_id=B, flight_id="AI101", timestamp=14:00:00.003  ← REMOVE (3ms apart)
          event_id=C, flight_id="AI101", timestamp=14:00:30.000  ← KEEP (30s apart)

        This catches cases where the producer accidentally sent the same
        position update twice within milliseconds (network retry).

        Algorithm:
          1. Sort by flight_id, then timestamp
          2. For each group (same flight_id), compute time diff between consecutive rows
          3. Mark rows where time diff < fuzzy_window as near-duplicates
          4. Keep only non-near-duplicates
        """
        if "event_timestamp" not in df.columns or "flight_id" not in df.columns:
            return df, 0

        before = len(df)

        ts_col = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
        df = df.copy()
        df["_ts_parsed"] = ts_col

        # Sort: within each flight, chronological order
        df = df.sort_values(["flight_id", "_ts_parsed"])

        # For each flight group, compute time since previous event
        # groupby().diff() computes the difference between consecutive rows
        # in the same group — vectorised (fast)
        df["_time_since_prev"] = (
            df.groupby("flight_id")["_ts_parsed"]
            .diff()   # Result is timedelta
        )

        # Mark as near-duplicate if within fuzzy window
        # First row in each group has NaT (no previous) → always keep
        near_dup_mask = (
            df["_time_since_prev"].notna() &
            (df["_time_since_prev"] < self.fuzzy_window)
        )

        df = df[~near_dup_mask]
        removed = before - len(df)

        # Clean up helper columns
        df = df.drop(columns=["_ts_parsed", "_time_since_prev"], errors="ignore")

        if removed > 0:
            logger.debug("Fuzzy dedup removed %d near-duplicate records", removed)

        return df.reset_index(drop=True), removed

    @staticmethod
    def find_duplicate_event_ids(df: pd.DataFrame) -> pd.DataFrame:
        """
        Utility: return a DataFrame of all duplicate event_ids for inspection.
        Useful for debugging and the DUPLICATE_EVENTS_LOG table.
        """
        if "event_id" not in df.columns:
            return pd.DataFrame()

        duplicate_ids = df[df.duplicated(subset=["event_id"], keep=False)]
        return duplicate_ids.sort_values("event_id")