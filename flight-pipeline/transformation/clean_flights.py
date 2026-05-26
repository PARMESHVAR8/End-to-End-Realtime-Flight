# transformation/clean_flights.py
"""
Flight Data Transformation Engine
===================================
Applies all cleaning and enrichment rules to raw flight records.

Design principles:
  1. PURE FUNCTIONS — each transformation function takes data in, returns
     data out, with no side effects. This makes them independently testable.

  2. PIPELINE PATTERN — transformations are chained:
     raw_df → deduplicate → validate_types → clean_strings → impute_nulls
            → cap_ranges → derive_columns → flag_quality → clean_df

  3. AUDIT TRAIL — every record knows exactly what happened to it.
     The 'transformation_log' column records which rules were applied.

  4. IDEMPOTENT — running the same input through twice produces the same
     output. No random elements, no side effects.

USAGE:
    from transformation.clean_flights import FlightTransformer
    transformer = FlightTransformer()
    clean_df, report = transformer.transform(raw_df)
"""

import logging
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

# Hard physical limits — values outside these are data errors, not outliers
ALTITUDE_MIN, ALTITUDE_MAX = 0, 60_000        # feet
SPEED_MIN,    SPEED_MAX    = 0, 1_200         # km/h (SR-71 Blackbird tops out here)
LAT_MIN,      LAT_MAX      = -90, 90
LON_MIN,      LON_MAX      = -180, 180
DELAY_MIN                  = 0               # Negative delay is meaningless

# Status values our system recognises
VALID_STATUSES = frozenset({
    "active", "scheduled", "landed", "cancelled", "diverted", "unknown"
})

# Indian domestic airports (for is_international flag)
INDIA_AIRPORT_CODES = frozenset({
    "DEL", "BOM", "BLR", "MAA", "CCU", "HYD",
    "AMD", "COK", "GOI", "JAI", "PNQ", "NAG",
    "IXC", "ATQ", "SXR", "IXB", "GAU", "IMF",
})

# Airline IATA code → canonical full name mapping
# Normalises variations like "Air India Ltd" → "Air India"
AIRLINE_NAME_MAP = {
    "AI": "Air India",
    "6E": "IndiGo",
    "SG": "SpiceJet",
    "UK": "Vistara",
    "G8": "GoFirst",
    "EK": "Emirates",
    "SQ": "Singapore Airlines",
    "BA": "British Airways",
    "QR": "Qatar Airways",
    "G9": "Air Arabia",
    "IX": "Air India Express",
    "I5": "AIX Connect",
}


class FlightTransformer:
    """
    Applies the full transformation pipeline to a DataFrame of raw flights.

    Each step is a separate method — call them individually for testing
    or call transform() for the full pipeline.
    """

    def __init__(self, strict_mode: bool = False):
        """
        Args:
            strict_mode: If True, raise exceptions on bad data instead of
                         imputing/flagging. Use in testing to catch schema issues.
        """
        self.strict_mode  = strict_mode
        self.stats: dict  = {}   # Populated by transform() for reporting

    # ─── Main pipeline entry point ────────────────────────────────────────────

    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """
        Run the full transformation pipeline.

        Args:
            df: Raw DataFrame from Snowflake RAW layer or PostgreSQL staging.
                Must have columns matching our flight schema.

        Returns:
            (clean_df, report) where:
              clean_df: Transformed DataFrame ready for CLEAN layer
              report  : Dict with counts and stats for logging/monitoring
        """
        if df.empty:
            logger.warning("Transformer received empty DataFrame — nothing to do")
            return df, {"records_in": 0, "records_out": 0}

        start_time   = datetime.now(timezone.utc)
        records_in   = len(df)
        logger.info("Starting transformation | records=%d", records_in)

        # ── Run pipeline steps in order ───────────────────────────────────────
        # Each step returns a modified copy of the DataFrame.
        # We use .copy() to prevent pandas SettingWithCopyWarning.
        df = df.copy()

        df = self._add_transformation_log(df)   # Step 0: add audit column
        df = self._cast_types(df)               # Step 1: enforce correct types
        df = self._clean_strings(df)            # Step 2: normalise text fields
        df = self._impute_nulls(df)             # Step 3: fill missing values
        df = self._cap_ranges(df)               # Step 4: clip numeric outliers
        df = self._normalise_status(df)         # Step 5: validate enum values
        df = self._derive_columns(df)           # Step 6: add computed columns
        df = self._flag_quality(df)             # Step 7: mark anomalous records
        df = self._add_metadata(df)             # Step 8: add pipeline metadata

        records_out  = len(df)
        flagged      = int(df["data_quality_flag"].sum()) if "data_quality_flag" in df.columns else 0
        elapsed      = (datetime.now(timezone.utc) - start_time).total_seconds()

        report = {
            "records_in"       : records_in,
            "records_out"      : records_out,
            "records_flagged"  : flagged,
            "flag_rate_pct"    : round(flagged / max(records_out, 1) * 100, 2),
            "elapsed_seconds"  : round(elapsed, 3),
            "throughput_rps"   : round(records_out / max(elapsed, 0.001)),
        }
        logger.info("Transformation complete | %s", report)
        self.stats = report
        return df, report

    # ─── Step 0: Audit column ─────────────────────────────────────────────────

    def _add_transformation_log(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds a 'transformation_log' column — a list of rules applied to each row.
        Example value: "cast_types,imputed_altitude,capped_speed"

        This gives every record a "paper trail" — when an analyst asks
        "why does this record have altitude=0?", the log says "imputed_altitude".
        """
        df["transformation_log"] = ""
        return df

    def _append_log(self, df: pd.DataFrame, mask: pd.Series, rule: str) -> pd.DataFrame:
        """Helper: append a rule name to transformation_log for flagged rows."""
        df.loc[mask, "transformation_log"] = (
            df.loc[mask, "transformation_log"]
            .str.cat([rule] * mask.sum(), sep=",")
            .str.strip(",")
        )
        return df

    # ─── Step 1: Type casting ─────────────────────────────────────────────────

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enforce correct Python/Pandas types for all columns.

        WHY THIS MATTERS:
          Kafka sends everything as JSON, which deserialises to Python dicts.
          Numbers sometimes arrive as strings: "altitude": "35000" (with quotes).
          If you don't cast, pandas stores these as 'object' dtype (slow)
          and arithmetic operations silently fail or produce NaN.

        pd.to_numeric(errors='coerce'):
          Converts "35000" → 35000
          Converts "not_a_number" → NaN (instead of raising an exception)
          NaN is then handled in the next step (impute_nulls)
        """
        numeric_cols = {
            "altitude"      : "Int64",   # Capital I = nullable integer
            "speed"         : "float64",
            "latitude"      : "float64",
            "longitude"     : "float64",
            "heading"       : "float64",
            "delay_minutes" : "Int64",
        }
        for col, dtype in numeric_cols.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Int64 (nullable) can hold NaN; int64 cannot
                if dtype == "Int64":
                    df[col] = df[col].astype("Int64")

        # Timestamp columns: parse to datetime with UTC timezone
        ts_cols = ["event_timestamp", "departure_time", "arrival_time",
                   "loaded_at", "ingested_at"]
        for col in ts_cols:
            if col in df.columns and df[col].dtype == object:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

        return df

    # ─── Step 2: String normalisation ─────────────────────────────────────────

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalise all string/text columns.

        Common issues in real flight data:
          - Leading/trailing whitespace: "  Air India  " → "Air India"
          - Wrong case: "del" → "DEL", "air india" → "Air India"
          - Placeholder strings: "N/A", "None", "null", "???" → actual NaN
          - Airline name variations: "AIR INDIA LIMITED" → "Air India"
        """
        # Airport codes: always 3-letter uppercase IATA codes
        airport_cols = ["source_airport", "dest_airport"]
        for col in airport_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    .replace({"NAN": None, "NONE": None, "???": None, "": None, "N/A": None})
                )

        # Airline name: Title Case
        if "airline" in df.columns:
            df["airline"] = df["airline"].astype(str).str.strip().str.title()
            # Map IATA code to canonical name where possible
            if "airline_iata" in df.columns:
                iata_mask = df["airline_iata"].isin(AIRLINE_NAME_MAP)
                df.loc[iata_mask, "airline"] = df.loc[iata_mask, "airline_iata"].map(AIRLINE_NAME_MAP)

        # Airline IATA: always 2-letter uppercase
        if "airline_iata" in df.columns:
            df["airline_iata"] = (
                df["airline_iata"]
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({"NAN": None, "NONE": None, "??": None, "": None})
            )

        # Status: always lowercase
        if "status" in df.columns:
            df["status"] = df["status"].astype(str).str.strip().str.lower()

        # City names: Title Case
        for col in ["source_city", "dest_city"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()
                df[col] = df[col].replace({"Nan": None, "None": None, "": None})

        return df

    # ─── Step 3: Null imputation ──────────────────────────────────────────────

    def _impute_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill missing values with sensible defaults.

        IMPUTATION STRATEGY:
          - altitude: 0 (aircraft on ground if no altitude signal)
          - speed: 0 (stationary if no speed signal)
          - delay_minutes: 0 (assume on-time if not specified)
          - heading: NaN kept (we don't know direction — don't guess)
          - status: 'unknown' (explicit unknown is better than null)
          - lat/lon: kept as NaN — these are critical; we flag, not guess

        WHY NOT IMPUTE LAT/LON?
          If latitude is null, the aircraft has no tracked position.
          Guessing a position (e.g., midpoint of route) would create
          false data that corrupts map visualisations. Better to flag
          the record and let the analyst decide what to do with it.
        """
        imputation_rules = {
            "altitude"      : 0,
            "speed"         : 0.0,
            "delay_minutes" : 0,
            "status"        : "unknown",
        }
        for col, fill_val in imputation_rules.items():
            if col in df.columns:
                null_mask = df[col].isna()
                if null_mask.any():
                    df.loc[null_mask, col] = fill_val
                    df = self._append_log(df, null_mask, f"imputed_{col}")
                    logger.debug("Imputed %d nulls in %s", null_mask.sum(), col)

        return df

    # ─── Step 4: Range capping ────────────────────────────────────────────────

    def _cap_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clip numeric values to physically possible ranges.

        'clip' is Pandas' built-in range enforcer:
          df["altitude"].clip(lower=0, upper=60000)
          Values below 0 → 0
          Values above 60000 → 60000
          Values in range → unchanged

        IMPORTANT: We log which records were capped so analysts can
        trace back: "Why does this record have speed=1200?"
        Answer: "It was capped from 1450 — original value was impossible."
        """
        range_rules = [
            ("altitude",      ALTITUDE_MIN, ALTITUDE_MAX),
            ("speed",         SPEED_MIN,    SPEED_MAX),
            ("latitude",      LAT_MIN,      LAT_MAX),
            ("longitude",     LON_MIN,      LON_MAX),
            ("delay_minutes", DELAY_MIN,    None),       # No upper cap on delay
        ]
        for col, low, high in range_rules:
            if col not in df.columns:
                continue
            original = df[col].copy()

            if low is not None and high is not None:
                df[col] = df[col].clip(lower=low, upper=high)
            elif low is not None:
                df[col] = df[col].clip(lower=low)
            elif high is not None:
                df[col] = df[col].clip(upper=high)

            # Detect which rows were actually changed by clipping
            capped_mask = (original != df[col]) & original.notna()
            if capped_mask.any():
                df = self._append_log(df, capped_mask, f"capped_{col}")
                logger.debug("Capped %d values in %s", capped_mask.sum(), col)

        return df

    # ─── Step 5: Status normalisation ────────────────────────────────────────

    def _normalise_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map non-standard status values to our approved enum.

        Real APIs return creative status values:
          "EN-ROUTE" → "active"
          "IN_AIR"   → "active"
          "ARRIVAL"  → "landed"
          "DIVRT"    → "diverted"

        Anything unmapped → "unknown" (safe fallback)
        """
        if "status" not in df.columns:
            return df

        STATUS_MAPPING = {
            # Active variants
            "en-route": "active", "en_route": "active", "in_air": "active",
            "airborne": "active", "in-flight": "active",
            # Landed variants
            "arrived": "landed", "arrival": "landed", "on_ground": "landed",
            # Cancelled variants
            "canceled": "cancelled", "cncl": "cancelled",
            # Diverted variants
            "divrt": "diverted", "divert": "diverted",
        }

        # Map non-standard values
        non_standard_mask = ~df["status"].isin(VALID_STATUSES)
        if non_standard_mask.any():
            df.loc[non_standard_mask, "status"] = (
                df.loc[non_standard_mask, "status"]
                .map(STATUS_MAPPING)
                .fillna("unknown")
            )
            df = self._append_log(df, non_standard_mask, "normalised_status")

        return df

    # ─── Step 6: Derived columns ──────────────────────────────────────────────

    def _derive_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute new columns from existing ones.
        These columns DO NOT exist in the raw data — we create them here.

        All derivations are deterministic — same input always gives same output.
        """
        # ── is_international ──────────────────────────────────────────────────
        # True when source OR dest is outside India
        if all(c in df.columns for c in ["source_airport", "dest_airport"]):
            df["is_international"] = ~(
                df["source_airport"].isin(INDIA_AIRPORT_CODES) &
                df["dest_airport"].isin(INDIA_AIRPORT_CODES)
            )

        # ── delay_bucket ──────────────────────────────────────────────────────
        # pd.cut divides a continuous variable into categorical bins
        # bins=[0, 1, 15, 60, 180, inf] means:
        #   0-1   → 'on_time'
        #   1-15  → 'minor_delay'
        #   15-60 → 'moderate_delay'
        #   etc.
        if "delay_minutes" in df.columns:
            delay_numeric = pd.to_numeric(df["delay_minutes"], errors="coerce").fillna(0)
            df["delay_bucket"] = pd.cut(
                delay_numeric,
                bins   = [-1, 0, 15, 60, 180, float("inf")],
                labels = ["on_time", "minor_delay", "moderate_delay",
                          "major_delay", "severe_delay"],
                right  = True,
            ).astype(str)

        # ── flight_phase ──────────────────────────────────────────────────────
        if "altitude" in df.columns:
            alt = pd.to_numeric(df["altitude"], errors="coerce").fillna(0)
            df["flight_phase"] = pd.cut(
                alt,
                bins   = [-1, 999, 14999, 31999, float("inf")],
                labels = ["ground", "climbing", "mid_altitude", "cruise"],
                right  = True,
            ).astype(str)

        # ── region ────────────────────────────────────────────────────────────
        # Vectorised apply using np.select — much faster than row-by-row apply()
        # np.select(conditions, choices, default) is Pandas/NumPy's switch-case
        if all(c in df.columns for c in ["latitude", "longitude"]):
            lat = pd.to_numeric(df["latitude"], errors="coerce")
            lon = pd.to_numeric(df["longitude"], errors="coerce")

            conditions = [
                (lat.between(8, 37)) & (lon.between(68, 97)),
                lat >= 35,
                lat <= 0,
            ]
            choices = ["India", "Europe_Asia", "Southern"]
            df["region"] = np.select(conditions, choices, default="Middle_East_SE_Asia")

        # ── route_key ─────────────────────────────────────────────────────────
        # "DEL→BOM" format — used as display label in dashboard
        if all(c in df.columns for c in ["source_airport", "dest_airport"]):
            df["route_key"] = (
                df["source_airport"].fillna("???") + "→" +
                df["dest_airport"].fillna("???")
            )

        # ── hour_of_day, day_of_week ──────────────────────────────────────────
        # Pre-extract time components for fast GROUP BY in analytics queries
        if "event_timestamp" in df.columns:
            ts = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
            df["event_date"]   = ts.dt.date
            df["event_hour"]   = ts.dt.hour
            df["day_of_week"]  = ts.dt.dayofweek   # 0=Monday, 6=Sunday

        return df

    # ─── Step 7: Quality flagging ─────────────────────────────────────────────

    def _flag_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mark records with data quality issues.

        We DO NOT remove bad records — we flag them.
        This keeps them in the CLEAN layer but labelled.

        Downstream:
          FACT_FLIGHTS only loads records where data_quality_flag = FALSE.
          Analysts can still query flagged records for debugging.

        think of flags like a yellow card in football —
        the player stays on the field but is watched closely.
        A red card (hard delete) removes the player entirely.
        We prefer yellow cards: data is preserved with a warning.
        """
        if "data_quality_flag" not in df.columns:
            df["data_quality_flag"] = False

        quality_rules = {
            # Critical: aircraft has no tracked position
            "missing_coordinates": (
                df.get("latitude", pd.Series(dtype=float)).isna() |
                df.get("longitude", pd.Series(dtype=float)).isna()
            ),
            # Critical: we don't know what flight this is
            "missing_flight_id": df.get("flight_id", pd.Series(dtype=str)).isna(),

            # Suspicious: active flight showing zero speed
            # (Valid during taxi — but flag anyway for review)
            "active_with_zero_speed": (
                (df.get("status", pd.Series(dtype=str)) == "active") &
                (df.get("speed", pd.Series(dtype=float)).fillna(0) == 0)
            ),

            # Suspicious: unknown airport code
            "unknown_airport": (
                df.get("source_airport", pd.Series(dtype=str)).isin(["???", "UNK", ""]) |
                df.get("dest_airport",   pd.Series(dtype=str)).isin(["???", "UNK", ""])
            ),

            # Data error: aircraft at cruise speed but on ground
            "speed_altitude_mismatch": (
                (df.get("altitude", pd.Series(dtype=float)).fillna(0) < 1000) &
                (df.get("speed",    pd.Series(dtype=float)).fillna(0) > 500)
            ),
        }

        flag_mask = pd.Series(False, index=df.index)
        for rule_name, condition in quality_rules.items():
            # Align index before OR — prevents pandas alignment errors
            condition = condition.reindex(df.index, fill_value=False)
            triggered = flag_mask | condition
            newly_flagged = condition & ~flag_mask
            if newly_flagged.any():
                df = self._append_log(df, newly_flagged, f"flag:{rule_name}")
            flag_mask = triggered

        df["data_quality_flag"] = flag_mask
        flagged_count = int(flag_mask.sum())
        if flagged_count > 0:
            logger.warning("Flagged %d records for quality issues", flagged_count)

        return df

    # ─── Step 8: Pipeline metadata ────────────────────────────────────────────

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add columns that identify when and how this data was transformed."""
        df["transformed_at"]       = datetime.now(timezone.utc)
        df["transformer_version"]  = "1.0.0"   # Increment when rules change
        return df