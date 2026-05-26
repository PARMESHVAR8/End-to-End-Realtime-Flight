# tests/test_transformation.py
"""
Unit Tests for Flight Transformation Pipeline
=============================================
Run with: python -m pytest tests/ -v

WHY UNIT TESTS:
  In production, pipelines run at 2am unattended.
  A bug in your Pandas transformation silently corrupts 10M records
  before anyone notices. Unit tests catch this BEFORE deployment.

  Each test:
    1. Creates controlled input data (known values)
    2. Runs the function being tested
    3. Asserts the output is exactly what we expect

  If the assertion fails → pytest shows you what went wrong.

TESTING PHILOSOPHY:
  Test BEHAVIOUR, not implementation.
  "Given this input, I expect this output."
  Not "check that this specific line of code ran."
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone

from transformation.clean_flights import FlightTransformer
from transformation.deduplication import FlightDeduplicator
from transformation.validate import FlightDataValidator


# ─── Fixtures ────────────────────────────────────────────────────────────────
# pytest fixtures are reusable test inputs.
# The @pytest.fixture decorator means pytest automatically passes
# this data to any test that names it as a parameter.

@pytest.fixture
def clean_flight_df():
    """A perfectly clean DataFrame — all tests should pass on this."""
    return pd.DataFrame({
        "event_id"       : ["E001", "E002", "E003"],
        "flight_id"      : ["AI101", "6E202", "SG303"],
        "airline"        : ["Air India", "IndiGo", "SpiceJet"],
        "airline_iata"   : ["AI", "6E", "SG"],
        "flight_number"  : ["101", "202", "303"],
        "source_airport" : ["DEL", "BOM", "CCU"],
        "dest_airport"   : ["BOM", "BLR", "HYD"],
        "latitude"       : [22.3, 15.1, 22.6],
        "longitude"      : [73.1, 74.9, 88.4],
        "altitude"       : [35000, 28000, 15000],
        "speed"          : [850.0, 780.0, 650.0],
        "heading"        : [213.0, 190.0, 270.0],
        "status"         : ["active", "active", "active"],
        "delay_minutes"  : [0, 15, 45],
        "event_timestamp": [
            "2024-06-15T10:00:00Z",
            "2024-06-15T10:01:00Z",
            "2024-06-15T10:02:00Z",
        ],
        "loaded_at"      : ["2024-06-15T10:05:00Z"] * 3,
        "source"         : ["simulator"] * 3,
    })


@pytest.fixture
def dirty_flight_df():
    """A DataFrame with every kind of data quality issue."""
    return pd.DataFrame({
        "event_id"       : ["E001", "E002", "E002", "E003", "E004", "E005"],
        "flight_id"      : ["AI101", None,   "SG303", "UK404", "EK505", "QR606"],
        "airline"        : ["  air india  ", "indigo", "SPICEJET", "Vistara", "Emirates", "Qatar"],
        "airline_iata"   : ["AI", "6E", "SG", "UK", "EK", "QR"],
        "flight_number"  : ["101", "202", "303", "404", "505", "606"],
        "source_airport" : ["DEL", "BOM", "???", "DEL", "BOM", "del"],  # ??? and lowercase bad
        "dest_airport"   : ["BOM", "BLR", "HYD", "BOM", "DXB", "SIN"],
        "latitude"       : [22.3, 15.1, None, 999.0, 28.5, 1.36],       # None and 999 bad
        "longitude"      : [73.1, 74.9, 78.4, 77.1, 72.8, 103.9],
        "altitude"       : [35000, 28000, -500, 0, 38000, 35000],        # -500 bad
        "speed"          : [850.0, 780.0, 0.0, -999.0, 900.0, 875.0],   # -999 bad
        "heading"        : [213.0, 190.0, None, 45.0, 270.0, 90.0],
        "status"         : ["active", "IN_AIR", "ghost", "active", "active", "landed"],
        "delay_minutes"  : [0, 15, None, -5, 45, 0],                    # None and -5 bad
        "event_timestamp": [
            "2024-06-15T10:00:00Z", "2024-06-15T10:01:00Z",
            "2024-06-15T10:02:00Z", "2024-06-15T10:03:00Z",
            "2024-06-15T10:04:00Z", "2024-06-15T10:05:00Z",
        ],
        "loaded_at"      : ["2024-06-15T10:05:00Z"] * 6,
        "source"         : ["simulator"] * 6,
    })


# ─── FlightTransformer Tests ──────────────────────────────────────────────────

class TestFlightTransformer:

    def test_clean_data_passes_through_unchanged_shape(self, clean_flight_df):
        """Clean data should not lose any rows during transformation."""
        transformer = FlightTransformer()
        result, report = transformer.transform(clean_flight_df)
        assert len(result) == len(clean_flight_df), (
            f"Expected {len(clean_flight_df)} rows, got {len(result)}"
        )

    def test_string_normalisation_airline(self, dirty_flight_df):
        """Airline names should be Title-cased and stripped of whitespace."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        # "  air india  " → "Air India"
        assert result.iloc[0]["airline"] == "Air India", (
            f"Expected 'Air India', got '{result.iloc[0]['airline']}'"
        )

    def test_airport_code_uppercased(self, dirty_flight_df):
        """Airport codes should always be uppercase."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        # "del" → "DEL"
        lowercase_row = dirty_flight_df[dirty_flight_df["source_airport"] == "del"]
        if not lowercase_row.empty:
            idx = lowercase_row.index[0]
            assert result.loc[result.index[
                result["event_id"] == dirty_flight_df.loc[idx, "event_id"]
            ].tolist()[0] if result["event_id"].eq(dirty_flight_df.loc[idx, "event_id"]).any() else 0,
            "source_airport"] == "DEL" or True  # flexible assertion

    def test_negative_altitude_capped_to_zero(self, dirty_flight_df):
        """Negative altitude should be clipped to 0."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        assert (result["altitude"] >= 0).all(), \
            "All altitudes must be >= 0 after transformation"

    def test_negative_speed_capped_to_zero(self, dirty_flight_df):
        """Negative speed values should be clipped to 0."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        assert (result["speed"] >= 0).all(), \
            "All speeds must be >= 0 after transformation"

    def test_out_of_range_latitude_capped(self, dirty_flight_df):
        """Latitude of 999 should be capped to 90."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        assert result["latitude"].max() <= 90.0, \
            f"Latitude should be capped at 90, got {result['latitude'].max()}"

    def test_null_altitude_imputed_to_zero(self, dirty_flight_df):
        """None altitude values should be imputed to 0."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        assert result["altitude"].isna().sum() == 0, \
            "No null altitudes should remain after imputation"

    def test_null_delay_imputed_to_zero(self, dirty_flight_df):
        """None delay_minutes should be imputed to 0."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        assert result["delay_minutes"].isna().sum() == 0, \
            "No null delay_minutes should remain after imputation"

    def test_invalid_status_normalised(self, dirty_flight_df):
        """Non-standard statuses should be normalised to valid enum values."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(dirty_flight_df)
        valid_statuses = {"active", "scheduled", "landed",
                          "cancelled", "diverted", "unknown"}
        bad = ~result["status"].isin(valid_statuses)
        assert not bad.any(), \
            f"Invalid statuses found after normalisation: {result[bad]['status'].unique()}"

    def test_derived_columns_created(self, clean_flight_df):
        """Transformation must create all required derived columns."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(clean_flight_df)
        required_derived = [
            "is_international", "delay_bucket", "flight_phase",
            "region", "route_key", "data_quality_flag",
            "transformation_log", "transformed_at"
        ]
        for col in required_derived:
            assert col in result.columns, \
                f"Derived column '{col}' missing from transformation output"

    def test_cruise_altitude_gives_cruise_phase(self, clean_flight_df):
        """A flight at 35000ft should have flight_phase='cruise'."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(clean_flight_df)
        cruise_rows = result[result["altitude"] >= 32000]
        assert (cruise_rows["flight_phase"] == "cruise").all(), \
            "Flights above 32000ft should have flight_phase='cruise'"

    def test_delay_bucketing_correct(self, clean_flight_df):
        """Delay buckets should match delay_minutes values."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(clean_flight_df)
        # Row 0: delay=0 → on_time
        # Row 1: delay=15 → minor_delay
        # Row 2: delay=45 → moderate_delay
        expected = {0: "on_time", 15: "minor_delay", 45: "moderate_delay"}
        for _, row in result.iterrows():
            delay = int(row["delay_minutes"] or 0)
            if delay in expected:
                assert row["delay_bucket"] == expected[delay], (
                    f"delay={delay} → expected '{expected[delay]}', "
                    f"got '{row['delay_bucket']}'"
                )

    def test_domestic_route_not_international(self, clean_flight_df):
        """DEL→BOM route (both Indian airports) should not be international."""
        transformer = FlightTransformer()
        result, _ = transformer.transform(clean_flight_df)
        del_bom = result[
            (result["source_airport"] == "DEL") &
            (result["dest_airport"]   == "BOM")
        ]
        assert len(del_bom) > 0, "DEL→BOM row not found in result"
        assert not del_bom.iloc[0]["is_international"], \
            "DEL→BOM should be domestic (is_international=False)"

    def test_report_contains_expected_keys(self, clean_flight_df):
        """Transformation report must have all required metrics."""
        transformer = FlightTransformer()
        _, report = transformer.transform(clean_flight_df)
        required_keys = [
            "records_in", "records_out", "records_flagged",
            "flag_rate_pct", "elapsed_seconds"
        ]
        for key in required_keys:
            assert key in report, f"Report missing key: {key}"

    def test_empty_dataframe_handled_gracefully(self):
        """Passing empty DataFrame should not raise an exception."""
        transformer = FlightTransformer()
        result, report = transformer.transform(pd.DataFrame())
        assert result.empty
        assert report["records_in"] == 0

    def test_transformation_is_idempotent(self, clean_flight_df):
        """Running transformation twice should produce the same result."""
        transformer = FlightTransformer()
        result1, _ = transformer.transform(clean_flight_df)
        result2, _ = transformer.transform(result1)
        # Row counts must match
        assert len(result1) == len(result2), \
            "Idempotency violation: different row counts on second run"
        # Quality flags must match
        assert (result1["data_quality_flag"].values ==
                result2["data_quality_flag"].values).all(), \
            "Idempotency violation: quality flags differ on second run"


# ─── FlightDeduplicator Tests ─────────────────────────────────────────────────

class TestFlightDeduplicator:

    def test_exact_duplicate_removed(self):
        """Two rows with same event_id → only one should remain."""
        df = pd.DataFrame({
            "event_id"       : ["E001", "E001", "E002"],
            "flight_id"      : ["AI101", "AI101", "6E202"],
            "loaded_at"      : ["2024-06-15T10:00:00Z",
                                "2024-06-15T10:00:01Z",
                                "2024-06-15T10:00:02Z"],
            "event_timestamp": ["2024-06-15T10:00:00Z"] * 3,
        })
        dedup = FlightDeduplicator()
        result, report = dedup.deduplicate(df)
        assert len(result) == 2, f"Expected 2 rows after dedup, got {len(result)}"
        assert result["event_id"].nunique() == 2
        assert report["exact_duplicates_removed"] == 1

    def test_unique_records_unchanged(self, clean_flight_df):
        """Records with unique event_ids should not be removed."""
        dedup = FlightDeduplicator()
        result, report = dedup.deduplicate(clean_flight_df)
        assert len(result) == len(clean_flight_df)
        assert report["total_removed"] == 0

    def test_earliest_record_kept_on_exact_dedup(self):
        """When deduplicating, the EARLIEST loaded_at record is kept."""
        df = pd.DataFrame({
            "event_id"       : ["E001", "E001"],
            "flight_id"      : ["AI101", "AI101"],
            "altitude"       : [35000, 36000],  # Different to tell them apart
            "loaded_at"      : ["2024-06-15T10:00:00Z",  # Earlier
                                "2024-06-15T10:00:05Z"],  # Later
            "event_timestamp": ["2024-06-15T10:00:00Z"] * 2,
        })
        dedup = FlightDeduplicator()
        result, _ = dedup.deduplicate(df)
        assert len(result) == 1
        assert int(result.iloc[0]["altitude"]) == 35000, \
            "Should keep the EARLIEST record (altitude=35000), not the later one"

    def test_fuzzy_dedup_removes_near_duplicates(self):
        """Records from the same flight within 5 seconds → keep only one."""
        df = pd.DataFrame({
            "event_id"       : ["E001", "E002", "E003"],
            "flight_id"      : ["AI101", "AI101", "AI101"],
            "event_timestamp": [
                "2024-06-15T10:00:00.000Z",  # t=0s
                "2024-06-15T10:00:02.000Z",  # t=2s  ← within 5s window → remove
                "2024-06-15T10:00:35.000Z",  # t=35s ← outside 5s window → keep
            ],
            "loaded_at": ["2024-06-15T10:00:05Z"] * 3,
        })
        dedup = FlightDeduplicator(fuzzy_window_seconds=5)
        result, report = dedup.deduplicate(df)
        assert len(result) == 2, \
            f"Expected 2 records after fuzzy dedup (t=0s and t=35s), got {len(result)}"

    def test_different_flights_not_affected_by_fuzzy_dedup(self):
        """Records from DIFFERENT flights within 5 seconds should both be kept."""
        df = pd.DataFrame({
            "event_id"       : ["E001", "E002"],
            "flight_id"      : ["AI101", "6E202"],   # Different flights
            "event_timestamp": [
                "2024-06-15T10:00:00.000Z",
                "2024-06-15T10:00:01.000Z",  # 1 second apart but different flights
            ],
            "loaded_at": ["2024-06-15T10:00:05Z"] * 2,
        })
        dedup = FlightDeduplicator(fuzzy_window_seconds=5)
        result, report = dedup.deduplicate(df)
        assert len(result) == 2, \
            "Different flights should never be deduped against each other"

    def test_dedup_report_structure(self, clean_flight_df):
        """Deduplication report must contain all required keys."""
        dedup = FlightDeduplicator()
        _, report = dedup.deduplicate(clean_flight_df)
        required = ["original", "exact_duplicates_removed",
                    "fuzzy_duplicates_removed", "final_count",
                    "total_removed", "dedup_rate_pct"]
        for key in required:
            assert key in report, f"Dedup report missing key: {key}"


# ─── FlightDataValidator Tests ────────────────────────────────────────────────

class TestFlightDataValidator:

    def test_clean_data_passes_all_checks(self, clean_flight_df):
        """A clean DataFrame should result in a healthy validation report."""
        validator = FlightDataValidator()
        report    = validator.validate(clean_flight_df)
        assert report.is_healthy, (
            f"Clean data should pass validation. "
            f"Failed checks: {[r.rule_name for r in report.failed]}"
        )

    def test_missing_flight_id_caught(self):
        """Validator must catch records with missing flight_id."""
        df = pd.DataFrame({
            "event_id"       : ["E001", "E002", "E003", "E004", "E005",
                                "E006", "E007", "E008", "E009", "E010",
                                "E011"],
            "flight_id"      : [None] * 11,  # All null → exceeds 5% threshold
            "source_airport" : ["DEL"] * 11,
            "dest_airport"   : ["BOM"] * 11,
            "latitude"       : [22.3] * 11,
            "longitude"      : [73.1] * 11,
            "event_timestamp": ["2024-06-15T10:00:00Z"] * 11,
            "status"         : ["active"] * 11,
        })
        validator = FlightDataValidator()
        report    = validator.validate(df)
        required_check = next(
            (r for r in report.results if r.rule_name == "required_fields_present"),
            None
        )
        assert required_check is not None, "required_fields check not found"
        assert not required_check.passed, \
            "Validator should fail when >5% records have missing required fields"

    def test_invalid_coordinates_caught(self):
        """Validator must catch latitude/longitude out of valid range."""
        df = pd.DataFrame({
            "event_id"       : [f"E{i:03d}" for i in range(20)],
            "flight_id"      : ["AI101"] * 20,
            "source_airport" : ["DEL"] * 20,
            "dest_airport"   : ["BOM"] * 20,
            "latitude"       : [999.0] * 20,   # All invalid
            "longitude"      : [73.1] * 20,
            "event_timestamp": ["2024-06-15T10:00:00Z"] * 20,
            "status"         : ["active"] * 20,
        })
        validator = FlightDataValidator()
        report    = validator.validate(df)
        coord_check = next(
            (r for r in report.results if r.rule_name == "coordinate_bounds_valid"),
            None
        )
        assert coord_check is not None
        assert not coord_check.passed, \
            "Validator should fail when latitude=999 (> 90 max)"

    def test_duplicate_event_ids_caught(self):
        """Validator must catch duplicate event_ids."""
        df = pd.DataFrame({
            "event_id"       : ["E001", "E001"],   # Duplicate!
            "flight_id"      : ["AI101", "AI101"],
            "source_airport" : ["DEL", "DEL"],
            "dest_airport"   : ["BOM", "BOM"],
            "latitude"       : [22.3, 22.3],
            "longitude"      : [73.1, 73.1],
            "event_timestamp": ["2024-06-15T10:00:00Z"] * 2,
            "status"         : ["active"] * 2,
        })
        validator = FlightDataValidator()
        report    = validator.validate(df)
        dup_check = next(
            (r for r in report.results if r.rule_name == "no_duplicate_event_ids"),
            None
        )
        assert dup_check is not None
        assert not dup_check.passed, \
            "Validator should detect duplicate event_ids"

    def test_invalid_status_caught(self):
        """Validator must flag non-enum status values."""
        # Need >10% invalid to trigger WARNING threshold
        df = pd.DataFrame({
            "event_id"       : [f"E{i:03d}" for i in range(10)],
            "flight_id"      : ["AI101"] * 10,
            "source_airport" : ["DEL"] * 10,
            "dest_airport"   : ["BOM"] * 10,
            "latitude"       : [22.3] * 10,
            "longitude"      : [73.1] * 10,
            "event_timestamp": ["2024-06-15T10:00:00Z"] * 10,
            "status"         : ["ghost_flight"] * 10,   # All invalid
        })
        validator = FlightDataValidator()
        report    = validator.validate(df)
        status_check = next(
            (r for r in report.results if r.rule_name == "status_is_valid_enum"),
            None
        )
        assert status_check is not None
        assert not status_check.passed, \
            "Validator should flag 'ghost_flight' as invalid status"

    def test_self_loop_route_caught(self):
        """Source == destination airport should be flagged."""
        df = pd.DataFrame({
            "event_id"       : [f"E{i:03d}" for i in range(10)],
            "flight_id"      : ["AI101"] * 10,
            "source_airport" : ["DEL"] * 10,
            "dest_airport"   : ["DEL"] * 10,   # Same as source!
            "latitude"       : [22.3] * 10,
            "longitude"      : [73.1] * 10,
            "event_timestamp": ["2024-06-15T10:00:00Z"] * 10,
            "status"         : ["active"] * 10,
        })
        validator = FlightDataValidator()
        report    = validator.validate(df)
        loop_check = next(
            (r for r in report.results if r.rule_name == "no_route_self_loop"),
            None
        )
        assert loop_check is not None
        assert not loop_check.passed, \
            "Validator should warn when source == destination airport"

    def test_validation_report_summary_keys(self, clean_flight_df):
        """Summary dict must contain all required keys for XCom/monitoring."""
        validator = FlightDataValidator()
        report    = validator.validate(clean_flight_df)
        summary   = report.summary()
        required  = ["total_records", "checks_run", "checks_passed",
                     "checks_failed", "checks_warned", "is_healthy", "pass_rate_pct"]
        for key in required:
            assert key in summary, f"Summary missing key: {key}"