# transformation/validate.py
"""
Data Quality Validation
========================
Validates flight records against business rules and schema constraints.

VALIDATION vs TRANSFORMATION:
  Transformation CHANGES data (imputation, capping, normalisation).
  Validation CHECKS data and REPORTS results — it never changes values.

  Run validation BEFORE transformation to see the raw quality.
  Run validation AFTER transformation to confirm cleaning worked.

DESIGN PATTERN — Results object:
  Every check returns a ValidationResult with:
    - passed: bool
    - rule_name: str (machine-readable)
    - message: str (human-readable, for alerts/dashboards)
    - affected_count: int (how many records failed this check)
    - affected_pct: float (0.0–1.0)
    - sample_failures: list (first 5 bad records for debugging)
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    rule_name       : str
    passed          : bool
    message         : str
    affected_count  : int   = 0
    affected_pct    : float = 0.0
    severity        : str   = "error"    # "error", "warning", "info"
    sample_failures : list  = field(default_factory=list)

    def __str__(self):
        status = "✓ PASS" if self.passed else ("⚠ WARN" if self.severity == "warning" else "✗ FAIL")
        return (
            f"{status} | {self.rule_name:<40} | "
            f"affected={self.affected_count} ({self.affected_pct*100:.1f}%)"
        )


@dataclass
class ValidationReport:
    """Collection of all validation results for one run."""
    results         : list[ValidationResult] = field(default_factory=list)
    total_records   : int = 0

    @property
    def passed(self) -> list[ValidationResult]:
        return [r for r in self.results if r.passed]

    @property
    def failed(self) -> list[ValidationResult]:
        return [r for r in self.results if not r.passed and r.severity == "error"]

    @property
    def warnings(self) -> list[ValidationResult]:
        return [r for r in self.results if not r.passed and r.severity == "warning"]

    @property
    def is_healthy(self) -> bool:
        """Pipeline is healthy if no ERROR-severity checks fail."""
        return len(self.failed) == 0

    @property
    def pass_rate(self) -> float:
        return len(self.passed) / max(len(self.results), 1)

    def summary(self) -> dict:
        return {
            "total_records" : self.total_records,
            "checks_run"    : len(self.results),
            "checks_passed" : len(self.passed),
            "checks_failed" : len(self.failed),
            "checks_warned" : len(self.warnings),
            "is_healthy"    : self.is_healthy,
            "pass_rate_pct" : round(self.pass_rate * 100, 2),
        }

    def print_report(self) -> None:
        print(f"\n{'='*70}")
        print(f"VALIDATION REPORT | records={self.total_records} | {self.summary()}")
        print(f"{'='*70}")
        for result in self.results:
            print(result)
            if not result.passed and result.sample_failures:
                print(f"   Sample failures: {result.sample_failures[:3]}")
        print(f"{'='*70}\n")


class FlightDataValidator:
    """
    Validates a DataFrame of flight records against all business rules.
    Returns a ValidationReport — never modifies the input DataFrame.
    """

    # Maximum acceptable failure rate for each severity level
    ERROR_THRESHOLD   = 0.05   # >5% failures = ERROR
    WARNING_THRESHOLD = 0.10   # >10% failures = WARNING

    def validate(self, df: pd.DataFrame) -> ValidationReport:
        """Run all validation checks and return comprehensive report."""
        report = ValidationReport(total_records=len(df))

        if df.empty:
            logger.warning("Validator received empty DataFrame")
            return report

        # Run all check methods — each returns a ValidationResult
        checks = [
            self._check_required_fields(df),
            self._check_coordinate_bounds(df),
            self._check_altitude_range(df),
            self._check_speed_range(df),
            self._check_status_enum(df),
            self._check_timestamp_freshness(df),
            self._check_airport_code_format(df),
            self._check_duplicate_event_ids(df),
            self._check_route_self_loop(df),
            self._check_speed_altitude_consistency(df),
        ]

        report.results = checks

        summary = report.summary()
        logger.info("Validation complete | %s", summary)

        if not report.is_healthy:
            failed_names = [r.rule_name for r in report.failed]
            logger.warning("Validation FAILED checks: %s", failed_names)

        return report

    # ─── Individual checks ────────────────────────────────────────────────────

    def _check_required_fields(self, df: pd.DataFrame) -> ValidationResult:
        """All critical fields must be non-null."""
        required = ["flight_id", "source_airport", "dest_airport",
                    "latitude", "longitude", "event_timestamp"]
        missing_any = pd.Series(False, index=df.index)
        for col in required:
            if col in df.columns:
                missing_any = missing_any | df[col].isna()

        count = int(missing_any.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "required_fields_present",
            passed         = pct <= self.ERROR_THRESHOLD,
            message        = f"{count} records missing at least one required field",
            affected_count = count,
            affected_pct   = pct,
            severity       = "error",
            sample_failures= df[missing_any]["flight_id"].head(5).tolist(),
        )

    def _check_coordinate_bounds(self, df: pd.DataFrame) -> ValidationResult:
        """Latitude must be -90 to 90, longitude -180 to 180."""
        if not all(c in df.columns for c in ["latitude", "longitude"]):
            return ValidationResult("coordinate_bounds", True, "columns not present")

        lat = pd.to_numeric(df["latitude"], errors="coerce")
        lon = pd.to_numeric(df["longitude"], errors="coerce")
        bad = (
            lat.isna() | lon.isna() |
            ~lat.between(-90, 90) | ~lon.between(-180, 180)
        )
        count = int(bad.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "coordinate_bounds_valid",
            passed         = pct <= self.ERROR_THRESHOLD,
            message        = f"{count} records have invalid lat/lon coordinates",
            affected_count = count,
            affected_pct   = pct,
            severity       = "error",
            sample_failures= df[bad][["flight_id","latitude","longitude"]].head(3).to_dict("records"),
        )

    def _check_altitude_range(self, df: pd.DataFrame) -> ValidationResult:
        if "altitude" not in df.columns:
            return ValidationResult("altitude_range", True, "column not present")
        alt = pd.to_numeric(df["altitude"], errors="coerce")
        bad = alt.notna() & ~alt.between(0, 60000)
        count = int(bad.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "altitude_in_valid_range",
            passed         = pct <= self.WARNING_THRESHOLD,
            message        = f"{count} records have altitude outside 0–60,000 ft",
            affected_count = count,
            affected_pct   = pct,
            severity       = "warning",
        )

    def _check_speed_range(self, df: pd.DataFrame) -> ValidationResult:
        if "speed" not in df.columns:
            return ValidationResult("speed_range", True, "column not present")
        spd = pd.to_numeric(df["speed"], errors="coerce")
        bad = spd.notna() & ~spd.between(0, 1200)
        count = int(bad.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "speed_in_valid_range",
            passed         = pct <= self.WARNING_THRESHOLD,
            message        = f"{count} records have speed outside 0–1200 km/h",
            affected_count = count,
            affected_pct   = pct,
            severity       = "warning",
        )

    def _check_status_enum(self, df: pd.DataFrame) -> ValidationResult:
        valid = {"active","scheduled","landed","cancelled","diverted","unknown"}
        if "status" not in df.columns:
            return ValidationResult("status_enum", True, "column not present")
        bad = ~df["status"].isin(valid)
        count = int(bad.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "status_is_valid_enum",
            passed         = pct <= self.ERROR_THRESHOLD,
            message        = f"{count} records have unrecognised status value",
            affected_count = count,
            affected_pct   = pct,
            severity       = "error",
            sample_failures= df[bad]["status"].unique().tolist()[:5],
        )

    def _check_timestamp_freshness(self, df: pd.DataFrame) -> ValidationResult:
        """event_timestamp should not be in the future or more than 24h old."""
        if "event_timestamp" not in df.columns:
            return ValidationResult("timestamp_freshness", True, "column not present")
        from datetime import datetime, timezone, timedelta
        now   = pd.Timestamp.now(tz="UTC")
        cutoff= now - pd.Timedelta(hours=24)
        ts    = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
        bad   = ts.isna() | (ts > now) | (ts < cutoff)
        count = int(bad.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "timestamp_is_fresh",
            passed         = pct <= self.WARNING_THRESHOLD,
            message        = f"{count} records have stale or future timestamps",
            affected_count = count,
            affected_pct   = pct,
            severity       = "warning",
        )

    def _check_airport_code_format(self, df: pd.DataFrame) -> ValidationResult:
        """Airport codes must be 2–4 uppercase letters."""
        import re
        pattern = re.compile(r"^[A-Z]{2,4}$")
        for col in ["source_airport", "dest_airport"]:
            if col not in df.columns:
                continue
            bad = ~df[col].astype(str).str.match(pattern) | df[col].isna()
            count = int(bad.sum())
            pct   = count / max(len(df), 1)
            if pct > self.ERROR_THRESHOLD:
                return ValidationResult(
                    rule_name      = "airport_code_format_valid",
                    passed         = False,
                    message        = f"{count} records in {col} have invalid airport code format",
                    affected_count = count,
                    affected_pct   = pct,
                    severity       = "error",
                )
        return ValidationResult("airport_code_format_valid", True, "All airport codes valid")

    def _check_duplicate_event_ids(self, df: pd.DataFrame) -> ValidationResult:
        if "event_id" not in df.columns:
            return ValidationResult("no_duplicate_event_ids", True, "column not present")
        dups  = df.duplicated(subset=["event_id"], keep=False)
        count = int(dups.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "no_duplicate_event_ids",
            passed         = count == 0,
            message        = f"{count} records share a duplicate event_id",
            affected_count = count,
            affected_pct   = pct,
            severity       = "error",
            sample_failures= df[dups]["event_id"].head(5).tolist(),
        )

    def _check_route_self_loop(self, df: pd.DataFrame) -> ValidationResult:
        """Source and destination airports must be different."""
        if not all(c in df.columns for c in ["source_airport","dest_airport"]):
            return ValidationResult("no_route_self_loop", True, "columns not present")
        loop = df["source_airport"] == df["dest_airport"]
        count = int(loop.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "no_route_self_loop",
            passed         = pct <= self.WARNING_THRESHOLD,
            message        = f"{count} records have source == destination airport",
            affected_count = count,
            affected_pct   = pct,
            severity       = "warning",
            sample_failures= df[loop]["source_airport"].head(5).tolist(),
        )

    def _check_speed_altitude_consistency(self, df: pd.DataFrame) -> ValidationResult:
        """Flag aircraft showing cruise speed while on the ground."""
        if not all(c in df.columns for c in ["speed","altitude"]):
            return ValidationResult("speed_altitude_consistent", True, "columns not present")
        spd = pd.to_numeric(df["speed"],    errors="coerce").fillna(0)
        alt = pd.to_numeric(df["altitude"], errors="coerce").fillna(0)
        bad = (alt < 500) & (spd > 400)
        count = int(bad.sum())
        pct   = count / max(len(df), 1)
        return ValidationResult(
            rule_name      = "speed_altitude_consistent",
            passed         = pct <= self.WARNING_THRESHOLD,
            message        = f"{count} records show high speed at ground level",
            affected_count = count,
            affected_pct   = pct,
            severity       = "warning",
        )