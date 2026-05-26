# transformation/gx_validation.py
"""
Great Expectations Integration
================================
GX is the industry-standard Python library for data validation.
It replaces our hand-written FlightDataValidator with a declarative,
self-documenting approach.

WHY GREAT EXPECTATIONS:
  Our custom validator (Phase 6.4) works, but has limitations:
    - Rules are buried in Python code — hard for non-engineers to audit
    - No automatic HTML documentation of what we check
    - No built-in result store (we have to build our own)
    - No integration with dbt, Airflow, or other tools

  GX solves all of this:
    - Expectations are declared in JSON — readable by anyone
    - Auto-generates "Data Docs" — a website showing all checks + results
    - Native Airflow operator exists
    - Industry standard — put it on your resume

KEY CONCEPTS:
  Expectation  : One rule. "Column altitude must be between 0 and 60000."
  Suite        : A collection of Expectations for one dataset.
  Checkpoint   : Runs a Suite against a batch of data, saves results.
  Data Docs    : Auto-generated HTML site showing all results visually.

USAGE:
  from transformation.gx_validation import FlightGXValidator
  gx = FlightGXValidator()
  result = gx.validate_dataframe(df, suite_name="flights_raw_suite")
  print(result.success)  # True / False
"""

import os
import logging
import pandas as pd
from typing import Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class FlightGXValidator:
    """
    Wraps Great Expectations to validate flight DataFrames.

    Design: We use the "in-memory" (Pandas) validator approach —
    no file system or database needed. Pass a DataFrame in,
    get a validation result back. Simple and Airflow-compatible.
    """

    def __init__(self, output_dir: str = "./gx_reports"):
        """
        Args:
            output_dir: Where to write HTML Data Docs reports.
                        Set to /opt/airflow/project/gx_reports in Docker.
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def validate_raw_flights(self, df: pd.DataFrame) -> dict:
        """
        Validate a raw flights DataFrame using GX Expectations.
        Returns a summary dict compatible with our existing pipeline.
        """
        try:
            import great_expectations as gx
            from great_expectations.dataset import PandasDataset
        except ImportError:
            logger.error("great-expectations not installed. Run: pip install great-expectations")
            return {"success": False, "error": "great_expectations not installed"}

        logger.info("Running Great Expectations validation on %d records", len(df))

        # Wrap the DataFrame in a GX PandasDataset
        # This gives us the .expect_*() methods on every column
        gx_df = PandasDataset(df)

        results = []

        # ── Schema Expectations ──────────────────────────────────────────────
        # Expect these columns to exist — catches schema drift immediately
        for col in ["event_id", "flight_id", "airline", "airline_iata",
                    "source_airport", "dest_airport", "latitude", "longitude",
                    "altitude", "speed", "status", "event_timestamp"]:
            result = gx_df.expect_column_to_exist(col)
            results.append(("schema", col, result.success))

        # ── Null Rate Expectations ────────────────────────────────────────────
        # Critical columns: must be 100% populated
        for col in ["event_id", "flight_id", "event_timestamp"]:
            if col in df.columns:
                result = gx_df.expect_column_values_to_not_be_null(col)
                results.append(("not_null_critical", col, result.success))

        # Non-critical columns: allow up to 5% null
        for col in ["source_airport", "dest_airport", "latitude", "longitude"]:
            if col in df.columns:
                result = gx_df.expect_column_values_to_not_be_null(
                    col,
                    mostly=0.95   # 95% must be non-null (5% tolerance)
                )
                results.append(("not_null_95pct", col, result.success))

        # ── Value Range Expectations ──────────────────────────────────────────
        if "altitude" in df.columns:
            result = gx_df.expect_column_values_to_be_between(
                "altitude",
                min_value=0,
                max_value=60000,
                mostly=0.95
            )
            results.append(("range", "altitude", result.success))

        if "speed" in df.columns:
            result = gx_df.expect_column_values_to_be_between(
                "speed",
                min_value=0,
                max_value=1200,
                mostly=0.95
            )
            results.append(("range", "speed", result.success))

        if "latitude" in df.columns:
            result = gx_df.expect_column_values_to_be_between(
                "latitude",
                min_value=-90,
                max_value=90,
                mostly=0.99
            )
            results.append(("range", "latitude", result.success))

        if "longitude" in df.columns:
            result = gx_df.expect_column_values_to_be_between(
                "longitude",
                min_value=-180,
                max_value=180,
                mostly=0.99
            )
            results.append(("range", "longitude", result.success))

        if "delay_minutes" in df.columns:
            result = gx_df.expect_column_values_to_be_between(
                "delay_minutes",
                min_value=0,
                max_value=None,   # No upper cap
                mostly=0.99
            )
            results.append(("range", "delay_minutes", result.success))

        # ── Enum / Set Expectations ───────────────────────────────────────────
        if "status" in df.columns:
            result = gx_df.expect_column_values_to_be_in_set(
                "status",
                value_set={"active", "scheduled", "landed",
                           "cancelled", "diverted", "unknown"},
                mostly=0.95
            )
            results.append(("enum", "status", result.success))

        # ── Uniqueness Expectations ───────────────────────────────────────────
        if "event_id" in df.columns:
            result = gx_df.expect_column_values_to_be_unique("event_id")
            results.append(("unique", "event_id", result.success))

        # ── String Format Expectations ────────────────────────────────────────
        # Airport codes: 2–4 uppercase letters (IATA format)
        for col in ["source_airport", "dest_airport"]:
            if col in df.columns:
                result = gx_df.expect_column_values_to_match_regex(
                    col,
                    regex=r"^[A-Z]{2,4}$",
                    mostly=0.95
                )
                results.append(("format", col, result.success))

        # Airline IATA: exactly 2 uppercase letters
        if "airline_iata" in df.columns:
            result = gx_df.expect_column_values_to_match_regex(
                "airline_iata",
                regex=r"^[A-Z0-9]{2}$",
                mostly=0.95
            )
            results.append(("format", "airline_iata", result.success))

        # ── Statistical Expectations ──────────────────────────────────────────
        # These catch "the data looks structurally fine but something is wrong"
        # e.g., all speeds are 0 (pipeline bug) or all delays are 999

        if "speed" in df.columns and len(df) > 10:
            # Mean speed should be between 100 and 900 km/h for a realistic fleet
            result = gx_df.expect_column_mean_to_be_between(
                "speed",
                min_value=50,
                max_value=950
            )
            results.append(("statistical", "speed_mean", result.success))

        if "altitude" in df.columns and len(df) > 10:
            # At least some records should be at cruise altitude
            result = gx_df.expect_column_max_to_be_between(
                "altitude",
                min_value=10000,  # At least one flight should be above 10,000 ft
                max_value=None
            )
            results.append(("statistical", "altitude_max", result.success))

        # ── Compile results ───────────────────────────────────────────────────
        total     = len(results)
        passed    = sum(1 for _, _, s in results if s)
        failed    = total - passed
        pass_rate = passed / max(total, 1)

        # Write HTML report
        report_path = self._write_html_report(df, results)

        summary = {
            "success"         : failed == 0,
            "total_checks"    : total,
            "checks_passed"   : passed,
            "checks_failed"   : failed,
            "pass_rate_pct"   : round(pass_rate * 100, 2),
            "report_path"     : report_path,
            "validated_at"    : datetime.now(timezone.utc).isoformat(),
            "records_validated": len(df),
        }

        if failed > 0:
            failed_checks = [f"{cat}:{col}" for cat, col, s in results if not s]
            summary["failed_checks"] = failed_checks
            logger.warning(
                "GX validation FAILED | failed_checks=%s", failed_checks
            )
        else:
            logger.info(
                "GX validation PASSED | %d/%d checks | records=%d",
                passed, total, len(df)
            )

        return summary

    def _write_html_report(
        self,
        df      : pd.DataFrame,
        results : list,
    ) -> str:
        """
        Generate a simple HTML report showing validation results.
        In production, GX Data Docs gives you a much richer version of this.
        """
        timestamp  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename   = f"validation_report_{timestamp}.html"
        filepath   = os.path.join(self.output_dir, filename)

        passed_results = [(c, col) for c, col, s in results if s]
        failed_results = [(c, col) for c, col, s in results if not s]

        html = f"""<!DOCTYPE html>
<html>
<head>
  <title>Flight Data Validation Report — {timestamp}</title>
  <style>
    body {{ font-family: system-ui; max-width: 900px; margin: 40px auto; padding: 0 20px; }}
    h1   {{ color: #1e3a8a; }}
    .pass {{ color: #166534; background: #f0fdf4; padding: 6px 12px;
             border-left: 4px solid #22c55e; margin: 4px 0; border-radius: 4px; }}
    .fail {{ color: #991b1b; background: #fef2f2; padding: 6px 12px;
             border-left: 4px solid #ef4444; margin: 4px 0; border-radius: 4px; }}
    .stat {{ display: inline-block; padding: 12px 20px; border-radius: 8px;
             margin: 8px; text-align: center; font-weight: 700; }}
    .s-blue  {{ background: #dbeafe; color: #1e3a8a; }}
    .s-green {{ background: #dcfce7; color: #166534; }}
    .s-red   {{ background: #fee2e2; color: #991b1b; }}
  </style>
</head>
<body>
  <h1>✈ Flight Data Validation Report</h1>
  <p>Generated: {timestamp} UTC | Records validated: {len(df):,}</p>

  <div>
    <span class="stat s-blue">{len(results)} Total Checks</span>
    <span class="stat s-green">{len(passed_results)} Passed</span>
    <span class="stat s-red">{len(failed_results)} Failed</span>
  </div>

  <h2>{'✅ All Checks Passed' if not failed_results else '❌ Failed Checks'}</h2>
  {''.join(f'<div class="fail">✗ [{cat}] {col}</div>' for cat, col in failed_results) or '<p>None</p>'}

  <h2>✅ Passed Checks</h2>
  {''.join(f'<div class="pass">✓ [{cat}] {col}</div>' for cat, col in passed_results)}

  <h2>Dataset Profile</h2>
  <pre>{df.describe().to_string()}</pre>
</body>
</html>"""

        with open(filepath, "w") as f:
            f.write(html)

        logger.info("Validation HTML report written to %s", filepath)
        return filepath