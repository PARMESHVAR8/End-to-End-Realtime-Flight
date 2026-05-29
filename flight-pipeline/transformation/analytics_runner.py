# transformation/analytics_runner.py
"""
Analytics Runner
================
Executes all 10 business analytics queries and returns
structured results for the Airflow DAG and Streamlit dashboard.

Can be run standalone for debugging:
  python -m transformation.analytics_runner --query all
  python -m transformation.analytics_runner --query airline_league
"""

import logging
import argparse
import json
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from snowflake.connection import SnowflakeConnection
from monitoring.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# ── Query registry ────────────────────────────────────────────────────────────
# Maps a friendly name to the view/query to run.
# Adding a new analytics query = add one entry here.
ANALYTICS_QUERIES = {
    "active_airlines_now" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_ACTIVE_AIRLINES_NOW",
        "description": "Most active airlines right now",
        "team"       : "Operations",
        "limit"      : 20,
    },
    "delay_patterns" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_DELAY_PATTERNS",
        "description": "Hourly delay patterns by airline",
        "team"       : "Operations",
        "limit"      : 200,
    },
    "airline_league" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_AIRLINE_LEAGUE_TABLE",
        "description": "Airline performance league table",
        "team"       : "Management",
        "limit"      : 50,
    },
    "peak_hours" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_PEAK_TRAFFIC_HOURS",
        "description": "Peak traffic hours analysis",
        "team"       : "Infrastructure",
        "limit"      : 24,
    },
    "airport_congestion" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_AIRPORT_CONGESTION",
        "description": "Airport congestion and delay ranking",
        "team"       : "Ground Operations",
        "limit"      : 30,
    },
    "route_efficiency" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_ROUTE_EFFICIENCY",
        "description": "Route efficiency and health ranking",
        "team"       : "Network Planning",
        "limit"      : 100,
    },
    "aircraft_performance" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_AIRCRAFT_PERFORMANCE",
        "description": "Aircraft type performance analysis",
        "team"       : "Fleet Planning",
        "limit"      : 30,
    },
    "weekly_trends" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_WEEKLY_TRENDS",
        "description": "Week-over-week performance trends",
        "team"       : "Executive",
        "limit"      : 30,
    },
    "intl_vs_domestic" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_INTL_VS_DOMESTIC",
        "description": "International vs domestic traffic split",
        "team"       : "Revenue Management",
        "limit"      : 10,
    },
    "pipeline_health" : {
        "view"       : "FLIGHT_DB.ANALYTICS.V_PIPELINE_HEALTH",
        "description": "Data pipeline health scorecard",
        "team"       : "Data Engineering",
        "limit"      : 1,
    },
}


class AnalyticsRunner:
    """
    Executes analytics queries and returns structured results.
    Designed to be called by Airflow DAG tasks and Streamlit dashboard.
    """

    def __init__(self):
        self.results : dict = {}
        self.errors  : dict = {}

    def run_query(
        self,
        query_name : str,
        limit      : Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Execute one named analytics query and return as DataFrame.

        Args:
            query_name : Key from ANALYTICS_QUERIES registry
            limit      : Override default row limit

        Returns:
            DataFrame with query results. Empty DataFrame on error.
        """
        if query_name not in ANALYTICS_QUERIES:
            raise ValueError(
                f"Unknown query '{query_name}'. "
                f"Available: {list(ANALYTICS_QUERIES.keys())}"
            )

        config     = ANALYTICS_QUERIES[query_name]
        view_name  = config["view"]
        row_limit  = limit or config["limit"]

        logger.info(
            "Running analytics query | name=%s | view=%s | limit=%d",
            query_name, view_name, row_limit
        )

        try:
            with SnowflakeConnection(
                database="FLIGHT_DB",
                schema="ANALYTICS"
            ) as sf:
                results = sf.execute(
                    f"SELECT * FROM {view_name} LIMIT %s",
                    params=(row_limit,)
                )

            if not results:
                logger.warning("Query '%s' returned 0 rows", query_name)
                return pd.DataFrame()

            df = pd.DataFrame(results)
            df.columns = [c.lower() for c in df.columns]

            logger.info(
                "Query '%s' returned %d rows | cols=%d",
                query_name, len(df), len(df.columns)
            )
            return df

        except Exception as e:
            logger.error("Query '%s' failed: %s", query_name, e)
            self.errors[query_name] = str(e)
            return pd.DataFrame()

    def run_all(self) -> dict[str, pd.DataFrame]:
        """
        Run all 10 analytics queries and return dict of DataFrames.
        Used by Airflow DAG to refresh all analytics in one task.
        """
        logger.info("Running all %d analytics queries", len(ANALYTICS_QUERIES))
        start = datetime.now(timezone.utc)

        all_results = {}
        for name in ANALYTICS_QUERIES:
            df = self.run_query(name)
            all_results[name] = df
            self.results[name] = {
                "rows"   : len(df),
                "columns": list(df.columns) if not df.empty else [],
                "status" : "success" if not df.empty else "empty",
            }

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(
            "All analytics complete | elapsed=%.2fs | queries=%d | errors=%d",
            elapsed, len(ANALYTICS_QUERIES), len(self.errors)
        )
        return all_results

    def get_kpi_summary(self) -> dict:
        """
        Returns a compact KPI dict for dashboard headline cards.
        Pulls the most important number from each query.
        """
        kpis = {}

        try:
            with SnowflakeConnection(database="FLIGHT_DB", schema="ANALYTICS") as sf:

                # KPI 1: Active flights right now
                r = sf.execute("""
                    SELECT SUM(active_flights) AS total_active
                    FROM FLIGHT_DB.ANALYTICS.V_ACTIVE_AIRLINES_NOW
                """)
                kpis["active_flights_now"] = int(r[0]["TOTAL_ACTIVE"] or 0) if r else 0

                # KPI 2: Fleet-wide OTP rate today
                r = sf.execute("""
                    SELECT ROUND(AVG(otp_rate_pct), 1) AS fleet_otp
                    FROM FLIGHT_DB.ANALYTICS.V_AIRLINE_LEAGUE_TABLE
                """)
                kpis["fleet_otp_rate_pct"] = float(r[0]["FLEET_OTP"] or 0) if r else 0

                # KPI 3: Average delay right now
                r = sf.execute("""
                    SELECT ROUND(AVG(avg_delay_minutes), 1) AS avg_delay
                    FROM FLIGHT_DB.ANALYTICS.V_ACTIVE_AIRLINES_NOW
                """)
                kpis["avg_delay_minutes_now"] = float(r[0]["AVG_DELAY"] or 0) if r else 0

                # KPI 4: Most congested airport
                r = sf.execute("""
                    SELECT airport_code, city, congestion_index
                    FROM FLIGHT_DB.ANALYTICS.V_AIRPORT_CONGESTION
                    ORDER BY congestion_rank LIMIT 1
                """)
                if r:
                    kpis["most_congested_airport"] = {
                        "code"  : r[0].get("AIRPORT_CODE", "N/A"),
                        "city"  : r[0].get("CITY", "N/A"),
                        "index" : float(r[0].get("CONGESTION_INDEX", 0) or 0),
                    }

                # KPI 5: Best performing airline today
                r = sf.execute("""
                    SELECT airline_name, otp_rate_pct
                    FROM FLIGHT_DB.ANALYTICS.V_AIRLINE_LEAGUE_TABLE
                    ORDER BY otp_rank LIMIT 1
                """)
                if r:
                    kpis["best_airline"] = {
                        "name"    : r[0].get("AIRLINE_NAME", "N/A"),
                        "otp_pct" : float(r[0].get("OTP_RATE_PCT", 0) or 0),
                    }

                # KPI 6: Pipeline health
                r = sf.execute("""
                    SELECT overall_health, clean_record_pct, raw_lag_minutes
                    FROM FLIGHT_DB.ANALYTICS.V_PIPELINE_HEALTH
                """)
                if r:
                    kpis["pipeline"] = {
                        "health"           : r[0].get("OVERALL_HEALTH", "UNKNOWN"),
                        "clean_record_pct" : float(r[0].get("CLEAN_RECORD_PCT", 0) or 0),
                        "lag_minutes"      : int(r[0].get("RAW_LAG_MINUTES", 0) or 0),
                    }

        except Exception as e:
            logger.error("KPI summary failed: %s", e)
            kpis["error"] = str(e)

        kpis["generated_at"] = datetime.now(timezone.utc).isoformat()
        return kpis

    def print_summary(self) -> None:
        """Print a human-readable summary of all query results."""
        print(f"\n{'='*65}")
        print(f"  ANALYTICS RUNNER SUMMARY — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC")
        print(f"{'='*65}")
        for name, config in ANALYTICS_QUERIES.items():
            status = self.results.get(name, {})
            rows   = status.get("rows", "?")
            state  = status.get("status", "not_run")
            err    = self.errors.get(name, "")
            icon   = "✓" if state == "success" else ("○" if state == "empty" else "✗")
            print(
                f"  {icon} {name:<28} {rows:>5} rows  "
                f"[{config['team']}]"
                + (f"  ERROR: {err}" if err else "")
            )
        print(f"{'='*65}\n")


# ─── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flight Analytics Runner")
    parser.add_argument(
        "--query",
        default="all",
        choices=["all"] + list(ANALYTICS_QUERIES.keys()),
        help="Which query to run (default: all)"
    )
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Override row limit"
    )
    args = parser.parse_args()

    runner = AnalyticsRunner()

    if args.query == "all":
        results = runner.run_all()
        runner.print_summary()

        # Also print KPI summary
        print("\n── KPI SUMMARY ──────────────────────────────────────────")
        kpis = runner.get_kpi_summary()
        print(json.dumps(kpis, indent=2, default=str))

    else:
        df = runner.run_query(args.query, limit=args.limit)
        if df.empty:
            print(f"Query '{args.query}' returned no results")
        elif args.format == "json":
            print(df.to_json(orient="records", indent=2))
        elif args.format == "csv":
            print(df.to_csv(index=False))
        else:
            print(df.to_string(index=False))