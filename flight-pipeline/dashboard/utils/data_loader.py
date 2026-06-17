# dashboard/utils/data_loader.py
"""
Snowflake Data Loader for Streamlit Dashboard
==============================================
Fetches data from all 10 analytics views and returns DataFrames.

KEY STREAMLIT CONCEPT — st.cache_data:
  Without caching: every chart re-queries Snowflake on every user interaction.
  With caching:    the query runs once, result is stored in memory.
                   Next 60 seconds = instant load from cache.
                   After 60 seconds = fresh query from Snowflake.

  This reduces Snowflake credit usage by ~95% for a typical dashboard.
  In production, cache TTL is tuned to match your data refresh cadence.
  Our analytics DAG runs every 30 min → TTL=30*60=1800 makes sense.
  For the live map (refreshes every 60s) → TTL=60.

USAGE:
  from dashboard.utils.data_loader import DataLoader
  loader = DataLoader()
  df_airlines = loader.get_airline_league_table()
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)

# How long to cache each query result (seconds)
# Lower TTL = fresher data but more Snowflake queries
CACHE_TTL = {
    "live"      : 60,      # Live map: refresh every 60 seconds
    "analytics" : 300,     # Charts: refresh every 5 minutes
    "health"    : 30,      # Pipeline health: refresh every 30 seconds
    "static"    : 3600,    # Reference data: refresh every hour
}


def _get_snowflake_connection():
    """Create a Snowflake connection using environment variables."""
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = os.getenv("SNOWFLAKE_DATABASE", "FLIGHT_DB"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "FLIGHT_WH"),
        role      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


# dashboard/utils/data_loader.py
# Replace the _query_snowflake function with this safer version:

def _query_snowflake(sql: str, params: tuple = ()) -> pd.DataFrame:
    """
    Execute SQL on Snowflake and return as DataFrame.
    Returns empty DataFrame (not an error) when table has no rows.
    """
    try:
        conn    = _get_snowflake_connection()
        cursor  = conn.cursor()
        cursor.execute(sql, params)

        if cursor.description is None:
            cursor.close()
            conn.close()
            return pd.DataFrame()

        cols    = [desc[0].lower() for desc in cursor.description]
        rows    = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            # Return empty DataFrame with correct columns — no error shown
            return pd.DataFrame(columns=cols)

        df = pd.DataFrame(rows, columns=cols)
        return df

    except Exception as e:
        error_msg = str(e)
        # Only show error to user if it's not just "no rows"
        if "does not exist" in error_msg or "not authorized" in error_msg:
            st.error(
                f"⚠️ View not found: {error_msg[:120]}\n\n"
                f"**Fix:** Run the view creation script in Snowflake. "
                f"See `snowflake/queries/06_business_analytics.sql`"
            )
        else:
            st.warning(f"Query returned no data: {error_msg[:120]}")
        logger.error("Snowflake query failed: %s", e)
        return pd.DataFrame()

# ─── Cached data fetchers ─────────────────────────────────────────────────────
# Each function is decorated with @st.cache_data(ttl=N)
# The ttl argument tells Streamlit how many seconds to keep the cached result.
# When TTL expires, Streamlit re-runs the function on the next call.
@st.cache_data(ttl=CACHE_TTL["live"], show_spinner=False)
def get_active_flights() -> pd.DataFrame:
    """Active airlines - relaxed time filter."""
    return _query_snowflake("""
        SELECT
            f.airline_iata,
            COALESCE(da.airline_name, f.airline_iata) AS airline_name,
            COUNT(DISTINCT f.flight_id)               AS active_flights,
            ROUND(AVG(f.altitude), 0)                 AS avg_altitude_ft,
            ROUND(AVG(f.speed), 1)                    AS avg_speed_kmh,
            ROUND(AVG(f.delay_minutes), 1)            AS avg_delay_minutes,
            ROUND(
                SUM(IFF(f.delay_bucket != 'on_time', 1, 0))::FLOAT
                / NULLIF(COUNT(*), 0) * 100, 1
            )                                         AS pct_delayed,
            MAX(f.event_timestamp)                    AS last_seen_at
        FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS f
        LEFT JOIN FLIGHT_DB.ANALYTICS.DIM_AIRLINES da
               ON f.airline_iata = da.airline_code
        WHERE f.event_date = CURRENT_DATE()
          AND f.data_quality_flag = FALSE
        GROUP BY f.airline_iata, da.airline_name
        HAVING COUNT(DISTINCT f.flight_id) >= 1
        ORDER BY active_flights DESC
        LIMIT 20
    """)
@st.cache_data(ttl=CACHE_TTL["live"], show_spinner=False)
def get_live_flight_positions() -> pd.DataFrame:
    """Fetch flight positions for live map."""
    return _query_snowflake("""
        SELECT
            flight_id,
            airline_iata,
            source_airport,
            dest_airport,
            COALESCE(route_key, source_airport || '→' || dest_airport) AS route_key,
            latitude,
            longitude,
            altitude,
            speed,
            status,
            delay_minutes,
            COALESCE(delay_bucket, 'on_time')   AS delay_bucket,
            COALESCE(flight_phase, 'cruise')     AS flight_phase,
            is_international,
            event_timestamp
        FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
        WHERE event_date = CURRENT_DATE()
          AND data_quality_flag = FALSE
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude != 0
          AND longitude != 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY flight_id
            ORDER BY event_timestamp DESC
        ) = 1
        ORDER BY event_timestamp DESC
        LIMIT 500
    """)
@st.cache_data(ttl=CACHE_TTL["analytics"], show_spinner=False)
def get_airline_league_table() -> pd.DataFrame:
    """Airline performance rankings. Refreshes every 5 minutes."""
    return _query_snowflake("""
        SELECT
            otp_rank,
            airline_iata,
            airline_name,
            total_unique_flights,
            otp_rate_pct,
            avg_delay_minutes,
            median_delay_minutes,
            worst_single_delay,
            pct_minor_delay,
            pct_moderate_delay,
            pct_major_delay,
            pct_severe_delay,
            avg_speed_kmh,
            pct_international_flights,
            composite_score,
            performance_quartile
        FROM FLIGHT_DB.ANALYTICS.V_AIRLINE_LEAGUE_TABLE
        ORDER BY otp_rank
    """)


@st.cache_data(ttl=CACHE_TTL["analytics"], show_spinner=False)
def get_delay_patterns() -> pd.DataFrame:
    """Hourly delay patterns for heatmap."""
    return _query_snowflake("""
        SELECT
            event_date,
            event_hour,
            airline_iata,
            avg_delay,
            otp_rate_pct,
            on_time,
            minor_delays,
            moderate_delays,
            major_delays,
            severe_delays,
            delay_signal
        FROM FLIGHT_DB.ANALYTICS.V_DELAY_PATTERNS
        WHERE event_date >= DATEADD(day, -7, CURRENT_DATE())
        ORDER BY event_date, event_hour
    """)


@st.cache_data(ttl=CACHE_TTL["analytics"], show_spinner=False)
def get_peak_hours() -> pd.DataFrame:
    """Hourly traffic distribution for area chart."""
    return _query_snowflake("""
        SELECT
            event_hour,
            hour_label,
            total_flights,
            pct_of_daily_traffic,
            avg_delay_minutes,
            active_flights,
            landings,
            traffic_band,
            traffic_rank
        FROM FLIGHT_DB.ANALYTICS.V_PEAK_TRAFFIC_HOURS
        ORDER BY event_hour
    """)


@st.cache_data(ttl=CACHE_TTL["analytics"], show_spinner=False)
def get_route_efficiency() -> pd.DataFrame:
    """Route efficiency data for scatter plot."""
    return _query_snowflake("""
        SELECT
            route_key,
            source_airport,
            dest_airport,
            is_international,
            total_flights,
            otp_rate_pct,
            avg_delay_minutes,
            avg_speed_kmh,
            cancellation_rate_pct,
            efficiency_score,
            route_health
        FROM FLIGHT_DB.ANALYTICS.V_ROUTE_EFFICIENCY
        ORDER BY efficiency_score DESC
        LIMIT 100
    """)


@st.cache_data(ttl=CACHE_TTL["analytics"], show_spinner=False)
def get_weekly_trends() -> pd.DataFrame:
    """Week-over-week trend data for line chart."""
    return _query_snowflake("""
        SELECT
            event_date,
            daily_flights,
            otp_rate_pct,
            avg_delay,
            cancellations,
            otp_last_week,
            otp_pp_change,
            delay_wow_pct,
            otp_7day_rolling_avg,
            otp_trend
        FROM FLIGHT_DB.ANALYTICS.V_WEEKLY_TRENDS
        WHERE event_date >= DATEADD(day, -30, CURRENT_DATE())
        ORDER BY event_date
    """)


@st.cache_data(ttl=CACHE_TTL["analytics"], show_spinner=False)
def get_airport_congestion() -> pd.DataFrame:
    """Airport congestion data for bar chart."""
    return _query_snowflake("""
        SELECT
            airport_code,
            city,
            total_movements,
            departures,
            arrivals,
            avg_delay_minutes,
            delay_rate_pct,
            congestion_rank
        FROM FLIGHT_DB.ANALYTICS.V_AIRPORT_CONGESTION
        ORDER BY congestion_rank
        LIMIT 20
    """)


@st.cache_data(ttl=CACHE_TTL["health"], show_spinner=False)
def get_pipeline_health() -> dict:
    """Pipeline health scorecard. Refreshes every 30 seconds."""

    df = _query_snowflake("""
        SELECT
            overall_health,
            raw_records_today,
            clean_records_today,
            analytics_records_today,
            raw_freshness,
            clean_freshness,
            analytics_freshness,
            raw_lag_minutes,
            clean_lag_minutes,
            clean_record_pct,
            flagged_records_today,
            raw_to_analytics_pct,
            report_generated_at
        FROM FLIGHT_DB.ANALYTICS.V_PIPELINE_HEALTH
    """)

    if df.empty:
        return {}

    kpis = df.iloc[0].to_dict()

    # Active Flights KPI
    active_df = _query_snowflake("""
        SELECT COUNT(DISTINCT flight_id) AS total_active
        FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS
        WHERE event_date = CURRENT_DATE()
          AND data_quality_flag = FALSE
    """)

    kpis["active_flights_now"] = (
        int(active_df.iloc[0]["total_active"])
        if not active_df.empty
        else 0
    )

    return kpis
@st.cache_data(ttl=CACHE_TTL["health"], show_spinner=False)
def get_run_metrics_history() -> pd.DataFrame:
    """Last 50 pipeline run metrics for history chart."""
    return _query_snowflake("""
        SELECT
            run_id,
            dag_id,
            started_at,
            finished_at,
            records_read,
            records_written,
            elapsed_seconds,
            throughput_rps,
            error_rate_pct,
            status
        FROM FLIGHT_DB.RAW.PIPELINE_RUN_METRICS
        ORDER BY started_at DESC
        LIMIT 50
    """)


@st.cache_data(ttl=CACHE_TTL["analytics"], show_spinner=False)
def get_intl_vs_domestic() -> pd.DataFrame:
    """International vs domestic split."""
    return _query_snowflake("""
        SELECT
            segment,
            total_flights,
            traffic_share_pct,
            avg_delay_minutes,
            avg_otp_rate_pct,
            cancellation_rate_pct,
            unique_routes
        FROM FLIGHT_DB.ANALYTICS.V_INTL_VS_DOMESTIC
    """)