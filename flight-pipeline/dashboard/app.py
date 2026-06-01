# dashboard/app.py
"""
Flight Pipeline Dashboard — Main Entry Point
=============================================
Run with: streamlit run dashboard/app.py

This file configures the app, sets page layout, and
renders the sidebar navigation + KPI header that appears
on every page.

STREAMLIT MULTIPAGE APPS:
  Files in dashboard/pages/ are automatically detected.
  Each file = one page in the sidebar navigation.
  The filename determines the page order (1_, 2_, 3_).
  Underscores in filenames become spaces in the nav menu.

AUTO-REFRESH:
  streamlit-autorefresh sends a browser-level refresh signal
  every N milliseconds without reloading the entire page.
  Combined with st.cache_data(ttl=60), only expired queries
  re-run — cached results stay fast.
"""

import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Must be the FIRST Streamlit command in the script
st.set_page_config(
    page_title      = "✈ Flight Pipeline Dashboard",
    page_icon       = "✈",
    layout          = "wide",           # Use full browser width
    initial_sidebar_state = "expanded",
)

# Auto-refresh every 60 seconds (60,000 milliseconds)
# Returns the number of times it has refreshed (useful for debugging)
refresh_count = st_autorefresh(interval=60_000, key="dashboard_refresh")

# ── Global CSS ────────────────────────────────────────────────────────────────
# Inject custom CSS to polish the dashboard beyond default Streamlit styling
st.markdown("""
<style>
/* Hide Streamlit's default menu and footer in production */
#MainMenu  {visibility: hidden;}
footer     {visibility: hidden;}

/* Make metric cards look like proper KPI tiles */
[data-testid="stMetric"] {
    background    : #1e293b;
    border        : 1px solid #334155;
    border-radius : 10px;
    padding       : 14px 18px;
}

/* Colour the metric delta green/red */
[data-testid="stMetricDelta"] svg {
    display: none;  /* Hide default arrow icon — we use our own */
}

/* Tab styling */
.stTabs [data-baseweb="tab"] {
    font-weight : 600;
    font-size   : 13px;
}

/* Sidebar header */
.sidebar-header {
    font-size   : 11px;
    font-weight : 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color       : #64748b;
    margin      : 16px 0 6px;
}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/"
        "Boeing_787_dreamliner_logo.svg/200px-Boeing_787_dreamliner_logo.svg.png",
        width=60,
    )
    st.title("✈ Flight Pipeline")
    st.caption("Real-time aviation analytics")
    st.divider()

    # Refresh status
    st.markdown('<div class="sidebar-header">Auto-refresh</div>', unsafe_allow_html=True)
    st.caption(f"🔄 Refresh #{refresh_count} • Every 60 seconds")

    st.divider()

    # Global airline filter (used by pages via session state)
    from utils.data_loader import get_airline_league_table
    airlines_df = get_airline_league_table()
    airline_options = ["All Airlines"]
    if not airlines_df.empty and "airline_name" in airlines_df.columns:
        airline_options += airlines_df["airline_name"].dropna().tolist()

    st.markdown('<div class="sidebar-header">Filters</div>', unsafe_allow_html=True)
    selected_airline = st.selectbox(
        "Airline",
        options = airline_options,
        key     = "global_airline_filter",
    )
    selected_route_type = st.radio(
        "Route Type",
        options    = ["All", "Domestic", "International"],
        horizontal = True,
        key        = "global_route_filter",
    )

    st.divider()
    st.markdown('<div class="sidebar-header">Navigation</div>', unsafe_allow_html=True)
    st.page_link("pages/1_Live_Map.py",        label="🗺️ Live Flight Map",    icon="🗺️")
    st.page_link("pages/2_Analytics.py",        label="📊 Analytics",           icon="📊")
    st.page_link("pages/3_Pipeline_Health.py",  label="⚙️ Pipeline Health",    icon="⚙️")

    st.divider()
    st.caption("Built with Kafka · Airflow · Snowflake · Streamlit")

# ── Home page content ─────────────────────────────────────────────────────────
st.title("✈ Flight Data Pipeline Dashboard")
st.caption("Powered by Kafka → Airflow → Snowflake → Streamlit")

# Global KPI header
from utils.data_loader import get_active_flights, get_pipeline_health
from utils.formatters  import fmt_number, fmt_pct, fmt_minutes
from components.kpi_cards import render_kpi_row, render_health_badge

col_health, col_spacer = st.columns([3, 1])
with col_health:
    health = get_pipeline_health()
    overall = health.get("overall_health", "UNKNOWN")
    render_health_badge(overall, label="Pipeline Status")

st.divider()

# Top KPI row
flights_df   = get_active_flights()
total_active = int(flights_df["active_flights"].sum()) if not flights_df.empty else 0
avg_otp      = float(flights_df.get("avg_delay_minutes", [0]).mean()) if not flights_df.empty else 0

raw_records  = health.get("raw_records_today", 0) or 0
clean_pct    = health.get("clean_record_pct", 0)  or 0
raw_lag      = int(health.get("raw_lag_minutes", 0) or 0)

render_kpi_row([
    {
        "label": "✈ Active Flights Now",
        "value": fmt_number(total_active),
        "help" : "Flights with a position update in the last 15 minutes",
    },
    {
        "label": "📦 Records Ingested Today",
        "value": fmt_number(raw_records),
        "help" : "Total records loaded into Snowflake RAW layer today",
    },
    {
        "label": "✅ Clean Record Rate",
        "value": fmt_pct(clean_pct),
        "help" : "Percentage of records passing all quality checks",
    },
    {
        "label": "⏱️ Pipeline Lag",
        "value": f"{raw_lag}m",
        "delta": "Fresh" if raw_lag < 15 else f"{raw_lag}m behind",
        "delta_color": "normal" if raw_lag < 15 else "inverse",
        "help" : "Minutes since last record landed in RAW layer",
    },
])

st.divider()
st.info(
    "👈 Use the sidebar to navigate between pages: "
    "**Live Map** for real-time flight positions, "
    "**Analytics** for performance insights, "
    "**Pipeline Health** for engineering metrics."
)