# dashboard/pages/1_Live_Map.py
"""
Page 1 — Live Flight Map
=========================
Shows real-time flight positions using Pydeck (WebGL map).
Auto-refreshes every 60 seconds via parent app.

WHY PYDECK:
  st.map() is basic — just dots on a map.
  Pydeck = full WebGL rendering, custom layers, tooltips,
  rotation, lighting effects. Used by Uber's data teams.
  st.pydeck_chart() renders Pydeck directly in Streamlit.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import streamlit as st
import pydeck   as pdk
import pandas   as pd
import plotly.express as px

from utils.data_loader  import get_live_flight_positions, get_active_flights
from utils.formatters   import fmt_number, fmt_pct, fmt_altitude, fmt_speed
from components.kpi_cards import render_kpi_row

st.title("🗺️ Live Flight Map")
st.caption("Flight positions updated every 60 seconds · Last 15 minutes of data")

# ── Data ──────────────────────────────────────────────────────────────────────
df_positions = get_live_flight_positions()
df_airlines  = get_active_flights()

if df_positions.empty:
    st.warning(
        "No live flight data available. "
        "Make sure the Kafka producer and Airflow DAGs are running."
    )
    st.stop()

# ── KPI cards ─────────────────────────────────────────────────────────────────
total_flights  = df_positions["flight_id"].nunique()
active_flights = len(df_positions[df_positions["status"] == "active"])
avg_altitude   = df_positions["altitude"].mean()
avg_speed      = df_positions["speed"].mean()

render_kpi_row([
    {"label": "Tracked Flights",  "value": fmt_number(total_flights)},
    {"label": "Active Flights",   "value": fmt_number(active_flights)},
    {"label": "Avg Altitude",     "value": fmt_altitude(avg_altitude)},
    {"label": "Avg Speed",        "value": fmt_speed(avg_speed)},
])

st.divider()

# ── Filters ───────────────────────────────────────────────────────────────────
col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    status_filter = st.multiselect(
        "Status",
        options  = df_positions["status"].unique().tolist(),
        default  = ["active"],
    )
with col_f2:
    phase_filter = st.multiselect(
        "Flight Phase",
        options  = df_positions["flight_phase"].dropna().unique().tolist(),
        default  = df_positions["flight_phase"].dropna().unique().tolist(),
    )
with col_f3:
    intl_filter = st.radio(
        "Route",
        options    = ["All", "Domestic", "International"],
        horizontal = True,
    )

# Apply filters
df_map = df_positions.copy()
if status_filter:
    df_map = df_map[df_map["status"].isin(status_filter)]
if phase_filter:
    df_map = df_map[df_map["flight_phase"].isin(phase_filter)]
if intl_filter == "International":
    df_map = df_map[df_map["is_international"] == True]
elif intl_filter == "Domestic":
    df_map = df_map[df_map["is_international"] == False]

st.caption(f"Showing {len(df_map):,} flights after filters")

# ── Colour mapping ────────────────────────────────────────────────────────────
# Pydeck uses [R, G, B, A] colour arrays (0-255)
STATUS_COLORS = {
    "active"   : [59,  130, 246, 220],   # Blue
    "landed"   : [34,  197, 94,  180],   # Green
    "scheduled": [139, 92,  246, 180],   # Purple
    "cancelled": [239, 68,  68,  200],   # Red
    "diverted" : [245, 158, 11,  200],   # Amber
    "unknown"  : [100, 116, 139, 150],   # Gray
}
df_map["color"] = df_map["status"].map(STATUS_COLORS).apply(
    lambda x: x if isinstance(x, list) else [100, 116, 139, 150]
)
# Scale radius by altitude: higher = larger dot
df_map["radius"] = (df_map["altitude"].fillna(0) / 1000 + 5) * 1500

# ── Pydeck map ────────────────────────────────────────────────────────────────
# Initial view centred on India (where most flights originate)
view_state = pdk.ViewState(
    latitude    = 20.5937,
    longitude   = 78.9629,
    zoom        = 4,
    pitch       = 0,
    bearing     = 0,
)

# ScatterplotLayer: one dot per flight
scatter_layer = pdk.Layer(
    "ScatterplotLayer",
    data            = df_map,
    get_position    = ["longitude", "latitude"],
    get_radius      = "radius",
    get_fill_color  = "color",
    pickable        = True,    # Enables click/hover tooltips
    auto_highlight  = True,
    opacity         = 0.8,
)

# TextLayer: airline code labels (only when zoomed in enough)
text_layer = pdk.Layer(
    "TextLayer",
    data            = df_map[df_map["altitude"] > 25000],  # Only high-altitude
    get_position    = ["longitude", "latitude"],
    get_text        = "airline_iata",
    get_size        = 10,
    get_color       = [241, 245, 249],
    get_anchor      = "middle",
)

tooltip = {
    "html": """
        <div style="background:#1e293b;border:1px solid #334155;
                    border-radius:8px;padding:10px 14px;font-family:system-ui;">
            <div style="font-size:14px;font-weight:700;color:#f1f5f9;margin-bottom:6px;">
                ✈ {flight_id} — {airline_iata}
            </div>
            <div style="font-size:12px;color:#94a3b8;">
                {source_airport} → {dest_airport}
            </div>
            <hr style="border-color:#334155;margin:6px 0;">
            <div style="font-size:11px;color:#cbd5e1;">
                <b>Status:</b> {status} &nbsp;|&nbsp;
                <b>Phase:</b> {flight_phase}<br>
                <b>Altitude:</b> {altitude} ft &nbsp;|&nbsp;
                <b>Speed:</b> {speed} km/h<br>
                <b>Delay:</b> {delay_minutes} min ({delay_bucket})
            </div>
        </div>
    """,
    "style": {"padding": "0"},
}


st.pydeck_chart(
    pdk.Deck(
        layers          = [scatter_layer, text_layer],
        initial_view_state= view_state,
        tooltip         = tooltip,
        map_style       = "mapbox://styles/mapbox/dark-v11",
    ),
    use_container_width = True,
)

# ── Legend ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:8px;">
  <span>🔵 Active</span>
  <span>🟢 Landed</span>
  <span>🟣 Scheduled</span>
  <span>🔴 Cancelled</span>
  <span>🟡 Diverted</span>
  <span style="color:#64748b">⬤ Unknown</span>
  <span style="color:#64748b;margin-left:auto;">Dot size = altitude</span>
</div>
""", unsafe_allow_html=True)

st.divider()

# ── Airline activity table ─────────────────────────────────────────────────────
st.subheader("Active Airlines")
if not df_airlines.empty:
    display_cols = {
        "airline_name"     : "Airline",
        "active_flights"   : "Active Flights",
        "avg_altitude_ft"  : "Avg Altitude (ft)",
        "avg_speed_kmh"    : "Avg Speed (km/h)",
        "avg_delay_minutes": "Avg Delay (min)",
        "pct_delayed"      : "% Delayed",
    }
    df_display = df_airlines[[c for c in display_cols if c in df_airlines.columns]]
    df_display = df_display.rename(columns=display_cols)
    st.dataframe(
        df_display,
        use_container_width = True,
        hide_index          = True,
    )