# dashboard/pages/3_Pipeline_Health.py
"""
Page 3 — Pipeline Health
==========================
Engineering-focused view of pipeline status.
Shows data freshness, run metrics, DLQ status, and quality.
This is the page YOUR TEAM watches, not business users.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from dashboard.utils.data_loader  import get_pipeline_health, get_run_metrics_history
from dashboard.utils.formatters   import health_colour, fmt_number, fmt_pct
from dashboard.components.kpi_cards import render_kpi_row, render_health_badge
from dashboard.components.charts   import pipeline_metrics_timeline

st.title("⚙️ Pipeline Health")
st.caption("Engineering observability · Refreshed every 30 seconds")

# ── Health scorecard ──────────────────────────────────────────────────────────
health = get_pipeline_health()

if not health:
    st.warning("Pipeline health data unavailable. Check Snowflake connection.")
    st.stop()

overall = str(health.get("overall_health", "UNKNOWN"))
render_health_badge(overall)
st.divider()

# ── Layer freshness cards ─────────────────────────────────────────────────────
st.subheader("Data Layer Status")
col1, col2, col3 = st.columns(3)

def freshness_card(col, layer: str, freshness: str, lag: int, records: int):
    colour = health_colour(freshness)
    col.markdown(
        f"""
        <div style="background:#1e293b;border:1.5px solid {colour};
                    border-radius:10px;padding:14px 16px;">
            <div style="font-size:11px;font-weight:700;letter-spacing:.05em;
                        text-transform:uppercase;color:{colour};margin-bottom:6px;">
                {layer} LAYER
            </div>
            <div style="font-size:22px;font-weight:700;color:#f1f5f9;">
                {freshness}
            </div>
            <div style="font-size:11px;color:#94a3b8;margin-top:4px;">
                {lag} min lag · {records:,} records today
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

freshness_card(
    col1, "RAW",
    str(health.get("raw_freshness",       "UNKNOWN")),
    int(health.get("raw_lag_minutes",     0) or 0),
    int(health.get("raw_records_today",   0) or 0),
)
freshness_card(
    col2, "CLEAN",
    str(health.get("clean_freshness",     "UNKNOWN")),
    int(health.get("clean_lag_minutes",   0) or 0),
    int(health.get("clean_records_today", 0) or 0),
)
freshness_card(
    col3, "ANALYTICS",
    str(health.get("analytics_freshness", "UNKNOWN")),
    0,
    int(health.get("analytics_records_today", 0) or 0),
)

st.divider()

# ── Quality metrics ───────────────────────────────────────────────────────────
st.subheader("Data Quality")
clean_pct    = float(health.get("clean_record_pct",      0) or 0)
flagged      = int(health.get("flagged_records_today",   0) or 0)
funnel_pct   = float(health.get("raw_to_analytics_pct", 0) or 0)

render_kpi_row([
    {
        "label": "✅ Clean Record Rate",
        "value": fmt_pct(clean_pct),
        "delta": "Healthy" if clean_pct >= 95 else "Below threshold",
        "delta_color": "normal" if clean_pct >= 95 else "inverse",
    },
    {
        "label": "🚩 Flagged Records Today",
        "value": fmt_number(flagged),
        "delta": "Low" if flagged < 50 else "High — investigate",
        "delta_color": "normal" if flagged < 50 else "inverse",
    },
    {
        "label": "🔄 RAW → Analytics Yield",
        "value": fmt_pct(funnel_pct),
        "help" : "% of raw records that made it to the analytics layer",
    },
])

# Quality gauge chart
gauge = go.Figure(go.Indicator(
    mode    = "gauge+number+delta",
    value   = clean_pct,
    delta   = {"reference": 95, "valueformat": ".1f"},
    title   = {"text": "Clean Record %", "font": {"color": "#f1f5f9"}},
    gauge   = {
        "axis"      : {"range": [0, 100], "tickcolor": "#64748b"},
        "bar"       : {"color": "#3b82f6"},
        "bgcolor"   : "#1e293b",
        "bordercolor": "#334155",
        "steps"     : [
            {"range": [0,  80], "color": "#ef4444"},
            {"range": [80, 95], "color": "#f59e0b"},
            {"range": [95, 100],"color": "#22c55e"},
        ],
        "threshold" : {
            "line" : {"color": "#f1f5f9", "width": 2},
            "value": 95,
        },
    },
    number  = {"suffix": "%", "font": {"color": "#f1f5f9"}},
))
gauge.update_layout(
    paper_bgcolor = "rgba(0,0,0,0)",
    plot_bgcolor  = "rgba(0,0,0,0)",
    font          = dict(color="#f1f5f9"),
    height        = 260,
)
st.plotly_chart(gauge, use_container_width=True)
st.divider()

# ── Run metrics history ────────────────────────────────────────────────────────
st.subheader("Pipeline Run History")
metrics_df = get_run_metrics_history()

if not metrics_df.empty:
    st.plotly_chart(
        pipeline_metrics_timeline(metrics_df),
        use_container_width=True,
    )

    # Summary table
    st.subheader("Recent Runs")
    display = metrics_df.head(20)[[
        c for c in [
            "dag_id", "started_at", "records_read", "records_written",
            "elapsed_seconds", "throughput_rps", "error_rate_pct", "status",
        ] if c in metrics_df.columns
    ]].copy()

    if "elapsed_seconds" in display.columns:
        display["elapsed_seconds"] = display["elapsed_seconds"].apply(
            lambda x: f"{x:.1f}s" if x else "N/A"
        )
    if "throughput_rps" in display.columns:
        display["throughput_rps"] = display["throughput_rps"].apply(
            lambda x: f"{x:.0f} r/s" if x else "N/A"
        )
    if "error_rate_pct" in display.columns:
        display["error_rate_pct"] = display["error_rate_pct"].apply(
            lambda x: f"{x:.2f}%" if x is not None else "N/A"
        )

    st.dataframe(display, hide_index=True, use_container_width=True)
else:
    st.info(
        "No run metrics yet. "
        "Make sure the pipeline has run at least once "
        "and PIPELINE_RUN_METRICS table exists in Snowflake."
    )