# dashboard/pages/2_Analytics.py
"""
Page 2 — Analytics
====================
Business performance analytics across all 10 query results.
Uses tabs to organise without overwhelming the user.
"""

import streamlit as st
import pandas as pd

from dashboard.utils.data_loader import (
    get_airline_league_table, get_delay_patterns, get_peak_hours,
    get_route_efficiency, get_weekly_trends, get_airport_congestion,
    get_intl_vs_domestic,
)
from dashboard.components.charts import (
    airline_otp_bar_chart, delay_heatmap, peak_hours_area_chart,
    route_efficiency_scatter, weekly_trend_line, airport_congestion_bars,
)
from dashboard.utils.formatters import (
    fmt_number, fmt_pct, fmt_minutes, route_health_emoji
)
from dashboard.components.kpi_cards import render_kpi_row

st.title("📊 Analytics")
st.caption("Business performance insights · Refreshed every 5 minutes")

# ── Top KPIs ──────────────────────────────────────────────────────────────────
league_df = get_airline_league_table()
trends_df = get_weekly_trends()

if not league_df.empty:
    best_airline = league_df.iloc[0]
    worst_airline= league_df.iloc[-1]
    fleet_otp    = league_df["otp_rate_pct"].mean()
    fleet_delay  = league_df["avg_delay_minutes"].mean()

    render_kpi_row([
        {
            "label": "🏆 Best Airline",
            "value": best_airline.get("airline_name", best_airline.get("airline_iata", "N/A")),
            "delta": f"OTP: {best_airline.get('otp_rate_pct', 0):.1f}%",
        },
        {
            "label": "📉 Worst Airline",
            "value": worst_airline.get("airline_name", worst_airline.get("airline_iata", "N/A")),
            "delta": f"OTP: {worst_airline.get('otp_rate_pct', 0):.1f}%",
            "delta_color": "inverse",
        },
        {
            "label": "✈ Fleet OTP Rate",
            "value": fmt_pct(fleet_otp),
            "help" : "Average on-time performance across all airlines",
        },
        {
            "label": "⏱ Fleet Avg Delay",
            "value": fmt_minutes(fleet_delay),
            "help" : "Average delay across all flights",
        },
    ])
    st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏆 Airlines",
    "⏱ Delays",
    "📈 Trends",
    "🗺 Routes",
    "🛫 Airports",
    "🌍 Int'l vs Dom",
])

# ── Tab 1: Airline League Table ───────────────────────────────────────────────
with tab1:
    st.subheader("Airline Performance League Table")
    st.caption("Ranked by composite score (OTP × 0.6 + speed × 0.3 + consistency × 0.1)")

    if not league_df.empty:
        col_chart, col_table = st.columns([1, 1])

        with col_chart:
            st.plotly_chart(
                airline_otp_bar_chart(league_df),
                use_container_width=True,
            )

        with col_table:
            # Show formatted table
            display = league_df[[
                c for c in [
                    "otp_rank", "airline_name", "airline_iata",
                    "total_unique_flights", "otp_rate_pct",
                    "avg_delay_minutes", "composite_score",
                ] if c in league_df.columns
            ]].copy()

            if "otp_rate_pct" in display.columns:
                display["otp_rate_pct"] = display["otp_rate_pct"].apply(
                    lambda x: f"{x:.1f}%"
                )
            if "avg_delay_minutes" in display.columns:
                display["avg_delay_minutes"] = display["avg_delay_minutes"].apply(
                    lambda x: f"{x:.1f} min"
                )
            if "composite_score" in display.columns:
                display["composite_score"] = display["composite_score"].apply(
                    lambda x: f"{x:.1f}"
                )

            st.dataframe(
                display,
                use_container_width = True,
                hide_index          = True,
            )
    else:
        st.info("No airline data available yet.")

# ── Tab 2: Delay Analysis ─────────────────────────────────────────────────────
with tab2:
    st.subheader("Delay Patterns Analysis")
    delay_df = get_delay_patterns()
    hours_df = get_peak_hours()

    col1, col2 = st.columns([1, 1])
    with col1:
        st.plotly_chart(delay_heatmap(delay_df), use_container_width=True)
    with col2:
        st.plotly_chart(peak_hours_area_chart(hours_df), use_container_width=True)

    # Delay distribution donut
    if not league_df.empty:
        st.subheader("Fleet Delay Bucket Distribution")
        delay_cols = {
            "on_time"        : "On Time",
            "pct_minor_delay": "Minor (1-15 min)",
            "pct_moderate_delay": "Moderate (15-60 min)",
            "pct_major_delay"  : "Major (1-3 hrs)",
            "pct_severe_delay" : "Severe (>3 hrs)",
        }
        bucket_totals = {}
        for col, label in delay_cols.items():
            if col in league_df.columns:
                bucket_totals[label] = league_df[col].mean()

        if bucket_totals:
            import plotly.graph_objects as go
            donut = go.Figure(go.Pie(
                labels    = list(bucket_totals.keys()),
                values    = list(bucket_totals.values()),
                hole      = 0.55,
                marker_colors = [
                    "#22c55e", "#3b82f6", "#f59e0b", "#f97316", "#ef4444"
                ],
                textfont  = dict(color="#f1f5f9"),
            ))
            donut.update_layout(
                paper_bgcolor = "rgba(0,0,0,0)",
                plot_bgcolor  = "rgba(0,0,0,0)",
                font          = dict(color="#f1f5f9"),
                height        = 320,
                showlegend    = True,
                annotations   = [dict(
                    text      = "Delay<br>Mix",
                    x=0.5, y=0.5,
                    font_size = 14,
                    showarrow = False,
                    font_color= "#f1f5f9",
                )],
            )
            st.plotly_chart(donut, use_container_width=True)

# ── Tab 3: Weekly Trends ──────────────────────────────────────────────────────
with tab3:
    st.subheader("30-Day Performance Trend")
    trends_df = get_weekly_trends()
    if not trends_df.empty:
        st.plotly_chart(weekly_trend_line(trends_df), use_container_width=True)

        # Show trend direction for last 7 days
        recent = trends_df.tail(7)
        if "otp_trend" in recent.columns:
            trend_counts = recent["otp_trend"].value_counts()
            col1, col2, col3 = st.columns(3)
            col1.metric("IMPROVING days",  trend_counts.get("IMPROVING", 0))
            col2.metric("STABLE days",     trend_counts.get("STABLE",    0))
            col3.metric("DEGRADING days",  trend_counts.get("DEGRADING", 0))
    else:
        st.info("No trend data available yet — need 2+ weeks of data.")

# ── Tab 4: Routes ─────────────────────────────────────────────────────────────
with tab4:
    st.subheader("Route Efficiency Analysis")
    routes_df = get_route_efficiency()
    if not routes_df.empty:
        st.plotly_chart(route_efficiency_scatter(routes_df), use_container_width=True)
        st.caption(
            "Top-right quadrant = high OTP + low delay = most efficient routes. "
            "Bottom-left = needs attention."
        )

        # Best/worst routes table
        col_best, col_worst = st.columns(2)
        with col_best:
            st.subheader("🟢 Most Efficient Routes")
            best = routes_df.head(5)[["route_key","otp_rate_pct","avg_delay_minutes","route_health"]]
            best["route_health"] = best["route_health"].apply(route_health_emoji)
            st.dataframe(best, hide_index=True, use_container_width=True)
        with col_worst:
            st.subheader("🔴 Least Efficient Routes")
            worst = routes_df.tail(5)[["route_key","otp_rate_pct","avg_delay_minutes","route_health"]]
            worst["route_health"] = worst["route_health"].apply(route_health_emoji)
            st.dataframe(worst, hide_index=True, use_container_width=True)
    else:
        st.info("No route data available yet.")

# ── Tab 5: Airports ───────────────────────────────────────────────────────────
with tab5:
    st.subheader("Airport Congestion Analysis")
    airports_df = get_airport_congestion()
    if not airports_df.empty:
        st.plotly_chart(airport_congestion_bars(airports_df), use_container_width=True)
        st.dataframe(
            airports_df[["airport_code","city","total_movements",
                          "avg_delay_minutes","delay_rate_pct"]].head(15),
            hide_index          = True,
            use_container_width = True,
        )
    else:
        st.info("No airport data available yet.")

# ── Tab 6: International vs Domestic ─────────────────────────────────────────
with tab6:
    st.subheader("International vs Domestic Traffic Split")
    intl_df = get_intl_vs_domestic()
    if not intl_df.empty:
        col1, col2 = st.columns([1, 1])
        with col1:
            import plotly.graph_objects as go
            fig = go.Figure(go.Pie(
                labels        = intl_df["segment"],
                values        = intl_df["traffic_share_pct"],
                hole          = 0.4,
                marker_colors = ["#3b82f6", "#22c55e"],
                textfont      = dict(color="#f1f5f9"),
            ))
            fig.update_layout(
                paper_bgcolor = "rgba(0,0,0,0)",
                plot_bgcolor  = "rgba(0,0,0,0)",
                font          = dict(color="#f1f5f9"),
                height        = 300,
                title         = "Traffic Share by Segment",
            )
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            for _, row in intl_df.iterrows():
                st.metric(
                    label = row["segment"],
                    value = f"{row.get('total_flights', 0):,.0f} flights",
                )
                st.caption(
                    f"OTP: {row.get('avg_otp_rate_pct', 0):.1f}% · "
                    f"Avg Delay: {row.get('avg_delay_minutes', 0):.1f} min · "
                    f"Routes: {row.get('unique_routes', 0)}"
                )
    else:
        st.info("No segment data available yet.")