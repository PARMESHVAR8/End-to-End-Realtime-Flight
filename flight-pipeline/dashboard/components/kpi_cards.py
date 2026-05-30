# dashboard/components/kpi_cards.py
"""
Reusable KPI Card Components
=============================
Renders metric cards with delta indicators.
Used on every page for consistent headline metrics.
"""

import streamlit as st
from dashboard.utils.formatters import (
    fmt_number, fmt_pct, fmt_minutes, health_colour
)


def render_kpi_row(metrics: list[dict]) -> None:
    """
    Render a row of KPI cards.

    Args:
        metrics: List of dicts, each with keys:
            label       : str  — card label
            value       : str  — main displayed value
            delta       : str  — change vs previous period (optional)
            delta_color : "normal" | "inverse" | "off"  (optional)
            help        : str  — tooltip text (optional)

    EXAMPLE:
        render_kpi_row([
            {"label": "Active Flights", "value": "247", "delta": "+12 vs yesterday"},
            {"label": "Fleet OTP",      "value": "87.3%", "delta": "-2.1pp",
             "delta_color": "inverse"},
        ])
    """
    cols = st.columns(len(metrics))
    for col, metric in zip(cols, metrics):
        with col:
            st.metric(
                label       = metric["label"],
                value       = metric["value"],
                delta       = metric.get("delta"),
                delta_color = metric.get("delta_color", "normal"),
                help        = metric.get("help"),
            )


def render_health_badge(status: str, label: str = "Pipeline") -> None:
    """
    Render a coloured health badge using st.markdown.
    st.metric doesn't support custom colours, so we use HTML.
    """
    colour = health_colour(status)
    icon   = {
        "HEALTHY"  : "✅",
        "DEGRADED" : "⚠️",
        "DOWN"     : "🔴",
    }.get(status.upper(), "❓")

    st.markdown(
        f"""
        <div style="
            display: inline-block;
            background: {colour}22;
            border: 1.5px solid {colour};
            border-radius: 8px;
            padding: 6px 14px;
            font-size: 13px;
            font-weight: 700;
            color: {colour};
        ">
            {icon} {label}: {status}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_progress_bar(
    value   : float,
    max_val : float,
    label   : str,
    colour  : str = "#3b82f6",
) -> None:
    """
    Render a custom progress bar with label and percentage.
    Streamlit's built-in st.progress() doesn't support custom colours.
    """
    pct = min(value / max(max_val, 1) * 100, 100)
    st.markdown(
        f"""
        <div style="margin-bottom:8px;">
            <div style="display:flex;justify-content:space-between;
                        font-size:11px;margin-bottom:3px;">
                <span>{label}</span>
                <span style="font-weight:700;">{value:,.0f}</span>
            </div>
            <div style="background:#1e293b;border-radius:4px;height:8px;">
                <div style="
                    width:{pct:.1f}%;
                    background:{colour};
                    border-radius:4px;
                    height:8px;
                "></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )