# dashboard/components/charts.py
"""
Reusable Chart Functions
=========================
All charts use Plotly for consistency.
Each function takes a DataFrame and returns a Plotly figure.
Pages call these functions — they never build charts directly.

WHY PLOTLY OVER MATPLOTLIB:
  Plotly = interactive (hover, zoom, click, filter)
  Matplotlib = static images
  In a web dashboard, interactivity is always better.
  Plotly is also native to Streamlit — no extra configuration.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Optional

# Aviation-inspired colour palette
COLOURS = {
    "primary"   : "#3b82f6",
    "success"   : "#22c55e",
    "warning"   : "#f59e0b",
    "danger"    : "#ef4444",
    "purple"    : "#8b5cf6",
    "teal"      : "#14b8a6",
    "pink"      : "#ec4899",
    "navy"      : "#0f172a",
    "card_bg"   : "#1e293b",
    "text"      : "#f1f5f9",
    "grid"      : "#334155",
}

# Plotly dark template override
LAYOUT_DEFAULTS = dict(
    paper_bgcolor = "rgba(0,0,0,0)",     # Transparent background
    plot_bgcolor  = "rgba(0,0,0,0)",
    font          = dict(color=COLOURS["text"], family="system-ui"),
    margin        = dict(l=10, r=10, t=40, b=10),
    showlegend    = True,
    legend        = dict(
        bgcolor    = "rgba(30,41,59,0.8)",
        bordercolor= COLOURS["grid"],
        borderwidth= 1,
    ),
)


def _apply_layout(fig, title: str = "", height: int = 400) -> go.Figure:
    """Apply consistent layout defaults to any figure."""
    fig.update_layout(
        title  = dict(text=title, font=dict(size=14, color=COLOURS["text"])),
        height = height,
        **LAYOUT_DEFAULTS,
    )
    fig.update_xaxes(
        gridcolor   = COLOURS["grid"],
        linecolor   = COLOURS["grid"],
        tickfont    = dict(color=COLOURS["text"]),
    )
    fig.update_yaxes(
        gridcolor   = COLOURS["grid"],
        linecolor   = COLOURS["grid"],
        tickfont    = dict(color=COLOURS["text"]),
    )
    return fig


def airline_otp_bar_chart(df: pd.DataFrame) -> go.Figure:
    """
    Horizontal bar chart of airline OTP rates.
    Colour: green (≥90%), yellow (≥75%), red (<75%).

    WHY HORIZONTAL:
      Airline names are long. Horizontal bars give space for labels.
      The longest name (Singapore Airlines) still fits.
    """
    if df.empty:
        return go.Figure().update_layout(**LAYOUT_DEFAULTS)

    # Sort by OTP rate ascending (best airline at top in horizontal chart)
    df_sorted = df.nlargest(15, "otp_rate_pct").sort_values("otp_rate_pct")
    name_col  = "airline_name" if "airline_name" in df.columns else "airline_iata"

    # Assign colour based on OTP rate
    colours = [
        COLOURS["success"] if v >= 90 else
        COLOURS["warning"] if v >= 75 else
        COLOURS["danger"]
        for v in df_sorted["otp_rate_pct"]
    ]

    fig = go.Figure(go.Bar(
        x           = df_sorted["otp_rate_pct"],
        y           = df_sorted[name_col],
        orientation = "h",
        marker_color= colours,
        text        = df_sorted["otp_rate_pct"].apply(lambda v: f"{v:.1f}%"),
        textposition= "outside",
        hovertemplate= (
            "<b>%{y}</b><br>"
            "OTP Rate: %{x:.1f}%<br>"
            "<extra></extra>"
        ),
    ))

    # Add benchmark line at 80% (DGCA/industry standard)
    fig.add_vline(
        x           = 80,
        line_color  = COLOURS["warning"],
        line_dash   = "dash",
        annotation_text = "80% benchmark",
        annotation_font_color = COLOURS["warning"],
    )

    return _apply_layout(
        fig,
        title  = "Airline On-Time Performance (OTP) Rate",
        height = max(300, len(df_sorted) * 35 + 80),
    )


def delay_heatmap(df: pd.DataFrame) -> go.Figure:
    """
    Heatmap: hours of day (x) × airlines (y) × avg delay (colour).
    Reveals exactly WHEN each airline experiences peak delays.
    """
    if df.empty:
        return go.Figure().update_layout(**LAYOUT_DEFAULTS)

    # Pivot: rows=airline, cols=hour, values=avg_delay
    pivot = df.pivot_table(
        values  = "avg_delay",
        index   = "airline_iata",
        columns = "event_hour",
        aggfunc = "mean",
    ).fillna(0)

    fig = go.Figure(go.Heatmap(
        z           = pivot.values,
        x           = [f"{h:02d}:00" for h in pivot.columns],
        y           = pivot.index,
        colorscale  = [
            [0.0,  "#22c55e"],   # Green  = no delay
            [0.3,  "#3b82f6"],   # Blue   = minor
            [0.6,  "#f59e0b"],   # Amber  = moderate
            [1.0,  "#ef4444"],   # Red    = severe
        ],
        colorbar    = dict(title="Avg Delay (min)"),
        hovertemplate= (
            "Airline: %{y}<br>"
            "Hour: %{x}<br>"
            "Avg Delay: %{z:.1f} min<br>"
            "<extra></extra>"
        ),
    ))

    return _apply_layout(
        fig,
        title  = "Delay Heatmap: Hour of Day × Airline (last 7 days)",
        height = max(300, len(pivot) * 40 + 100),
    )


def peak_hours_area_chart(df: pd.DataFrame) -> go.Figure:
    """
    Area chart showing flight volume by hour of day.
    Colour bands show traffic intensity (PEAK / BUSY / MODERATE / QUIET).
    """
    if df.empty:
        return go.Figure().update_layout(**LAYOUT_DEFAULTS)

    band_colours = {
        "PEAK"    : COLOURS["danger"],
        "BUSY"    : COLOURS["warning"],
        "MODERATE": COLOURS["primary"],
        "QUIET"   : COLOURS["teal"],
    }

    fig = go.Figure()

    # Main area trace
    fig.add_trace(go.Scatter(
        x           = df["hour_label"],
        y           = df["total_flights"],
        fill        = "tozeroy",
        fillcolor   = "rgba(59,130,246,0.2)",
        line        = dict(color=COLOURS["primary"], width=2),
        name        = "Total Flights",
        hovertemplate= (
            "%{x}<br>"
            "Flights: %{y:,}<br>"
            "<extra></extra>"
        ),
    ))

    # Overlay avg delay as secondary line
    if "avg_delay_minutes" in df.columns:
        fig.add_trace(go.Scatter(
            x           = df["hour_label"],
            y           = df["avg_delay_minutes"],
            yaxis       = "y2",
            line        = dict(color=COLOURS["warning"], width=2, dash="dot"),
            name        = "Avg Delay (min)",
            hovertemplate= "%{x}<br>Avg Delay: %{y:.1f} min<extra></extra>",
        ))
        fig.update_layout(
            yaxis2=dict(
                title     = "Avg Delay (min)",
                overlaying= "y",
                side      = "right",
                gridcolor = COLOURS["grid"],
                tickfont  = dict(color=COLOURS["warning"]),
            )
        )

    fig.update_layout(yaxis_title="Flights")
    return _apply_layout(
        fig,
        title  = "Flight Volume & Average Delay by Hour of Day",
        height = 380,
    )


def route_efficiency_scatter(df: pd.DataFrame) -> go.Figure:
    """
    Scatter plot: OTP rate (x) vs avg delay (y).
    Size = total flights. Colour = route health.
    Top-right corner = high OTP + low delay = efficient routes.
    """
    if df.empty:
        return go.Figure().update_layout(**LAYOUT_DEFAULTS)

    health_colours = {
        "EXCELLENT": COLOURS["success"],
        "GOOD"     : COLOURS["primary"],
        "AVERAGE"  : COLOURS["warning"],
        "POOR"     : COLOURS["danger"],
        "CRITICAL" : "#7f1d1d",
    }
    df["colour"] = df["route_health"].map(health_colours).fillna("#94a3b8")
    df["size"]   = (df["total_flights"] / df["total_flights"].max() * 40 + 5).clip(5, 45)

    fig = go.Figure()
    for health, grp in df.groupby("route_health"):
        fig.add_trace(go.Scatter(
            x           = grp["otp_rate_pct"],
            y           = grp["avg_delay_minutes"],
            mode        = "markers",
            name        = health,
            marker      = dict(
                size        = grp["size"],
                color       = health_colours.get(health, "#94a3b8"),
                opacity     = 0.8,
                line        = dict(color="white", width=0.5),
            ),
            text        = grp["route_key"],
            hovertemplate= (
                "<b>%{text}</b><br>"
                "OTP: %{x:.1f}%<br>"
                "Avg Delay: %{y:.1f} min<br>"
                "Flights: %{marker.size:.0f}<br>"
                "<extra></extra>"
            ),
        ))

    # Quadrant lines
    fig.add_hline(y=15, line_dash="dash", line_color=COLOURS["grid"],
                  annotation_text="15 min delay", annotation_font_color=COLOURS["grid"])
    fig.add_vline(x=80, line_dash="dash", line_color=COLOURS["grid"],
                  annotation_text="80% OTP", annotation_font_color=COLOURS["grid"])

    fig.update_layout(
        xaxis_title = "OTP Rate (%)",
        yaxis_title = "Avg Delay (minutes)",
    )
    return _apply_layout(
        fig,
        title  = "Route Efficiency: OTP Rate vs Avg Delay  (bubble size = traffic volume)",
        height = 450,
    )


def weekly_trend_line(df: pd.DataFrame) -> go.Figure:
    """
    Dual-axis line chart: OTP rate + avg delay over 30 days.
    Includes 7-day rolling average as a smoother trend line.
    """
    if df.empty:
        return go.Figure().update_layout(**LAYOUT_DEFAULTS)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # OTP rate (primary y-axis)
    fig.add_trace(go.Scatter(
        x    = pd.to_datetime(df["event_date"]),
        y    = df["otp_rate_pct"],
        name = "Daily OTP %",
        line = dict(color=COLOURS["primary"], width=1.5),
        mode = "lines+markers",
        marker=dict(size=4),
    ), secondary_y=False)

    # 7-day rolling average
    if "otp_7day_rolling_avg" in df.columns:
        fig.add_trace(go.Scatter(
            x    = pd.to_datetime(df["event_date"]),
            y    = df["otp_7day_rolling_avg"],
            name = "7-day Rolling Avg",
            line = dict(color=COLOURS["teal"], width=2.5, dash="dot"),
        ), secondary_y=False)

    # Avg delay (secondary y-axis)
    if "avg_delay" in df.columns:
        fig.add_trace(go.Scatter(
            x    = pd.to_datetime(df["event_date"]),
            y    = df["avg_delay"],
            name = "Avg Delay (min)",
            line = dict(color=COLOURS["warning"], width=1.5),
            fill = "tozeroy",
            fillcolor = "rgba(245,158,11,0.1)",
        ), secondary_y=True)

    fig.update_yaxes(
        title_text  = "OTP Rate (%)",
        secondary_y = False,
        gridcolor   = COLOURS["grid"],
        tickfont    = dict(color=COLOURS["text"]),
    )
    fig.update_yaxes(
        title_text  = "Avg Delay (min)",
        secondary_y = True,
        gridcolor   = COLOURS["grid"],
        tickfont    = dict(color=COLOURS["warning"]),
    )
    fig.update_xaxes(gridcolor=COLOURS["grid"], tickfont=dict(color=COLOURS["text"]))
    fig.update_layout(height=380, **LAYOUT_DEFAULTS,
                      title="30-Day Performance Trend: OTP Rate & Average Delay")
    return fig


def airport_congestion_bars(df: pd.DataFrame) -> go.Figure:
    """
    Grouped bar chart: departures + arrivals per airport.
    Sorted by total movements descending.
    """
    if df.empty:
        return go.Figure().update_layout(**LAYOUT_DEFAULTS)

    df = df.head(12).sort_values("total_movements")
    label_col = "city" if "city" in df.columns and df["city"].notna().any() else "airport_code"

    fig = go.Figure([
        go.Bar(
            name        = "Departures",
            x           = df[label_col],
            y           = df["departures"],
            marker_color= COLOURS["primary"],
            hovertemplate= "%{x}<br>Departures: %{y:,}<extra></extra>",
        ),
        go.Bar(
            name        = "Arrivals",
            x           = df[label_col],
            y           = df["arrivals"],
            marker_color= COLOURS["teal"],
            hovertemplate= "%{x}<br>Arrivals: %{y:,}<extra></extra>",
        ),
    ])
    fig.update_layout(barmode="group")
    return _apply_layout(fig, title="Airport Traffic: Departures vs Arrivals", height=380)


def pipeline_metrics_timeline(df: pd.DataFrame) -> go.Figure:
    """
    Scatter timeline of pipeline run durations.
    Colour = success (green) / failed (red).
    Size = records processed.
    """
    if df.empty:
        return go.Figure().update_layout(**LAYOUT_DEFAULTS)

    df = df.copy()
    df["colour"] = df["status"].map({
        "success": COLOURS["success"],
        "failed" : COLOURS["danger"],
        "running": COLOURS["warning"],
    }).fillna(COLOURS["primary"])

    df["size"] = (
        (df["records_written"].fillna(0) / df["records_written"].max().clip(1) * 20 + 5)
        .clip(5, 25)
    )

    fig = go.Figure(go.Scatter(
        x           = pd.to_datetime(df["started_at"]),
        y           = df["elapsed_seconds"],
        mode        = "markers",
        marker      = dict(
            color   = df["colour"],
            size    = df["size"],
            opacity = 0.8,
        ),
        text        = df["dag_id"],
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Started: %{x}<br>"
            "Duration: %{y:.1f}s<br>"
            "<extra></extra>"
        ),
    ))

    fig.update_layout(
        xaxis_title = "Run Time",
        yaxis_title = "Duration (seconds)",
    )
    return _apply_layout(
        fig,
        title  = "Pipeline Run History (bubble size = records processed)",
        height = 350,
    )