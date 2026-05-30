# dashboard/utils/formatters.py
"""
Number and text formatting helpers for the dashboard.
Centralised so formatting is consistent across all pages.
"""

from datetime import datetime, timezone
from typing import Optional


def fmt_number(n: Optional[float], decimals: int = 0) -> str:
    """Format a number with commas: 1234567 → '1,234,567'"""
    if n is None:
        return "N/A"
    return f"{n:,.{decimals}f}"


def fmt_pct(n: Optional[float], decimals: int = 1) -> str:
    """Format as percentage: 0.923 or 92.3 → '92.3%'"""
    if n is None:
        return "N/A"
    # Handle both 0-1 and 0-100 inputs
    val = n * 100 if n <= 1.0 else n
    return f"{val:.{decimals}f}%"


def fmt_minutes(mins: Optional[float]) -> str:
    """Format minutes into human-readable: 125 → '2h 5m'"""
    if mins is None:
        return "N/A"
    mins = int(mins)
    if mins < 60:
        return f"{mins}m"
    hours = mins // 60
    remaining = mins % 60
    return f"{hours}h {remaining}m" if remaining else f"{hours}h"


def fmt_speed(kmh: Optional[float]) -> str:
    """Format speed: 850.5 → '850.5 km/h'"""
    if kmh is None:
        return "N/A"
    return f"{kmh:.1f} km/h"


def fmt_altitude(ft: Optional[float]) -> str:
    """Format altitude: 35000 → '35,000 ft'"""
    if ft is None:
        return "N/A"
    return f"{int(ft):,} ft"


def fmt_age(ts_str: Optional[str]) -> str:
    """Format timestamp as age: '2024-06-15T10:00:00Z' → '5 minutes ago'"""
    if not ts_str:
        return "N/A"
    try:
        ts  = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        if age < 60:
            return f"{int(age)}s ago"
        elif age < 3600:
            return f"{int(age/60)}m ago"
        else:
            return f"{int(age/3600)}h ago"
    except Exception:
        return str(ts_str)


def health_colour(status: str) -> str:
    """Map health status to hex colour."""
    return {
        "HEALTHY"          : "#22c55e",
        "DEGRADED"         : "#f59e0b",
        "DOWN"             : "#ef4444",
        "PIPELINE_DEGRADED": "#f59e0b",
        "QUALITY_DEGRADED" : "#f59e0b",
        "FRESH"            : "#22c55e",
        "ACCEPTABLE"       : "#3b82f6",
        "STALE"            : "#f59e0b",
        "CRITICAL"         : "#ef4444",
    }.get(str(status).upper(), "#94a3b8")


def delay_colour(delay_bucket: str) -> str:
    """Map delay bucket to colour for charts."""
    return {
        "on_time"        : "#22c55e",
        "minor_delay"    : "#3b82f6",
        "moderate_delay" : "#f59e0b",
        "major_delay"    : "#f97316",
        "severe_delay"   : "#ef4444",
    }.get(str(delay_bucket).lower(), "#94a3b8")


def route_health_emoji(health: str) -> str:
    """Map route health label to emoji."""
    return {
        "EXCELLENT": "🟢",
        "GOOD"     : "🔵",
        "AVERAGE"  : "🟡",
        "POOR"     : "🟠",
        "CRITICAL" : "🔴",
    }.get(str(health).upper(), "⚪")