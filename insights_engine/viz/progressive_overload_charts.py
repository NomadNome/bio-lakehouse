"""
Bio Insights Engine - Progressive Overload Charts

Single clean weekly watts trend with status-colored markers and
a rolling average overlay.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from insights_engine.viz import theme


STATUS_COLORS = {
    "Progressing": theme.SUCCESS,
    "Maintaining": theme.ACCENT,
    "Regressing": theme.WARNING,
    "Baseline": theme.TEXT_MUTED,
}


LOOKBACK_WEEKS = 26  # 6 months — bump to 52 once more Oura data lands


def build_overload_chart(weekly: pd.DataFrame) -> go.Figure:
    """Weekly avg watts line with status-colored markers + 4-week rolling avg.

    Shows the most recent LOOKBACK_WEEKS only.
    """

    # Trim to recent window
    plot = weekly.tail(LOOKBACK_WEEKS).copy()

    fig = go.Figure()

    marker_colors = [STATUS_COLORS.get(s, theme.ACCENT) for s in plot["status"]]

    # Main line: weekly avg watts (datetime x-axis for clean auto-ticks)
    fig.add_trace(
        go.Scatter(
            x=plot["week_start"],
            y=plot["weekly_avg_watts"],
            mode="lines+markers",
            name="Avg Watts",
            line=dict(color=theme.PRIMARY, width=2.5),
            marker=dict(
                color=marker_colors,
                size=10,
                line=dict(width=2, color=theme.PRIMARY),
            ),
            hovertemplate=(
                "Week of %{x|%b %-d}<br>"
                "Avg Watts: %{y:.0f}W<br>"
                "Status: %{customdata}"
                "<extra></extra>"
            ),
            customdata=plot["status"],
        )
    )

    # 4-week rolling average
    rolling = plot["weekly_avg_watts"].rolling(4, min_periods=2).mean()
    fig.add_trace(
        go.Scatter(
            x=plot["week_start"],
            y=rolling,
            mode="lines",
            name="4-week Avg",
            line=dict(color=theme.TEXT_MUTED, width=2, dash="dash"),
            hovertemplate="4-wk Avg: %{y:.0f}W<extra></extra>",
        )
    )

    n_workouts = int(plot["weekly_workout_count"].sum())
    theme.style_figure(fig, n=n_workouts, height=350)
    fig.update_layout(
        xaxis=dict(dtick="M1", tickformat="%b '%y"),
        yaxis_title="Avg Watts",
        legend=dict(orientation="h", y=1.05, x=0.5, xanchor="center"),
        margin=dict(l=60, r=30, t=30, b=50),
    )

    return fig
