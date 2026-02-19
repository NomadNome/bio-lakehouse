"""
Bio Insights Engine - What-If Simulator Charts

Scenario comparison bar chart and readiness gauge for the What-If page.
"""

from __future__ import annotations

import plotly.graph_objects as go

from insights_engine.viz import theme


def scenario_comparison_chart(
    scenarios: list[dict],
) -> go.Figure:
    """Grouped bar chart comparing predicted readiness across scenarios.

    Each dict in *scenarios* must have keys:
        label (str), predicted_readiness (float),
        confidence_range (tuple[float, float]).
    """
    labels = [s["label"] for s in scenarios]
    values = [s["predicted_readiness"] for s in scenarios]
    lows = [s["confidence_range"][0] for s in scenarios]
    highs = [s["confidence_range"][1] for s in scenarios]
    errors_minus = [v - lo for v, lo in zip(values, lows)]
    errors_plus = [hi - v for v, hi in zip(values, highs)]

    colors = theme.SERIES_COLORS[: len(scenarios)]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=labels,
            y=values,
            marker_color=colors,
            error_y=dict(
                type="data",
                symmetric=False,
                array=errors_plus,
                arrayminus=errors_minus,
                color=theme.TEXT_MUTED,
                thickness=1.5,
            ),
            text=[f"{v:.0f}" for v in values],
            textposition="outside",
            textfont=dict(color=theme.TEXT),
            hovertemplate=(
                "%{x}<br>"
                "Predicted: %{y:.1f}<br>"
                "Range: %{customdata[0]:.0f}–%{customdata[1]:.0f}"
                "<extra></extra>"
            ),
            customdata=list(zip(lows, highs)),
        )
    )

    theme.style_figure(
        fig,
        title="Scenario Comparison — Predicted Readiness",
        xaxis_title="Scenario",
        yaxis_title="Predicted Readiness",
        yaxis_range=[0, 110],
    )
    return fig


def readiness_gauge(
    predicted: float,
    confidence_range: tuple[float, float],
    baseline_avg: float,
) -> go.Figure:
    """Bullet-style gauge showing predicted readiness vs personal baseline."""
    fig = go.Figure()

    # Confidence band (background bar)
    fig.add_trace(
        go.Bar(
            x=[confidence_range[1] - confidence_range[0]],
            y=["Readiness"],
            orientation="h",
            base=confidence_range[0],
            marker_color=theme.SURFACE,
            name="Confidence Range",
            hovertemplate=f"Range: {confidence_range[0]:.0f}–{confidence_range[1]:.0f}<extra></extra>",
            width=0.5,
        )
    )

    # Predicted readiness (main bar)
    bar_color = (
        theme.SUCCESS if predicted >= 85
        else theme.PRIMARY if predicted >= 70
        else theme.WARNING if predicted >= 50
        else theme.DANGER
    )
    fig.add_trace(
        go.Bar(
            x=[predicted],
            y=["Readiness"],
            orientation="h",
            marker_color=bar_color,
            name="Predicted",
            hovertemplate=f"Predicted: {predicted:.1f}<extra></extra>",
            width=0.3,
        )
    )

    # Baseline marker
    fig.add_shape(
        type="line",
        x0=baseline_avg,
        x1=baseline_avg,
        y0=-0.4,
        y1=0.4,
        yref="y",
        line=dict(color=theme.SECONDARY, width=3, dash="dash"),
    )
    fig.add_annotation(
        x=baseline_avg,
        y="Readiness",
        text=f"7d avg: {baseline_avg:.0f}",
        showarrow=True,
        arrowhead=0,
        ax=0,
        ay=-35,
        font=dict(color=theme.SECONDARY, size=11),
        arrowcolor=theme.SECONDARY,
    )

    theme.style_figure(
        fig,
        title="Predicted Readiness",
        xaxis_title="Readiness Score",
        xaxis_range=[0, 105],
        height=200,
        showlegend=False,
        barmode="overlay",
    )
    return fig
