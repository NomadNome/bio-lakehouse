"""
Bio Insights Engine - What-If Simulator Charts

Scenario comparison bar chart, readiness gauge, and multi-day projection
charts for the What-If page.
"""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from insights_engine.config import READINESS_ZONE_BANDS
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


def multi_day_projection_chart(projections: list, baseline_readiness: float) -> go.Figure:
    """Two-row subplot: readiness projection with zone bands (top) and TSB bars (bottom)."""
    dates = [p.date_label for p in projections]
    readiness = [p.predicted_readiness for p in projections]
    lo = [p.confidence_range[0] for p in projections]
    hi = [p.confidence_range[1] for p in projections]
    tsb = [p.projected_tsb for p in projections]
    tss = [p.estimated_tss for p in projections]
    energy = [p.energy_state.replace("_", " ").title() for p in projections]

    fig = make_subplots(
        rows=2,
        cols=1,
        row_heights=[0.7, 0.3],
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=["Readiness Projection", "Training Stress Balance (TSB)"],
    )

    # ── Top: readiness with zone bands ──
    for band in READINESS_ZONE_BANDS:
        fig.add_hrect(
            y0=band["y0"],
            y1=band["y1"],
            fillcolor=band["color"],
            line_width=0,
            row=1,
            col=1,
        )
        fig.add_annotation(
            x=-0.02,
            y=(band["y0"] + band["y1"]) / 2,
            text=band["label"],
            showarrow=False,
            xref="paper",
            yref="y",
            font=dict(color=theme.TEXT_MUTED, size=9),
            xanchor="right",
            row=1,
            col=1,
        )

    # Confidence band
    fig.add_trace(
        go.Scatter(
            x=dates + dates[::-1],
            y=list(hi) + list(reversed(lo)),
            fill="toself",
            fillcolor=f"rgba(99,102,241,0.15)",
            line=dict(width=0),
            name="Confidence",
            hoverinfo="skip",
            showlegend=True,
        ),
        row=1,
        col=1,
    )

    # Readiness line
    fig.add_trace(
        go.Scatter(
            x=dates,
            y=readiness,
            mode="lines+markers",
            name="Predicted Readiness",
            line=dict(color=theme.PRIMARY, width=3),
            marker=dict(size=8, color=theme.PRIMARY),
            customdata=list(zip(energy, lo, hi, tss)),
            hovertemplate=(
                "%{x}<br>"
                "Readiness: %{y:.0f}<br>"
                "Energy: %{customdata[0]}<br>"
                "Range: %{customdata[1]:.0f}–%{customdata[2]:.0f}<br>"
                "TSS: %{customdata[3]:.0f}"
                "<extra></extra>"
            ),
        ),
        row=1,
        col=1,
    )

    # Baseline dashed line
    fig.add_hline(
        y=baseline_readiness,
        line_dash="dash",
        line_color=theme.SECONDARY,
        line_width=2,
        annotation_text=f"7d avg: {baseline_readiness:.0f}",
        annotation_font_color=theme.SECONDARY,
        annotation_font_size=10,
        row=1,
        col=1,
    )

    # ── Bottom: TSB bars ──
    tsb_colors = []
    for v in tsb:
        if v > 15:
            tsb_colors.append(theme.SUCCESS)
        elif v >= 0:
            tsb_colors.append(theme.ACCENT)
        elif v >= -15:
            tsb_colors.append(theme.WARNING)
        else:
            tsb_colors.append(theme.DANGER)

    fig.add_trace(
        go.Bar(
            x=dates,
            y=tsb,
            marker_color=tsb_colors,
            name="TSB",
            hovertemplate="%{x}<br>TSB: %{y:.1f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # TSB reference lines
    for ref_y, label in [(15, "Fresh"), (0, "Neutral"), (-15, "Fatigued")]:
        fig.add_hline(
            y=ref_y,
            line_dash="dot",
            line_color=theme.TEXT_MUTED,
            line_width=1,
            row=2,
            col=1,
        )

    # Style
    theme.style_figure(fig, height=550, showlegend=True)
    fig.update_yaxes(range=[0, 105], row=1, col=1)
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def day_card_html(projection, palette: dict) -> str:
    """Styled HTML card for a single projected day."""
    p = projection

    # Readiness badge color
    if p.predicted_readiness >= 85:
        badge_bg = palette["success"]
    elif p.predicted_readiness >= 70:
        badge_bg = palette["primary"]
    elif p.predicted_readiness >= 50:
        badge_bg = palette["warning"]
    else:
        badge_bg = palette["danger"]

    # Energy pill
    energy_colors = {
        "peak": palette["success"],
        "high": palette["accent"],
        "moderate": palette["primary"],
        "low": palette["warning"],
        "recovery_needed": palette["danger"],
    }
    energy_bg = energy_colors.get(p.energy_state, palette["text_muted"])
    energy_label = p.energy_state.replace("_", " ").title()

    # Risk pill
    risk_colors = {"low": palette["success"], "moderate": palette["warning"], "high": palette["danger"]}
    risk_bg = risk_colors.get(p.overtraining_risk, palette["text_muted"])

    return f"""
    <div style="background:{palette['surface']};border-radius:12px;padding:16px;margin-bottom:8px">
        <div style="font-weight:700;color:{palette['text']};margin-bottom:8px;font-size:0.95rem">
            {p.date_label}
        </div>
        <div style="text-align:center;margin-bottom:10px">
            <span style="background:{badge_bg};color:#fff;padding:6px 16px;
                border-radius:16px;font-weight:700;font-size:1.3rem">
                {p.predicted_readiness:.0f}
            </span>
        </div>
        <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px">
            <span style="background:{energy_bg};color:#fff;padding:2px 8px;
                border-radius:8px;font-size:0.75rem;font-weight:600">{energy_label}</span>
            <span style="background:{risk_bg};color:#fff;padding:2px 8px;
                border-radius:8px;font-size:0.75rem;font-weight:600">Risk: {p.overtraining_risk.title()}</span>
        </div>
        <div style="color:{palette['text_muted']};font-size:0.78rem;margin-bottom:6px">
            TSS {p.estimated_tss:.0f} &middot; TSB {p.projected_tsb:+.0f}
            &middot; Streak {p.consecutive_workout_days}d
        </div>
        <div style="color:{palette['text']};font-size:0.8rem;line-height:1.3">
            {p.recommendation[:120]}
        </div>
    </div>
    """
