"""
Experiment Visualizations — Plotly charts for intervention analysis.

4 chart types following existing config.py conventions:
1. Intervention timeline (metric line + colored rectangles)
2. Before/after distribution (violin)
3. Posterior plot (normal curve, shaded CI, zero line)
4. Summary table
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats

from insights_engine.config import CHART_CONFIG
from insights_engine.experiments.analyzer import BayesianResult, CorrelationResult, DiDResult

_palette = CHART_CONFIG["color_palette"]


def _chart_layout(fig: go.Figure, title: str = "", height: int = 400, dark: bool = True) -> go.Figure:
    """Apply standard layout settings."""
    fig.update_layout(
        title=title,
        height=height,
        template="plotly_dark" if dark else "plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=60, r=20, t=50, b=40),
    )
    return fig


def intervention_timeline(
    pre_df: pd.DataFrame,
    post_df: pd.DataFrame,
    metric_col: str,
    metric_label: str,
    intervention_name: str,
    start_date: str,
    end_date: str | None = None,
    dark: bool = True,
) -> go.Figure:
    """Metric line chart with colored intervention rectangle overlay."""
    fig = go.Figure()

    # Pre-period line
    if not pre_df.empty:
        fig.add_trace(go.Scatter(
            x=pre_df["date"], y=pre_df[metric_col],
            mode="lines+markers", name="Pre-intervention",
            line=dict(color=_palette["text_muted"], width=2),
            marker=dict(size=4),
        ))

    # Post-period line
    if not post_df.empty:
        fig.add_trace(go.Scatter(
            x=post_df["date"], y=post_df[metric_col],
            mode="lines+markers", name="Post-intervention",
            line=dict(color=_palette["primary"], width=2),
            marker=dict(size=4),
        ))

    # Intervention rectangle
    y_min = min(
        pre_df[metric_col].min() if not pre_df.empty else 0,
        post_df[metric_col].min() if not post_df.empty else 0,
    ) * 0.95
    y_max = max(
        pre_df[metric_col].max() if not pre_df.empty else 100,
        post_df[metric_col].max() if not post_df.empty else 100,
    ) * 1.05

    fig.add_shape(
        type="rect",
        x0=start_date,
        x1=end_date or pd.Timestamp.now().strftime("%Y-%m-%d"),
        y0=y_min, y1=y_max,
        fillcolor=_palette["accent"],
        opacity=0.15,
        line=dict(width=0),
    )

    # Start line
    fig.add_vline(
        x=start_date,
        line_dash="dash",
        line_color=_palette["accent"],
        annotation_text=intervention_name,
        annotation_position="top left",
    )

    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text=metric_label)

    return _chart_layout(fig, f"{metric_label} — {intervention_name}", dark=dark)


def before_after_distribution(
    pre_values: np.ndarray,
    post_values: np.ndarray,
    metric_label: str,
    dark: bool = True,
) -> go.Figure:
    """Violin/box plot comparing pre and post distributions."""
    fig = go.Figure()

    fig.add_trace(go.Violin(
        y=pre_values, name="Before",
        box_visible=True, meanline_visible=True,
        fillcolor=_palette["text_muted"],
        line_color=_palette["text"],
        opacity=0.7,
    ))

    fig.add_trace(go.Violin(
        y=post_values, name="After",
        box_visible=True, meanline_visible=True,
        fillcolor=_palette["primary"],
        line_color=_palette["accent"],
        opacity=0.7,
    ))

    fig.update_yaxes(title_text=metric_label)

    return _chart_layout(fig, f"{metric_label} — Before vs After", height=350, dark=dark)


def posterior_plot(
    result: BayesianResult,
    dark: bool = True,
) -> go.Figure:
    """Normal posterior distribution with shaded 95% CI and zero line."""
    fig = go.Figure()

    effect = result.posterior_mean_effect
    ci_low, ci_high = result.credible_interval_95

    # Posterior spread (approximate from CI width)
    posterior_std = (ci_high - ci_low) / (2 * 1.96)
    if posterior_std <= 0:
        posterior_std = 0.1

    x = np.linspace(effect - 4 * posterior_std, effect + 4 * posterior_std, 200)
    y = stats.norm.pdf(x, loc=effect, scale=posterior_std)

    # Full curve
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="lines", name="Posterior",
        line=dict(color=_palette["primary"], width=2),
    ))

    # Shaded CI
    ci_mask = (x >= ci_low) & (x <= ci_high)
    fig.add_trace(go.Scatter(
        x=x[ci_mask], y=y[ci_mask],
        fill="tozeroy", name="95% CI",
        fillcolor=f"rgba(99, 102, 241, 0.3)",
        line=dict(width=0),
    ))

    # Zero line
    fig.add_vline(
        x=0, line_dash="dash", line_color=_palette["danger"],
        annotation_text="No Effect",
    )

    # Effect line
    fig.add_vline(
        x=effect, line_dash="solid", line_color=_palette["success"],
        annotation_text=f"Effect: {effect:+.2f}",
        annotation_position="top right",
    )

    fig.update_xaxes(title_text="Effect Size")
    fig.update_yaxes(title_text="Density", showticklabels=False)

    return _chart_layout(fig, "Posterior Distribution of Effect", height=350, dark=dark)


def experiment_summary_table(
    experiments: list[dict],
) -> pd.DataFrame:
    """Create a summary DataFrame for display in Streamlit."""
    if not experiments:
        return pd.DataFrame()

    rows = []
    for exp in experiments:
        rows.append({
            "Name": exp.get("name", ""),
            "Type": exp.get("type", ""),
            "Start": exp.get("start_date", ""),
            "End": exp.get("end_date", "Active"),
            "Status": "Active" if exp.get("is_active") else "Ended",
            "Verdict": exp.get("verdict", "—"),
        })

    return pd.DataFrame(rows)


def correlation_scatter(
    result: CorrelationResult,
    input_label: str,
    outcome_label: str,
    dark: bool = True,
) -> go.Figure:
    """Scatter plot with OLS regression line and stats annotation."""
    fig = go.Figure()

    # Scatter points
    fig.add_trace(go.Scatter(
        x=result.scatter_x,
        y=result.scatter_y,
        mode="markers",
        name="Daily observations",
        marker=dict(
            color=_palette["primary"],
            size=7,
            opacity=0.6,
        ),
        customdata=result.scatter_dates,
        hovertemplate=(
            "Date: %{customdata}<br>"
            f"{input_label}: %{{x:.1f}}<br>"
            f"{outcome_label}: %{{y:.1f}}"
            "<extra></extra>"
        ),
    ))

    # OLS regression line
    x_arr = np.array(result.scatter_x)
    x_line = np.linspace(x_arr.min(), x_arr.max(), 100)
    y_line = result.regression_intercept + result.regression_slope * x_line
    fig.add_trace(go.Scatter(
        x=x_line, y=y_line,
        mode="lines", name="Regression line",
        line=dict(color=_palette["accent"], width=2, dash="dash"),
    ))

    # Stats annotation box
    sig_text = "p < 0.05" if result.p_value < 0.05 else f"p = {result.p_value:.4f}"
    annotation_text = (
        f"r = {result.pearson_r:.3f}<br>"
        f"R² = {result.r_squared:.3f}<br>"
        f"{sig_text}<br>"
        f"n = {result.n_observations}"
    )
    fig.add_annotation(
        x=0.02, y=0.98,
        xref="paper", yref="paper",
        text=annotation_text,
        showarrow=False,
        align="left",
        bgcolor="rgba(0,0,0,0.6)" if dark else "rgba(255,255,255,0.8)",
        bordercolor=_palette["text_muted"],
        borderwidth=1,
        borderpad=6,
        font=dict(size=11, color=_palette["text"]),
    )

    fig.update_xaxes(title_text=input_label)
    fig.update_yaxes(title_text=outcome_label)

    return _chart_layout(fig, f"{input_label} vs {outcome_label}", dark=dark)


def rolling_correlation_chart(
    result: CorrelationResult,
    input_label: str,
    outcome_label: str,
    dark: bool = True,
) -> go.Figure:
    """Rolling Pearson r over time with reference lines."""
    fig = go.Figure()

    # Rolling r line
    fig.add_trace(go.Scatter(
        x=result.rolling_r_dates,
        y=result.rolling_r_values,
        mode="lines",
        name="Rolling r",
        line=dict(color=_palette["primary"], width=2),
    ))

    # Zero line (no correlation)
    fig.add_hline(
        y=0, line_dash="dash", line_color=_palette["text_muted"],
        annotation_text="No correlation",
        annotation_position="bottom right",
    )

    # Moderate threshold lines at +/- 0.3
    fig.add_hline(
        y=0.3, line_dash="dot", line_color=_palette["success"],
        opacity=0.5,
    )
    fig.add_hline(
        y=-0.3, line_dash="dot", line_color=_palette["danger"],
        opacity=0.5,
    )

    # Overall r reference line
    fig.add_hline(
        y=result.pearson_r, line_dash="solid",
        line_color=_palette["accent"], line_width=1,
        annotation_text=f"Overall r = {result.pearson_r:.3f}",
        annotation_position="top right",
    )

    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Pearson r", range=[-1.05, 1.05])

    return _chart_layout(
        fig,
        f"Rolling Correlation: {input_label} vs {outcome_label}",
        dark=dark,
    )
