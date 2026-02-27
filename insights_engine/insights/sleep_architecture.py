"""
Bio Insights Engine - Sleep Architecture Analyzer

Breaks down Oura sleep quality into its component scores: deep sleep,
REM sleep, efficiency, latency, restfulness, timing, and total sleep.
Identifies which sleep factors are strongest/weakest and tracks trends.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme

SLEEP_FACTORS = {
    "deep_sleep": "Deep Sleep",
    "rem_sleep": "REM Sleep",
    "efficiency": "Efficiency",
    "latency": "Latency",
    "restfulness": "Restfulness",
    "timing": "Timing",
    "total_sleep": "Total Sleep",
}


class SleepArchitectureAnalyzer(InsightAnalyzer):
    """Analyzes sleep quality by breaking down Oura sleep contributor scores."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT day, sleep_score, deep_sleep, efficiency, latency,
               rem_sleep, restfulness, timing, total_sleep
        FROM bio_gold.sleep_architecture
        ORDER BY day
        """
        df = self.athena.execute_query(sql)
        df["day"] = pd.to_datetime(df["day"])

        if date_range:
            df = df[
                (df["day"].dt.date >= date_range.start)
                & (df["day"].dt.date <= date_range.end)
            ]

        df = df.sort_values("day").reset_index(drop=True)

        numeric_cols = ["sleep_score"] + list(SLEEP_FACTORS.keys())
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Factor averages and rankings
        factor_means = {}
        factor_recent_7d = {}
        for factor in SLEEP_FACTORS:
            vals = df[factor].dropna()
            factor_means[factor] = round(float(vals.mean()), 1) if len(vals) > 0 else None
            recent = df[factor].tail(7).dropna()
            factor_recent_7d[factor] = round(float(recent.mean()), 1) if len(recent) > 0 else None

        # Rank factors by average score (lowest = weakest link)
        ranked = sorted(
            [(k, v) for k, v in factor_means.items() if v is not None],
            key=lambda x: x[1],
        )
        weakest = ranked[0] if ranked else (None, None)
        strongest = ranked[-1] if ranked else (None, None)

        # 7-day rolling averages for each factor
        for factor in SLEEP_FACTORS:
            df[f"{factor}_7d"] = df[factor].rolling(7, min_periods=3).mean()

        # Compute factor-to-score correlations
        correlations = {}
        for factor in SLEEP_FACTORS:
            corr_data = df.dropna(subset=[factor, "sleep_score"])
            if len(corr_data) >= 7:
                correlations[factor] = round(
                    float(corr_data[factor].corr(corr_data["sleep_score"])), 2
                )

        # Most impactful factor (highest absolute correlation)
        most_impactful = max(correlations, key=lambda k: abs(correlations[k])) if correlations else None

        statistics = {
            "n": len(df),
            "sleep_mean": round(float(df["sleep_score"].mean()), 1) if len(df) > 0 else None,
            "factor_means": factor_means,
            "factor_recent_7d": factor_recent_7d,
            "weakest_factor": weakest[0],
            "weakest_score": weakest[1],
            "strongest_factor": strongest[0],
            "strongest_score": strongest[1],
            "correlations": correlations,
            "most_impactful": most_impactful,
        }

        result = InsightResult(
            insight_type="sleep_architecture",
            title="Sleep Architecture",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 14:
            caveats.append(f"Only {len(df)} nights of data — factor trends may not be stable.")
        caveats.append("Oura contributor scores are 0-100 relative measures, not absolute durations.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.12,
            subplot_titles=(
                "Sleep Factor Scores (7-Day Avg)",
                "Factor Averages vs Overall Sleep Score",
            ),
            row_heights=[0.6, 0.4],
            specs=[[{"type": "scatter"}], [{"type": "bar"}]],
        )

        # Time series of 7-day rolling averages for key factors
        colors = [theme.PRIMARY, theme.ACCENT, theme.SECONDARY,
                  theme.WARNING, theme.DANGER, theme.SUCCESS, theme.TEXT_MUTED]
        for i, (factor, label) in enumerate(SLEEP_FACTORS.items()):
            col_7d = f"{factor}_7d"
            if col_7d in df.columns:
                fig.add_trace(go.Scatter(
                    x=df["day"], y=df[col_7d],
                    mode="lines",
                    line=dict(color=colors[i % len(colors)], width=2),
                    name=label,
                ), row=1, col=1)

        # Bar chart: factor averages with color coding
        factor_labels = [SLEEP_FACTORS[f] for f in SLEEP_FACTORS if s["factor_means"].get(f) is not None]
        factor_vals = [s["factor_means"][f] for f in SLEEP_FACTORS if s["factor_means"].get(f) is not None]

        bar_colors = []
        for v in factor_vals:
            if v >= 85:
                bar_colors.append(theme.SUCCESS)
            elif v >= 70:
                bar_colors.append(theme.ACCENT)
            elif v >= 55:
                bar_colors.append(theme.WARNING)
            else:
                bar_colors.append(theme.DANGER)

        fig.add_trace(go.Bar(
            x=factor_labels, y=factor_vals,
            marker_color=bar_colors,
            name="Avg Score",
            text=[f"{v:.0f}" for v in factor_vals],
            textposition="outside",
            showlegend=False,
        ), row=2, col=1)

        # Add sleep score reference line
        if s["sleep_mean"]:
            fig.add_hline(
                y=s["sleep_mean"], row=2, col=1,
                line_dash="dash", line_color=theme.TEXT_MUTED,
                annotation_text=f"Overall Sleep: {s['sleep_mean']:.0f}",
            )

        theme.style_figure(
            fig,
            n=s["n"],
            title="Sleep Architecture Breakdown",
            height=600,
        )
        fig.update_layout(showlegend=True, legend=dict(orientation="h", y=1.08))

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        parts = []

        if s["weakest_factor"] and s["strongest_factor"]:
            parts.append(
                f"Your strongest sleep factor is **{SLEEP_FACTORS[s['strongest_factor']]}** "
                f"(avg {s['strongest_score']:.0f}) and your weakest is "
                f"**{SLEEP_FACTORS[s['weakest_factor']]}** (avg {s['weakest_score']:.0f})."
            )

        if s["most_impactful"] and s["correlations"]:
            corr_val = s["correlations"][s["most_impactful"]]
            parts.append(
                f"**{SLEEP_FACTORS[s['most_impactful']]}** has the strongest correlation "
                f"with your overall sleep score (r={corr_val:.2f})."
            )

        # Compare recent 7d to overall averages
        improving = []
        declining = []
        for factor in SLEEP_FACTORS:
            overall = s["factor_means"].get(factor)
            recent = s["factor_recent_7d"].get(factor)
            if overall and recent:
                diff = recent - overall
                if diff > 3:
                    improving.append(SLEEP_FACTORS[factor])
                elif diff < -3:
                    declining.append(SLEEP_FACTORS[factor])

        if improving:
            parts.append(f"Recently improving: {', '.join(improving)}.")
        if declining:
            parts.append(f"Recently declining: {', '.join(declining)} — worth monitoring.")

        return " ".join(parts) if parts else "Insufficient data for sleep architecture analysis."
