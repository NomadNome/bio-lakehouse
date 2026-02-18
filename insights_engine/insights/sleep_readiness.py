"""
Bio Insights Engine - Signature Insight 3a: Sleep → Readiness Correlation

Pearson correlation between sleep score and next-day readiness,
with scatter plot + regression line.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from scipy import stats

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class SleepReadinessAnalyzer(InsightAnalyzer):
    """Analyzes correlation between sleep and next-day readiness."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT
            sleep_date,
            performance_date,
            prev_night_sleep AS sleep_score,
            sleep_quality,
            next_day_readiness,
            next_day_output
        FROM bio_gold.sleep_performance_prediction
        WHERE prev_night_sleep IS NOT NULL
          AND next_day_readiness IS NOT NULL
        ORDER BY sleep_date
        """
        df = self.athena.execute_query(sql)

        if date_range:
            df = df[
                (pd.to_datetime(df["sleep_date"]).dt.date >= date_range.start)
                & (pd.to_datetime(df["sleep_date"]).dt.date <= date_range.end)
            ]

        n = len(df)
        if n < 5:
            return InsightResult(
                insight_type="sleep_readiness",
                title="Sleep → Readiness Correlation",
                narrative="Insufficient data for sleep-readiness analysis.",
                statistics={"n": n},
                caveats=["Fewer than 5 paired observations available."],
                data=df,
            )

        x = df["sleep_score"].astype(float).values
        y = df["next_day_readiness"].astype(float).values

        r, p_value = stats.pearsonr(x, y)
        slope, intercept, _, _, std_err = stats.linregress(x, y)

        strength = (
            "strong" if abs(r) >= 0.6
            else "moderate" if abs(r) >= 0.3
            else "weak"
        )
        direction = "positive" if r > 0 else "negative"

        statistics = {
            "r": round(r, 3),
            "p_value": round(p_value, 4),
            "n": n,
            "slope": round(slope, 3),
            "intercept": round(intercept, 1),
            "std_err": round(std_err, 4),
            "strength": strength,
            "direction": direction,
            "mean_sleep": round(float(np.mean(x)), 1),
            "mean_readiness": round(float(np.mean(y)), 1),
        }

        result = InsightResult(
            insight_type="sleep_readiness",
            title="Sleep → Readiness Correlation",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if n < 30:
            caveats.append(
                f"With only {n} data points, this correlation should be interpreted cautiously."
            )
        if p_value > 0.05:
            caveats.append(
                f"The correlation is not statistically significant (p={p_value:.3f})."
            )
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        x = df["sleep_score"].astype(float)
        y = df["next_day_readiness"].astype(float)

        fig = go.Figure()

        # Scatter points
        fig.add_trace(go.Scatter(
            x=x, y=y,
            mode="markers",
            marker=dict(color=theme.PRIMARY, size=8, opacity=0.7),
            name="Daily",
            hovertemplate="Sleep: %{x}<br>Readiness: %{y}<extra></extra>",
        ))

        # Regression line
        x_range = np.linspace(x.min(), x.max(), 100)
        y_pred = s["slope"] * x_range + s["intercept"]
        fig.add_trace(go.Scatter(
            x=x_range, y=y_pred,
            mode="lines",
            line=dict(color=theme.SECONDARY, width=2, dash="dash"),
            name=f"Trend (r={s['r']:.2f})",
        ))

        theme.style_figure(
            fig,
            n=s["n"],
            title="Sleep Score → Next-Day Readiness",
            xaxis_title="Sleep Score",
            yaxis_title="Next-Day Readiness Score",
        )
        fig.add_annotation(
            text=f"r = {s['r']:.3f}, p = {s['p_value']:.4f}, n = {s['n']}",
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            showarrow=False,
            font=dict(color=theme.TEXT_MUTED, size=11),
        )
        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        return (
            f"Your sleep score has a {s['strength']} {s['direction']} correlation "
            f"(r={s['r']:.2f}, p={s['p_value']:.3f}, n={s['n']}) with next-day readiness. "
            f"On average, a 1-point increase in sleep score is associated with a "
            f"{abs(s['slope']):.1f}-point {'increase' if s['slope'] > 0 else 'decrease'} "
            f"in readiness. Your mean sleep score is {s['mean_sleep']} and mean readiness "
            f"is {s['mean_readiness']}."
        )
