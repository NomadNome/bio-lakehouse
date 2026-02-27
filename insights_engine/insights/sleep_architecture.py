"""
Bio Insights Engine - Sleep Architecture Analyzer

Tracks Oura deep sleep and REM sleep contributor scores over time.
These two stages are the most restorative and actionable components
of sleep quality. Shows 7-day rolling averages and recent trends.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme

FOCUS_FACTORS = {
    "deep_sleep": "Deep Sleep",
    "rem_sleep": "REM Sleep",
}


class SleepArchitectureAnalyzer(InsightAnalyzer):
    """Tracks deep sleep and REM sleep contributor scores over time."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT day, sleep_score, deep_sleep, rem_sleep
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

        for col in ["sleep_score", "deep_sleep", "rem_sleep"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Averages
        deep_vals = df["deep_sleep"].dropna()
        rem_vals = df["rem_sleep"].dropna()
        deep_mean = round(float(deep_vals.mean()), 1) if len(deep_vals) > 0 else None
        rem_mean = round(float(rem_vals.mean()), 1) if len(rem_vals) > 0 else None

        # Recent 7-day averages
        deep_recent = df["deep_sleep"].tail(7).dropna()
        rem_recent = df["rem_sleep"].tail(7).dropna()
        deep_recent_avg = round(float(deep_recent.mean()), 1) if len(deep_recent) > 0 else None
        rem_recent_avg = round(float(rem_recent.mean()), 1) if len(rem_recent) > 0 else None

        # 7-day rolling averages for chart
        df["deep_sleep_7d"] = df["deep_sleep"].rolling(7, min_periods=3).mean()
        df["rem_sleep_7d"] = df["rem_sleep"].rolling(7, min_periods=3).mean()

        # Correlations with overall sleep score
        correlations = {}
        for factor in FOCUS_FACTORS:
            corr_data = df.dropna(subset=[factor, "sleep_score"])
            if len(corr_data) >= 7:
                correlations[factor] = round(
                    float(corr_data[factor].corr(corr_data["sleep_score"])), 2
                )

        statistics = {
            "n": len(df),
            "sleep_mean": round(float(df["sleep_score"].mean()), 1) if len(df) > 0 else None,
            "deep_mean": deep_mean,
            "rem_mean": rem_mean,
            "deep_recent_7d": deep_recent_avg,
            "rem_recent_7d": rem_recent_avg,
            "correlations": correlations,
        }

        result = InsightResult(
            insight_type="sleep_architecture",
            title="Deep & REM Sleep",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 14:
            caveats.append(f"Only {len(df)} nights of data — trends may not be stable.")
        caveats.append("Oura contributor scores are 0-100 relative quality measures, not minutes.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        fig = go.Figure()

        # Deep sleep 7-day rolling avg
        fig.add_trace(go.Scatter(
            x=df["day"], y=df["deep_sleep_7d"],
            mode="lines",
            line=dict(color=theme.PRIMARY, width=3),
            name=f"Deep Sleep (avg {s['deep_mean']:.0f})" if s["deep_mean"] else "Deep Sleep",
        ))

        # REM sleep 7-day rolling avg
        fig.add_trace(go.Scatter(
            x=df["day"], y=df["rem_sleep_7d"],
            mode="lines",
            line=dict(color=theme.ACCENT, width=3),
            name=f"REM Sleep (avg {s['rem_mean']:.0f})" if s["rem_mean"] else "REM Sleep",
        ))

        # Light scatter of raw daily values behind the lines
        fig.add_trace(go.Scatter(
            x=df["day"], y=df["deep_sleep"],
            mode="markers",
            marker=dict(color=theme.PRIMARY, size=3, opacity=0.3),
            name="Deep (daily)",
            showlegend=False,
        ))
        fig.add_trace(go.Scatter(
            x=df["day"], y=df["rem_sleep"],
            mode="markers",
            marker=dict(color=theme.ACCENT, size=3, opacity=0.3),
            name="REM (daily)",
            showlegend=False,
        ))

        theme.style_figure(
            fig,
            n=s["n"],
            title="Deep & REM Sleep Quality (7-Day Avg)",
            height=400,
        )
        fig.update_yaxes(title_text="Oura Score (0-100)", range=[0, 105])
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="h", y=1.08),
        )

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        parts = []

        if s["deep_mean"] and s["rem_mean"]:
            parts.append(
                f"Your average deep sleep score is **{s['deep_mean']:.0f}** "
                f"and REM sleep score is **{s['rem_mean']:.0f}**."
            )

        # Recent vs overall trend
        if s["deep_mean"] and s["deep_recent_7d"]:
            diff = s["deep_recent_7d"] - s["deep_mean"]
            if abs(diff) > 3:
                direction = "up" if diff > 0 else "down"
                parts.append(
                    f"Deep sleep is trending **{direction}** recently "
                    f"({s['deep_recent_7d']:.0f} vs {s['deep_mean']:.0f} avg)."
                )

        if s["rem_mean"] and s["rem_recent_7d"]:
            diff = s["rem_recent_7d"] - s["rem_mean"]
            if abs(diff) > 3:
                direction = "up" if diff > 0 else "down"
                parts.append(
                    f"REM sleep is trending **{direction}** recently "
                    f"({s['rem_recent_7d']:.0f} vs {s['rem_mean']:.0f} avg)."
                )

        if s["correlations"]:
            for factor, label in FOCUS_FACTORS.items():
                corr = s["correlations"].get(factor)
                if corr and abs(corr) > 0.3:
                    parts.append(
                        f"{label} correlates with overall sleep score (r={corr:.2f})."
                    )

        return " ".join(parts) if parts else "Insufficient data for sleep analysis."
