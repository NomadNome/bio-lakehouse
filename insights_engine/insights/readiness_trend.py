"""
Bio Insights Engine - Signature Insight 3c: Weekly Readiness Trends

Time series of daily readiness with 7-day and 14-day rolling averages,
plus trend direction indicator.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from scipy import stats

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class ReadinessTrendAnalyzer(InsightAnalyzer):
    """Tracks readiness trend with rolling averages and direction indicator."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT date, readiness_score, sleep_score, readiness_7day_avg, sleep_7day_avg
        FROM bio_gold.dashboard_30day
        WHERE readiness_score IS NOT NULL
        ORDER BY date
        """
        df = self.athena.execute_query(sql)
        df["date"] = pd.to_datetime(df["date"])

        if date_range:
            df = df[
                (df["date"].dt.date >= date_range.start)
                & (df["date"].dt.date <= date_range.end)
            ]

        df = df.sort_values("date").reset_index(drop=True)

        readiness = df["readiness_score"].astype(float)

        # Compute rolling averages
        df["ma_7"] = readiness.rolling(7, min_periods=3).mean()
        df["ma_14"] = readiness.rolling(14, min_periods=7).mean()

        # Trend direction from slope of 14-day MA over last 14 days
        recent_ma = df["ma_14"].dropna().tail(14)
        if len(recent_ma) >= 7:
            x = np.arange(len(recent_ma))
            slope, _, _, _, _ = stats.linregress(x, recent_ma.values)
            if slope > 0.3:
                trend = "improving"
                trend_icon = "↑"
            elif slope < -0.3:
                trend = "declining"
                trend_icon = "↓"
            else:
                trend = "stable"
                trend_icon = "→"
        else:
            slope = 0.0
            trend = "insufficient_data"
            trend_icon = "?"

        # Identify notable weeks
        df["week"] = df["date"].dt.isocalendar().week.astype(int)
        weekly = df.groupby("week")["readiness_score"].agg(["mean", "count"]).reset_index()
        overall_mean = float(readiness.mean())
        overall_std = float(readiness.std())
        notable_weeks = weekly[
            (weekly["mean"] > overall_mean + overall_std)
            | (weekly["mean"] < overall_mean - overall_std)
        ]

        statistics = {
            "n": len(df),
            "mean": round(overall_mean, 1),
            "std": round(overall_std, 1),
            "min": round(float(readiness.min()), 1),
            "max": round(float(readiness.max()), 1),
            "trend": trend,
            "trend_icon": trend_icon,
            "slope_14d": round(float(slope), 3),
            "notable_weeks_count": len(notable_weeks),
            "current_7d_avg": round(float(df["ma_7"].iloc[-1]), 1) if not df["ma_7"].isna().all() else None,
        }

        result = InsightResult(
            insight_type="readiness_trend",
            title=f"Readiness Trend {trend_icon}",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 30:
            caveats.append(f"Only {len(df)} days of data — trends may not be reliable.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        fig = go.Figure()

        # Daily points
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["readiness_score"],
            mode="markers",
            marker=dict(color=theme.PRIMARY, size=5, opacity=0.5),
            name="Daily",
        ))

        # 7-day MA
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["ma_7"],
            mode="lines",
            line=dict(color=theme.ACCENT, width=2),
            name="7-day avg",
        ))

        # 14-day MA
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["ma_14"],
            mode="lines",
            line=dict(color=theme.SECONDARY, width=2, dash="dash"),
            name="14-day avg",
        ))

        s = result.statistics
        theme.style_figure(
            fig,
            title=f"Readiness Trend ({s['trend_icon']} {s['trend']})",
            xaxis_title="Date",
            yaxis_title="Readiness Score",
        )
        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        narrative = (
            f"Your readiness is currently {s['trend']} {s['trend_icon']}. "
            f"Over {s['n']} days, your average readiness is {s['mean']} "
            f"(range: {s['min']}–{s['max']}). "
        )
        if s["current_7d_avg"]:
            narrative += f"Your current 7-day average is {s['current_7d_avg']}. "
        if s["notable_weeks_count"] > 0:
            narrative += f"{s['notable_weeks_count']} weeks stood out as notably above or below your baseline."
        return narrative
