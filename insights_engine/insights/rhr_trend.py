"""
Bio Insights Engine - Resting Heart Rate Trend Analyzer

Tracks resting heart rate (RHR) as an early indicator of overtraining,
illness, or improving cardiovascular fitness. Elevated RHR often appears
before subjective symptoms, making it a useful early-warning signal.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class RHRTrendAnalyzer(InsightAnalyzer):
    """Tracks resting heart rate trends and flags elevated periods."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT date, resting_heart_rate_bpm, readiness_score
        FROM bio_gold_gold.gold_daily_rollup
        WHERE resting_heart_rate_bpm IS NOT NULL
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
        df["rhr"] = pd.to_numeric(df["resting_heart_rate_bpm"], errors="coerce")
        df["readiness_score"] = pd.to_numeric(df["readiness_score"], errors="coerce")

        # Rolling averages
        df["rhr_7d"] = df["rhr"].rolling(7, min_periods=3).mean()
        df["rhr_30d"] = df["rhr"].rolling(30, min_periods=7).mean()

        # Baseline
        baseline = float(df["rhr"].mean())
        baseline_std = float(df["rhr"].std())

        # Current state
        latest = df.iloc[-1] if not df.empty else None
        current_rhr = float(latest["rhr"]) if latest is not None else 0
        current_7d = float(latest["rhr_7d"]) if latest is not None and pd.notna(latest["rhr_7d"]) else None
        current_30d = float(latest["rhr_30d"]) if latest is not None and pd.notna(latest["rhr_30d"]) else None

        # Status: elevated RHR = bad (inverse of HRV)
        if current_7d and current_30d:
            diff_bpm = current_7d - current_30d
            if diff_bpm > 3:
                status = "elevated"
                status_msg = f"Resting HR is elevated — 7-day avg is {diff_bpm:.0f} bpm above your 30-day baseline"
            elif diff_bpm < -3:
                status = "low"
                status_msg = f"Resting HR is lower than usual — a sign of good recovery"
            else:
                status = "normal"
                status_msg = "Resting heart rate is within your normal range"
        else:
            status = "normal"
            status_msg = "Resting heart rate is within your normal range"
            diff_bpm = 0

        # Consecutive elevated days (>1 std above baseline)
        threshold = baseline + baseline_std
        df["elevated"] = df["rhr"] > threshold
        streak = 0
        current_streak = 0
        for i in range(len(df)):
            if df.iloc[i]["elevated"]:
                streak += 1
            else:
                streak = 0
            if i == len(df) - 1:
                current_streak = streak

        # Correlation with readiness (expect negative — higher RHR = lower readiness)
        corr_data = df.dropna(subset=["rhr", "readiness_score"])
        rhr_readiness_corr = float(
            corr_data["rhr"].corr(corr_data["readiness_score"])
        ) if len(corr_data) >= 14 else None

        # Monthly trend
        if len(df) >= 60:
            recent_30 = df["rhr"].tail(30).mean()
            prior_30 = df["rhr"].iloc[-60:-30].mean()
            monthly_trend = float(recent_30 - prior_30)
        else:
            monthly_trend = None

        statistics = {
            "n": len(df),
            "current_rhr": round(current_rhr, 1),
            "current_7d": round(current_7d, 1) if current_7d else None,
            "current_30d": round(current_30d, 1) if current_30d else None,
            "baseline": round(baseline, 1),
            "baseline_std": round(baseline_std, 1),
            "threshold": round(threshold, 1),
            "status": status,
            "status_msg": status_msg,
            "diff_bpm": round(diff_bpm, 1),
            "current_streak": current_streak,
            "rhr_readiness_corr": round(rhr_readiness_corr, 2) if rhr_readiness_corr else None,
            "monthly_trend": round(monthly_trend, 1) if monthly_trend else None,
        }

        result = InsightResult(
            insight_type="rhr_trend",
            title="Resting Heart Rate Trend",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 30:
            caveats.append(f"Only {len(df)} days of RHR data — baseline may not be stable.")
        caveats.append("RHR can be affected by caffeine, alcohol, hydration, and sleep position.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        fig = go.Figure()

        # Daily RHR scatter (faint)
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rhr"],
            mode="markers",
            marker=dict(color=theme.SECONDARY, size=3, opacity=0.25),
            name="Daily RHR",
            showlegend=False,
        ))

        # 7-day rolling avg
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rhr_7d"],
            mode="lines",
            line=dict(color=theme.SECONDARY, width=3),
            name="7-day avg",
        ))

        # 30-day rolling avg
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["rhr_30d"],
            mode="lines",
            line=dict(color=theme.ACCENT, width=2, dash="dash"),
            name="30-day avg",
        ))

        # Elevated threshold line
        fig.add_hline(
            y=s["threshold"], line_dash="dot",
            line_color=theme.DANGER, opacity=0.5,
            annotation_text=f"Elevated ({s['threshold']:.0f} bpm)",
        )

        theme.style_figure(
            fig,
            n=s["n"],
            title="Resting Heart Rate (bpm)",
            height=400,
        )
        fig.update_yaxes(title_text="RHR (bpm)")
        fig.update_layout(showlegend=True, legend=dict(orientation="h", y=1.08))

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        parts = [f"**{s['status_msg']}.**"]

        if s["current_7d"] and s["current_30d"]:
            parts.append(
                f"Your 7-day avg is {s['current_7d']:.0f} bpm "
                f"vs 30-day baseline of {s['current_30d']:.0f} bpm."
            )

        if s["current_streak"] >= 3:
            parts.append(
                f"RHR has been elevated for {s['current_streak']} consecutive days — "
                "consider extra recovery."
            )

        if s["monthly_trend"] is not None:
            if s["monthly_trend"] > 1:
                parts.append(
                    f"30-day trend is **up {s['monthly_trend']:.0f} bpm** vs the prior month — watch for overtraining."
                )
            elif s["monthly_trend"] < -1:
                parts.append(
                    f"30-day trend is **down {abs(s['monthly_trend']):.0f} bpm** vs the prior month — fitness is improving."
                )

        return " ".join(parts)
