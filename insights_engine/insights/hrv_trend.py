"""
Bio Insights Engine - HRV Trend Analyzer

Tracks heart rate variability (HRV) over time as a recovery and
autonomic-nervous-system health indicator. Flags when current HRV
is suppressed relative to your personal baseline and correlates
with readiness scores.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go

from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class HRVTrendAnalyzer(InsightAnalyzer):
    """Tracks HRV trends and flags suppressed recovery periods."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT date, hrv_ms, readiness_score
        FROM bio_gold_gold.gold_daily_rollup
        WHERE hrv_ms IS NOT NULL
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
        df["hrv_ms"] = pd.to_numeric(df["hrv_ms"], errors="coerce")
        df["readiness_score"] = pd.to_numeric(df["readiness_score"], errors="coerce")

        # Rolling averages
        df["hrv_7d"] = df["hrv_ms"].rolling(7, min_periods=3).mean()
        df["hrv_30d"] = df["hrv_ms"].rolling(30, min_periods=7).mean()

        # Overall baseline
        baseline = float(df["hrv_ms"].mean())
        baseline_std = float(df["hrv_ms"].std())

        # Current state
        latest = df.iloc[-1] if not df.empty else None
        current_hrv = float(latest["hrv_ms"]) if latest is not None else 0
        current_7d = float(latest["hrv_7d"]) if latest is not None and pd.notna(latest["hrv_7d"]) else None
        current_30d = float(latest["hrv_30d"]) if latest is not None and pd.notna(latest["hrv_30d"]) else None

        # Status: how does current 7d avg compare to 30d baseline?
        if current_7d and current_30d:
            diff_pct = (current_7d - current_30d) / current_30d * 100
            if diff_pct < -15:
                status = "suppressed"
                status_msg = f"HRV is suppressed — 7-day avg is {abs(diff_pct):.0f}% below your 30-day baseline"
            elif diff_pct > 15:
                status = "elevated"
                status_msg = f"HRV is elevated — 7-day avg is {diff_pct:.0f}% above your 30-day baseline"
            else:
                status = "normal"
                status_msg = "HRV is within your normal range"
        else:
            status = "normal"
            status_msg = "HRV is within your normal range"
            diff_pct = 0

        # Correlation with readiness
        corr_data = df.dropna(subset=["hrv_ms", "readiness_score"])
        hrv_readiness_corr = float(
            corr_data["hrv_ms"].corr(corr_data["readiness_score"])
        ) if len(corr_data) >= 14 else None

        # Trend: compare last 30d avg to prior 30d avg
        if len(df) >= 60:
            recent_30 = df["hrv_ms"].tail(30).mean()
            prior_30 = df["hrv_ms"].iloc[-60:-30].mean()
            monthly_trend = float(recent_30 - prior_30)
        else:
            monthly_trend = None

        statistics = {
            "n": len(df),
            "current_hrv": round(current_hrv, 1),
            "current_7d": round(current_7d, 1) if current_7d else None,
            "current_30d": round(current_30d, 1) if current_30d else None,
            "baseline": round(baseline, 1),
            "baseline_std": round(baseline_std, 1),
            "status": status,
            "status_msg": status_msg,
            "diff_pct": round(diff_pct, 1),
            "hrv_readiness_corr": round(hrv_readiness_corr, 2) if hrv_readiness_corr else None,
            "monthly_trend": round(monthly_trend, 1) if monthly_trend else None,
        }

        result = InsightResult(
            insight_type="hrv_trend",
            title="HRV Trend",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 30:
            caveats.append(f"Only {len(df)} days of HRV data — baseline may not be stable.")
        caveats.append("HRV is highly variable day-to-day. Focus on the 7-day trend, not individual readings.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        # Show last 365 days for consistency with other charts
        cutoff = pd.Timestamp(datetime.now() - timedelta(days=365))
        plot_df = df[df["date"] >= cutoff]

        fig = go.Figure()

        # 7-day rolling avg
        fig.add_trace(go.Scatter(
            x=plot_df["date"], y=plot_df["hrv_7d"],
            mode="lines",
            line=dict(color=theme.PRIMARY, width=3),
            name="7-day avg",
        ))

        # 30-day rolling avg
        fig.add_trace(go.Scatter(
            x=plot_df["date"], y=plot_df["hrv_30d"],
            mode="lines",
            line=dict(color=theme.ACCENT, width=2, dash="dash"),
            name="30-day baseline",
        ))

        theme.style_figure(
            fig,
            n=s["n"],
            title="Heart Rate Variability (ms)",
            height=400,
        )
        fig.update_yaxes(title_text="HRV (ms)")
        fig.update_layout(showlegend=True, legend=dict(orientation="h", y=1.08))

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        parts = [f"**{s['status_msg']}.**"]

        if s["current_7d"] and s["current_30d"]:
            parts.append(
                f"Your 7-day avg is {s['current_7d']:.0f} ms "
                f"vs 30-day baseline of {s['current_30d']:.0f} ms."
            )

        if s["monthly_trend"] is not None:
            direction = "up" if s["monthly_trend"] > 0 else "down"
            parts.append(
                f"30-day trend is **{direction} {abs(s['monthly_trend']):.0f} ms** vs the prior month."
            )

        if s["hrv_readiness_corr"] is not None and abs(s["hrv_readiness_corr"]) > 0.2:
            parts.append(
                f"HRV correlates with readiness (r={s['hrv_readiness_corr']:.2f})."
            )

        return " ".join(parts)
