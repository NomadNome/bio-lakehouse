"""
Bio Insights Engine - Temperature Trend Analyzer

Tracks Oura ring body temperature deviations over time. Sustained elevation
(>0.3C for 3+ days) can signal illness onset, overtraining, or menstrual
cycle phase. Provides early-warning flags and correlates with readiness.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class TemperatureTrendAnalyzer(InsightAnalyzer):
    """Detects temperature anomalies that may signal illness or overtraining."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT day, temp_deviation, temp_trend_deviation,
               body_temp_score, temp_dev_7day_avg, temp_status,
               readiness_score
        FROM bio_gold.temperature_trends
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

        for col in ["temp_deviation", "temp_trend_deviation", "temp_dev_7day_avg",
                     "readiness_score", "body_temp_score"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        temp = df["temp_deviation"].dropna()

        # Detect consecutive elevated days (>0.3C)
        df["elevated"] = df["temp_deviation"].abs() > 0.3
        df["elevated_streak"] = 0
        streak = 0
        for i in range(len(df)):
            if df.iloc[i]["elevated"]:
                streak += 1
            else:
                streak = 0
            df.iloc[i, df.columns.get_loc("elevated_streak")] = streak

        max_streak = int(df["elevated_streak"].max()) if not df.empty else 0
        current_streak = int(df.iloc[-1]["elevated_streak"]) if not df.empty else 0

        # Current status
        latest = df.iloc[-1] if not df.empty else None
        current_temp = float(latest["temp_deviation"]) if latest is not None else 0
        current_trend = float(latest["temp_trend_deviation"]) if latest is not None else 0
        current_7d_avg = float(latest["temp_dev_7day_avg"]) if latest is not None else 0

        # Correlation: temp deviation vs readiness
        corr_data = df.dropna(subset=["temp_deviation", "readiness_score"])
        temp_readiness_corr = float(
            corr_data["temp_deviation"].corr(corr_data["readiness_score"])
        ) if len(corr_data) >= 7 else None

        # Alert logic
        if current_streak >= 3:
            alert = "warning"
            alert_msg = f"Temperature elevated for {current_streak} consecutive days"
        elif abs(current_temp) > 0.5:
            alert = "caution"
            alert_msg = f"Today's temperature deviation is notable ({current_temp:+.2f}C)"
        else:
            alert = "normal"
            alert_msg = "Temperature within normal range"

        statistics = {
            "n": len(df),
            "current_temp_dev": round(current_temp, 2),
            "current_trend_dev": round(current_trend, 2),
            "current_7d_avg": round(current_7d_avg, 2),
            "current_streak": current_streak,
            "max_streak": max_streak,
            "mean_deviation": round(float(temp.mean()), 2) if len(temp) > 0 else 0,
            "std_deviation": round(float(temp.std()), 2) if len(temp) > 0 else 0,
            "elevated_days_pct": round(float(df["elevated"].mean() * 100), 1),
            "temp_readiness_corr": round(temp_readiness_corr, 2) if temp_readiness_corr else None,
            "alert": alert,
            "alert_msg": alert_msg,
        }

        result = InsightResult(
            insight_type="temperature_trend",
            title="Body Temperature Trend",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 14:
            caveats.append(f"Only {len(df)} days of temperature data — patterns may not be reliable.")
        caveats.append("Temperature deviations are relative to your personal baseline, not absolute body temp.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            subplot_titles=("Temperature Deviation (C)", "Body Temperature Score"),
            row_heights=[0.6, 0.4],
        )

        # Temperature deviation bars colored by status
        colors = []
        for _, row in df.iterrows():
            if row["temp_status"] == "elevated":
                colors.append(theme.DANGER)
            elif row["temp_status"] == "moderate":
                colors.append(theme.WARNING)
            else:
                colors.append(theme.PRIMARY)

        fig.add_trace(go.Bar(
            x=df["day"], y=df["temp_deviation"],
            marker_color=colors,
            name="Temp Deviation",
            opacity=0.7,
        ), row=1, col=1)

        # 7-day moving average line
        fig.add_trace(go.Scatter(
            x=df["day"], y=df["temp_dev_7day_avg"],
            mode="lines",
            line=dict(color=theme.ACCENT, width=2.5),
            name="7-day avg",
        ), row=1, col=1)

        # Threshold lines
        fig.add_hline(y=0.3, row=1, col=1, line_dash="dot",
                       line_color=theme.WARNING, opacity=0.5)
        fig.add_hline(y=-0.3, row=1, col=1, line_dash="dot",
                       line_color=theme.WARNING, opacity=0.5)

        # Body temperature score (0-100)
        fig.add_trace(go.Scatter(
            x=df["day"], y=df["body_temp_score"],
            mode="lines+markers",
            line=dict(color=theme.SECONDARY, width=2),
            marker=dict(size=3),
            name="Body Temp Score",
        ), row=2, col=1)

        theme.style_figure(
            fig,
            n=s["n"],
            title="Body Temperature Trend",
            height=500,
        )
        fig.update_layout(showlegend=True, legend=dict(orientation="h", y=1.08))

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        parts = [f"**{s['alert_msg']}.**"]

        parts.append(
            f"Today's deviation is {s['current_temp_dev']:+.2f}C "
            f"(7-day avg: {s['current_7d_avg']:+.2f}C)."
        )

        if s["elevated_days_pct"] > 30:
            parts.append(
                f"{s['elevated_days_pct']:.0f}% of your days show elevated temperature — "
                "consider whether recovery, sleep, or illness may be a factor."
            )

        if s["temp_readiness_corr"] is not None:
            direction = "negatively" if s["temp_readiness_corr"] < -0.2 else "weakly"
            if s["temp_readiness_corr"] < -0.2:
                parts.append(
                    f"Temperature deviations correlate {direction} with readiness "
                    f"(r={s['temp_readiness_corr']:.2f}) — higher temps tend to lower your readiness."
                )

        return " ".join(parts)
