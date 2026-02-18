"""
Bio Insights Engine - Signature Insight 3d: Anomaly Detection

Flags days where readiness drops >1.5 standard deviations below personal mean,
and detects missed workout streaks.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class AnomalyDetectionAnalyzer(InsightAnalyzer):
    """Detects readiness anomalies and missed workout streaks."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT
            date, readiness_score, sleep_score, had_workout,
            combined_wellness_score, disciplines
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
        mean_r = float(readiness.mean())
        std_r = float(readiness.std())
        threshold = mean_r - 1.5 * std_r

        # Flag anomaly days
        df["is_anomaly"] = readiness < threshold
        anomalies = df[df["is_anomaly"]].copy()

        # Detect missed workout streaks (3+ consecutive workout days, then a gap)
        df["had_workout_bool"] = df["had_workout"].astype(str).str.lower() == "true"
        missed_workouts = []
        streak = 0
        for i, row in df.iterrows():
            if row["had_workout_bool"]:
                streak += 1
            else:
                if streak >= 3:
                    missed_workouts.append({
                        "date": row["date"],
                        "streak_broken": streak,
                        "readiness": row["readiness_score"],
                    })
                streak = 0

        # Build context for each anomaly
        anomaly_details = []
        for _, row in anomalies.iterrows():
            detail = {
                "date": row["date"].strftime("%Y-%m-%d"),
                "readiness": float(row["readiness_score"]),
                "sleep": float(row["sleep_score"]) if pd.notna(row["sleep_score"]) else None,
                "deviation": round((float(row["readiness_score"]) - mean_r) / std_r, 1),
            }
            anomaly_details.append(detail)

        statistics = {
            "mean_readiness": round(mean_r, 1),
            "std_readiness": round(std_r, 1),
            "threshold": round(threshold, 1),
            "anomaly_count": len(anomalies),
            "anomaly_details": anomaly_details,
            "missed_workout_breaks": missed_workouts,
            "total_days": len(df),
        }

        result = InsightResult(
            insight_type="anomaly_detection",
            title="Anomaly Detection",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)
        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        fig = go.Figure()

        # Normal days
        normal = df[~df["is_anomaly"]]
        fig.add_trace(go.Scatter(
            x=normal["date"], y=normal["readiness_score"],
            mode="lines+markers",
            line=dict(color=theme.PRIMARY, width=1.5),
            marker=dict(size=4, color=theme.PRIMARY),
            name="Readiness",
        ))

        # Anomaly days (highlighted)
        anomalies = df[df["is_anomaly"]]
        fig.add_trace(go.Scatter(
            x=anomalies["date"], y=anomalies["readiness_score"],
            mode="markers",
            marker=dict(color=theme.DANGER, size=12, symbol="diamond",
                        line=dict(width=2, color="white")),
            name="Anomaly",
        ))

        # Threshold line
        fig.add_hline(
            y=s["threshold"], line_dash="dot",
            line_color=theme.WARNING,
            annotation_text=f"Threshold ({s['threshold']:.0f})",
            annotation_font_color=theme.WARNING,
        )

        # Mean line
        fig.add_hline(
            y=s["mean_readiness"], line_dash="dash",
            line_color=theme.TEXT_MUTED,
            annotation_text=f"Mean ({s['mean_readiness']:.0f})",
            annotation_font_color=theme.TEXT_MUTED,
        )

        theme.style_figure(
            fig,
            title=f"Anomaly Detection ({s['anomaly_count']} anomalies in {s['total_days']} days)",
            xaxis_title="Date",
            yaxis_title="Readiness Score",
        )
        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        n_anom = s["anomaly_count"]

        narrative = (
            f"{n_anom} anomalies detected in the past {s['total_days']} days "
            f"(readiness below {s['threshold']:.0f}, which is 1.5 std devs below your mean of {s['mean_readiness']:.0f}). "
        )

        if s["anomaly_details"]:
            worst = min(s["anomaly_details"], key=lambda x: x["readiness"])
            narrative += (
                f"The biggest drop was to {worst['readiness']:.0f} on {worst['date']}"
            )
            if worst["sleep"]:
                narrative += f" (sleep score: {worst['sleep']:.0f})"
            narrative += ". "

        if s["missed_workout_breaks"]:
            narrative += (
                f"{len(s['missed_workout_breaks'])} workout streak breaks detected "
                f"(after 3+ consecutive workout days)."
            )

        return narrative
