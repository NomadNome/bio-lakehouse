"""
Bio Insights Engine - Recovery Window Prediction

Estimates how many days it takes readiness to return to baseline
after workouts of different intensities. Uses the dbt
workout_recovery_windows model for pre-computed recovery trajectories.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class RecoveryWindowAnalyzer(InsightAnalyzer):
    """Estimates recovery duration by workout intensity."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT
            workout_date,
            workout_day_readiness,
            workout_day_sleep,
            total_workouts,
            total_minutes,
            total_calories,
            total_output_kj,
            max_avg_hr,
            intensity,
            readiness_7d_baseline,
            readiness_d1,
            readiness_d2,
            readiness_d3,
            readiness_delta_d1,
            readiness_delta_d2,
            readiness_delta_d3,
            days_to_recover
        FROM bio_gold.workout_recovery_windows
        ORDER BY workout_date
        """
        df = self.athena.execute_query(sql)

        if date_range:
            df = df[
                (pd.to_datetime(df["workout_date"]).dt.date >= date_range.start)
                & (pd.to_datetime(df["workout_date"]).dt.date <= date_range.end)
            ]

        if len(df) < 10:
            return InsightResult(
                insight_type="recovery_windows",
                title="Recovery Window Analysis",
                narrative=(
                    f"Not enough workout data yet ({len(df)} workouts, need 10+). "
                    "This insight will appear once more data accumulates."
                ),
                statistics={"total_n": len(df)},
                caveats=["Insufficient data."],
                data=df,
            )

        # Compute stats by intensity
        intensity_stats = {}
        for intensity in ["light", "moderate", "high"]:
            subset = df[df["intensity"] == intensity]
            if len(subset) < 3:
                continue

            recovery_days = subset["days_to_recover"].dropna()
            readiness_d1 = subset["readiness_delta_d1"].dropna().astype(float)

            intensity_stats[intensity] = {
                "n": len(subset),
                "avg_recovery_days": round(float(recovery_days.mean()), 1) if len(recovery_days) > 0 else None,
                "median_recovery_days": round(float(recovery_days.median()), 1) if len(recovery_days) > 0 else None,
                "pct_recovered_d1": round(float((recovery_days == 1).sum() / len(recovery_days) * 100), 0) if len(recovery_days) > 0 else None,
                "avg_readiness_drop_d1": round(float(readiness_d1.mean()), 1) if len(readiness_d1) > 0 else None,
                "avg_total_minutes": round(float(subset["total_minutes"].astype(float).mean()), 0),
                "avg_calories": round(float(subset["total_calories"].astype(float).mean()), 0),
            }

        # Overall recovery trajectory (average readiness delta by day)
        trajectory = {
            "d1": round(float(df["readiness_delta_d1"].dropna().astype(float).mean()), 1),
            "d2": round(float(df["readiness_delta_d2"].dropna().astype(float).mean()), 1),
            "d3": round(float(df["readiness_delta_d3"].dropna().astype(float).mean()), 1),
        }

        statistics = {
            "total_n": len(df),
            "intensity_breakdown": intensity_stats,
            "avg_trajectory": trajectory,
        }

        result = InsightResult(
            insight_type="recovery_windows",
            title="Recovery Window Analysis",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        for name, s in intensity_stats.items():
            if s["n"] < 10:
                caveats.append(f"{name.title()} intensity: only {s['n']} observations.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        fig = go.Figure()

        color_map = {
            "light": theme.SECONDARY,
            "moderate": theme.WARNING,
            "high": theme.DANGER if hasattr(theme, "DANGER") else "#EF4444",
        }

        # Recovery trajectory by intensity (line chart)
        for intensity in ["light", "moderate", "high"]:
            subset = df[df["intensity"] == intensity]
            if len(subset) < 3:
                continue

            days = [0, 1, 2, 3]
            means = [
                0,  # Day 0 = workout day (baseline delta = 0)
                float(subset["readiness_delta_d1"].dropna().astype(float).mean()),
                float(subset["readiness_delta_d2"].dropna().astype(float).mean()),
                float(subset["readiness_delta_d3"].dropna().astype(float).mean()),
            ]

            fig.add_trace(go.Scatter(
                x=days,
                y=means,
                mode="lines+markers",
                name=f"{intensity.title()} (n={len(subset)})",
                line=dict(color=color_map.get(intensity, theme.PRIMARY), width=3),
                marker=dict(size=10),
            ))

        # Zero baseline line
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

        theme.style_figure(
            fig,
            n=result.statistics["total_n"],
            title="Readiness Recovery Trajectory by Workout Intensity",
            xaxis_title="Days After Workout",
            yaxis_title="Readiness Change (vs. Workout Day)",
        )
        fig.update_xaxes(tickvals=[0, 1, 2, 3], ticktext=["Workout", "+1 Day", "+2 Days", "+3 Days"])
        return fig

    def narrate(self, result: InsightResult) -> str:
        stats = result.statistics
        breakdown = stats["intensity_breakdown"]
        traj = stats["avg_trajectory"]

        parts = [f"Analyzed {stats['total_n']} workout days. "]

        for intensity in ["high", "moderate", "light"]:
            if intensity not in breakdown:
                continue
            s = breakdown[intensity]
            avg_days = s.get("avg_recovery_days")
            pct_d1 = s.get("pct_recovered_d1")
            drop = s.get("avg_readiness_drop_d1")

            line = f"{intensity.title()} intensity ({s['n']} workouts, avg {s['avg_total_minutes']:.0f} min): "
            if avg_days is not None:
                line += f"avg recovery {avg_days} days"
            if pct_d1 is not None:
                line += f", {pct_d1:.0f}% recovered by day 1"
            if drop is not None:
                direction = "drop" if drop < 0 else "gain"
                line += f", avg next-day readiness {direction} of {abs(drop):.1f} pts"
            parts.append(line + ". ")

        parts.append(
            f"Overall trajectory: D+1 {traj['d1']:+.1f}, D+2 {traj['d2']:+.1f}, D+3 {traj['d3']:+.1f} readiness points."
        )

        return "".join(parts)
