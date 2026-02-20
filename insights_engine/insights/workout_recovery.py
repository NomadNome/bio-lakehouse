"""
Bio Insights Engine - Signature Insight 3b: Workout Type → Recovery Analysis

Compare next-day readiness segmented by prior-day workout type,
with box plot and Mann-Whitney U test.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from scipy import stats

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class WorkoutRecoveryAnalyzer(InsightAnalyzer):
    """Compares next-day readiness by prior-day workout type."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        # Get daily data with workout info and next-day readiness
        sql = """
        SELECT
            a.date AS workout_date,
            a.readiness_score,
            a.disciplines,
            a.hk_workout_types,
            a.had_workout,
            a.total_output_kj,
            b.readiness_score AS next_day_readiness
        FROM bio_gold.dashboard_30day a
        JOIN bio_gold.dashboard_30day b
            ON COALESCE(TRY(CAST(b.date AS date)), TRY(date_parse(b.date, '%Y-%m-%d %H:%i:%s')))
             = date_add('day', 1, COALESCE(TRY(CAST(a.date AS date)), TRY(date_parse(a.date, '%Y-%m-%d %H:%i:%s'))))
        WHERE b.readiness_score IS NOT NULL
        ORDER BY a.date
        """
        df = self.athena.execute_query(sql)

        if date_range:
            df = df[
                (pd.to_datetime(df["workout_date"]).dt.date >= date_range.start)
                & (pd.to_datetime(df["workout_date"]).dt.date <= date_range.end)
            ]

        # Check minimum data threshold — need at least 20 unique workout days
        unique_days = df["workout_date"].nunique()
        if unique_days < 20:
            return InsightResult(
                insight_type="workout_recovery",
                title="Workout Type → Next-Day Recovery",
                narrative=(
                    f"Not enough data yet ({unique_days} unique workout days, need 20+). "
                    "This insight will appear once more data accumulates."
                ),
                statistics={"total_n": len(df), "unique_days": unique_days},
                caveats=["Insufficient data — minimum 20 unique workout days required."],
                data=df,
            )

        # Categorize workout type from Peloton disciplines + HealthKit types
        def categorize(row):
            if not row.get("had_workout") or row.get("had_workout") == "false":
                return "Rest Day"
            disciplines = str(row.get("disciplines", "")).lower()
            hk_types = str(row.get("hk_workout_types", "")).lower()
            combined = f"{disciplines},{hk_types}"

            if "cycling" in combined:
                return "Cycling"
            if "strength" in combined or "strength_training" in hk_types:
                return "Strength"
            if "walking" in combined or "hiking" in combined:
                return "Walking"
            if "running" in combined or "bootcamp" in combined or "hiit" in hk_types or "high_intensity" in hk_types:
                return "Cardio"
            if "yoga" in combined or "stretching" in combined or "meditation" in combined or "flexibility" in hk_types or "pilates" in hk_types:
                return "Recovery"
            return "Other Workout"

        df["workout_category"] = df.apply(categorize, axis=1)

        groups = {}
        for cat in df["workout_category"].unique():
            vals = df[df["workout_category"] == cat]["next_day_readiness"].dropna().astype(float)
            if len(vals) >= 5:
                groups[cat] = {
                    "values": vals.values,
                    "mean": round(float(vals.mean()), 1),
                    "median": round(float(vals.median()), 1),
                    "std": round(float(vals.std()), 1),
                    "n": len(vals),
                }

        # Mann-Whitney U between the two largest groups
        comparisons = {}
        group_names = sorted(groups.keys(), key=lambda k: groups[k]["n"], reverse=True)
        if len(group_names) >= 2:
            g1, g2 = group_names[0], group_names[1]
            u_stat, u_p = stats.mannwhitneyu(
                groups[g1]["values"], groups[g2]["values"], alternative="two-sided"
            )
            comparisons[f"{g1}_vs_{g2}"] = {
                "U": round(float(u_stat), 1),
                "p_value": round(float(u_p), 4),
                "significant": u_p < 0.05,
            }

        statistics = {
            "groups": {k: {kk: vv for kk, vv in v.items() if kk != "values"} for k, v in groups.items()},
            "comparisons": comparisons,
            "total_n": len(df),
        }

        result = InsightResult(
            insight_type="workout_recovery",
            title="Workout Type → Next-Day Recovery",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        for name, g in groups.items():
            if g["n"] < 10:
                caveats.append(f"{name}: only {g['n']} observations.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        fig = go.Figure()

        colors = [theme.PRIMARY, theme.SECONDARY, theme.ACCENT, theme.WARNING, "#8B5CF6", "#06B6D4", "#F97316"]
        categories = sorted(df["workout_category"].unique())

        for i, cat in enumerate(categories):
            vals = df[df["workout_category"] == cat]["next_day_readiness"].dropna().astype(float)
            fig.add_trace(go.Box(
                y=vals,
                name=cat,
                marker_color=colors[i % len(colors)],
                boxmean=True,
            ))

        theme.style_figure(
            fig,
            n=result.statistics["total_n"],
            title="Next-Day Readiness by Workout Type",
            yaxis_title="Next-Day Readiness Score",
            showlegend=False,
        )
        return fig

    def narrate(self, result: InsightResult) -> str:
        groups = result.statistics["groups"]
        comps = result.statistics["comparisons"]

        parts = []
        for name, g in sorted(groups.items(), key=lambda x: x[1]["mean"], reverse=True):
            parts.append(f"{name}: mean {g['mean']} (n={g['n']})")

        narrative = "Next-day readiness by workout type: " + "; ".join(parts) + ". "

        for comp_name, comp in comps.items():
            if comp["significant"]:
                narrative += f"The difference is statistically significant (U={comp['U']}, p={comp['p_value']:.3f}). "
            else:
                narrative += f"The difference is not statistically significant (U={comp['U']}, p={comp['p_value']:.3f}). "

        return narrative
