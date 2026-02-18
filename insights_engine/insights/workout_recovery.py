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

        # Categorize: rest day vs cycling vs other workout
        def categorize(row):
            if not row.get("had_workout") or row.get("had_workout") == "false":
                return "Rest Day"
            disciplines = str(row.get("disciplines", ""))
            if "Cycling" in disciplines:
                return "Cycling"
            if "Strength" in disciplines:
                return "Strength"
            return "Other Workout"

        df["workout_category"] = df.apply(categorize, axis=1)

        groups = {}
        for cat in df["workout_category"].unique():
            vals = df[df["workout_category"] == cat]["next_day_readiness"].dropna().astype(float)
            if len(vals) >= 2:
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

        colors = [theme.PRIMARY, theme.SECONDARY, theme.ACCENT, theme.WARNING]
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
