"""
Bio Insights Engine - Signature Insight 3e: Workout Timing Correlation

Analyzes whether workout timing (morning vs afternoon/evening) correlates
with readiness or recovery metrics. Uses Peloton workout timestamps.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from scipy import stats

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class TimingCorrelationAnalyzer(InsightAnalyzer):
    """Analyzes workout timing impact on next-day readiness."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        # Get Peloton workout data with timestamps from silver layer
        # Fall back to gold layer date-level data if timestamps unavailable
        try:
            sql = """
            SELECT
                d.date,
                d.readiness_score,
                d.total_output_kj,
                d.disciplines,
                d.had_workout,
                b.readiness_score AS next_day_readiness
            FROM bio_gold.dashboard_30day d
            JOIN bio_gold.dashboard_30day b
                ON COALESCE(TRY(CAST(b.date AS date)), TRY(date_parse(b.date, '%Y-%m-%d %H:%i:%s')))
                 = date_add('day', 1, COALESCE(TRY(CAST(d.date AS date)), TRY(date_parse(d.date, '%Y-%m-%d %H:%i:%s'))))
            WHERE d.had_workout = true
              AND d.total_output_kj IS NOT NULL
              AND b.readiness_score IS NOT NULL
            ORDER BY d.date
            """
            df = self.athena.execute_query(sql)
        except Exception:
            return InsightResult(
                insight_type="timing_correlation",
                title="Workout Timing Analysis",
                narrative="Insufficient timing data — skipping this analysis.",
                caveats=["No workout timestamp data available in current views."],
            )

        if len(df) < 5:
            return InsightResult(
                insight_type="timing_correlation",
                title="Workout Timing Analysis",
                narrative="Insufficient timing data — skipping this analysis.",
                statistics={"n": len(df)},
                caveats=["Fewer than 5 workout days with next-day readiness data."],
                data=df,
            )

        if date_range:
            df["date"] = pd.to_datetime(df["date"])
            df = df[
                (df["date"].dt.date >= date_range.start)
                & (df["date"].dt.date <= date_range.end)
            ]

        # Since gold layer doesn't have exact timestamps, analyze by
        # workout intensity (output) buckets as a proxy
        output = df["total_output_kj"].astype(float)
        median_output = float(output.median())

        df["intensity"] = np.where(output >= median_output, "High Intensity", "Low Intensity")

        groups = {}
        for intensity in df["intensity"].unique():
            vals = df[df["intensity"] == intensity]["next_day_readiness"].dropna().astype(float)
            if len(vals) >= 2:
                groups[intensity] = {
                    "values": vals.values,
                    "mean": round(float(vals.mean()), 1),
                    "median": round(float(vals.median()), 1),
                    "std": round(float(vals.std()), 1),
                    "n": len(vals),
                    "avg_output": round(float(df[df["intensity"] == intensity]["total_output_kj"].mean()), 1),
                }

        # Statistical test
        comparison = {}
        group_names = list(groups.keys())
        if len(group_names) >= 2:
            g1, g2 = group_names[0], group_names[1]
            u_stat, u_p = stats.mannwhitneyu(
                groups[g1]["values"], groups[g2]["values"], alternative="two-sided"
            )
            comparison = {
                "groups": f"{g1} vs {g2}",
                "U": round(float(u_stat), 1),
                "p_value": round(float(u_p), 4),
                "significant": u_p < 0.05,
            }

        statistics = {
            "groups": {k: {kk: vv for kk, vv in v.items() if kk != "values"} for k, v in groups.items()},
            "comparison": comparison,
            "median_output_threshold": round(median_output, 1),
            "total_n": len(df),
            "note": "Using workout intensity as proxy (exact timing data not in gold views).",
        }

        result = InsightResult(
            insight_type="timing_correlation",
            title="Workout Intensity → Next-Day Recovery",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = [
            "Exact workout timestamps not available in gold views; using intensity as proxy.",
        ]
        for name, g in groups.items():
            if g["n"] < 10:
                caveats.append(f"{name}: only {g['n']} observations.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        groups = result.statistics["groups"]
        fig = go.Figure()

        colors = [theme.PRIMARY, theme.SECONDARY]
        for i, (name, g) in enumerate(groups.items()):
            fig.add_trace(go.Bar(
                x=[name],
                y=[g["mean"]],
                name=f"{name} (n={g['n']})",
                marker_color=colors[i % len(colors)],
                text=[f"{g['mean']:.1f}"],
                textposition="auto",
                error_y=dict(type="data", array=[g["std"]], visible=True),
            ))

        theme.style_figure(
            fig,
            n=result.statistics["total_n"],
            title="Next-Day Readiness by Workout Intensity",
            yaxis_title="Next-Day Readiness Score",
            barmode="group",
        )

        comp = result.statistics.get("comparison", {})
        if comp:
            sig_text = "significant" if comp.get("significant") else "not significant"
            fig.add_annotation(
                text=f"p = {comp['p_value']:.3f} ({sig_text})",
                xref="paper", yref="paper",
                x=0.5, y=1.05,
                showarrow=False,
                font=dict(color=theme.TEXT_MUTED, size=11),
            )

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        groups = s["groups"]

        parts = []
        for name, g in sorted(groups.items(), key=lambda x: x[1]["mean"], reverse=True):
            parts.append(f"{name}: mean readiness {g['mean']} (avg output {g['avg_output']} kJ, n={g['n']})")

        narrative = "Next-day readiness by workout intensity: " + "; ".join(parts) + ". "

        comp = s.get("comparison", {})
        if comp:
            if comp["significant"]:
                narrative += f"The difference is statistically significant (p={comp['p_value']:.3f}). "
            else:
                narrative += f"The difference is not statistically significant (p={comp['p_value']:.3f}). "

        narrative += s.get("note", "")
        return narrative
