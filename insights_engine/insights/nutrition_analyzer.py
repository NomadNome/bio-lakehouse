"""
Bio Insights Engine - Signature Insight: Nutrition Tracking

Analyzes daily calorie intake, macro ratios, and their relationship
to readiness/recovery scores from MyFitnessPal data.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


class NutritionAnalyzer(InsightAnalyzer):
    """Analyzes nutrition intake patterns and their correlation with recovery."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        # Check if nutrition columns exist in Gold table yet
        # (they won't until the MFP Glue normalizer + dbt refresh have run)
        try:
            probe = self.athena.execute_query(
                "SELECT daily_calories FROM bio_gold.daily_readiness_performance LIMIT 1"
            )
        except Exception:
            return InsightResult(
                insight_type="nutrition",
                title="Nutrition Tracking",
                narrative=(
                    "Nutrition columns not yet available in Gold table. "
                    "Run the MFP Glue normalizer and `dbt run` to populate them."
                ),
                statistics={"n": 0},
                caveats=["MFP pipeline has not run yet."],
                data=pd.DataFrame(),
            )

        sql = """
        SELECT
            date, daily_calories, protein_g, carbs_g, fat_g, fiber_g,
            protein_pct, carb_pct, fat_pct, meal_count,
            readiness_score, weight_lbs
        FROM bio_gold.daily_readiness_performance
        WHERE daily_calories IS NOT NULL
        ORDER BY date
        """
        df = self.athena.execute_query(sql)

        if df.empty or len(df) < 3:
            return InsightResult(
                insight_type="nutrition",
                title="Nutrition Tracking",
                narrative="Insufficient nutrition data (need at least 3 days of MFP logging).",
                statistics={"n": len(df)},
                caveats=["Not enough MFP data logged yet."],
                data=df,
            )

        df["date"] = pd.to_datetime(df["date"])
        for col in ["daily_calories", "protein_g", "carbs_g", "fat_g", "fiber_g",
                     "protein_pct", "carb_pct", "fat_pct", "meal_count",
                     "readiness_score", "weight_lbs"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

        if date_range:
            df = df[
                (df["date"].dt.date >= date_range.start)
                & (df["date"].dt.date <= date_range.end)
            ]

        # Compute protein per lb if weight available
        if "weight_lbs" in df.columns:
            df["protein_per_lb"] = np.where(
                df["weight_lbs"] > 0,
                df["protein_g"] / df["weight_lbs"],
                np.nan,
            )
        else:
            df["protein_per_lb"] = np.nan

        # Key stats
        avg_cal = df["daily_calories"].mean()
        avg_protein = df["protein_g"].mean()
        avg_carbs = df["carbs_g"].mean()
        avg_fat = df["fat_g"].mean()
        avg_protein_pct = df["protein_pct"].mean() if "protein_pct" in df.columns else np.nan
        avg_protein_per_lb = df["protein_per_lb"].mean()

        # 7-day averages (if enough data)
        last_7 = df.tail(7)
        avg_cal_7d = last_7["daily_calories"].mean()
        avg_protein_7d = last_7["protein_g"].mean()

        # Correlation with readiness (if both exist)
        both = df.dropna(subset=["daily_calories", "readiness_score"])
        cal_readiness_r = np.nan
        protein_readiness_r = np.nan
        if len(both) >= 5:
            cal_readiness_r = both["daily_calories"].corr(both["readiness_score"])
            protein_both = both.dropna(subset=["protein_g"])
            if len(protein_both) >= 5:
                protein_readiness_r = protein_both["protein_g"].corr(protein_both["readiness_score"])

        statistics = {
            "n": len(df),
            "avg_calories": round(float(avg_cal), 0),
            "avg_protein_g": round(float(avg_protein), 1),
            "avg_carbs_g": round(float(avg_carbs), 1),
            "avg_fat_g": round(float(avg_fat), 1),
            "avg_protein_pct": round(float(avg_protein_pct), 1) if pd.notna(avg_protein_pct) else None,
            "avg_protein_per_lb": round(float(avg_protein_per_lb), 2) if pd.notna(avg_protein_per_lb) else None,
            "avg_calories_7d": round(float(avg_cal_7d), 0),
            "avg_protein_7d": round(float(avg_protein_7d), 1),
            "cal_readiness_r": round(float(cal_readiness_r), 3) if pd.notna(cal_readiness_r) else None,
            "protein_readiness_r": round(float(protein_readiness_r), 3) if pd.notna(protein_readiness_r) else None,
        }

        result = InsightResult(
            insight_type="nutrition",
            title="Nutrition Tracking",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 14:
            caveats.append(
                f"Only {len(df)} days of nutrition data — correlations will "
                "become more reliable with 2+ weeks of logging."
            )
        avg_meals = df["meal_count"].mean() if "meal_count" in df.columns else 0
        if pd.notna(avg_meals) and avg_meals < 3:
            caveats.append(
                f"Averaging {avg_meals:.1f} meals logged/day — incomplete logging "
                "will undercount actual intake."
            )
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.10,
            subplot_titles=(
                "Daily Calories & Macro Breakdown",
                "Protein Intake vs Readiness Score",
            ),
        )

        # Row 1: Stacked bar chart of macros (protein, carbs, fat in calories)
        fig.add_trace(
            go.Bar(
                x=df["date"], y=df["protein_g"] * 4,
                name="Protein (cal)", marker_color=theme.PRIMARY,
                hovertemplate="%{y:.0f} cal (%{customdata:.0f}g)<extra>Protein</extra>",
                customdata=df["protein_g"],
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Bar(
                x=df["date"], y=df["carbs_g"] * 4,
                name="Carbs (cal)", marker_color=theme.ACCENT,
                hovertemplate="%{y:.0f} cal (%{customdata:.0f}g)<extra>Carbs</extra>",
                customdata=df["carbs_g"],
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Bar(
                x=df["date"], y=df["fat_g"] * 9,
                name="Fat (cal)", marker_color=theme.WARNING,
                hovertemplate="%{y:.0f} cal (%{customdata:.0f}g)<extra>Fat</extra>",
                customdata=df["fat_g"],
            ),
            row=1, col=1,
        )
        fig.update_layout(barmode="stack")

        # Row 2: Dual-axis protein vs readiness
        both = df.dropna(subset=["protein_g", "readiness_score"])
        if not both.empty:
            fig.add_trace(
                go.Bar(
                    x=both["date"], y=both["protein_g"],
                    name="Protein (g)", marker_color=theme.PRIMARY,
                    opacity=0.5,
                ),
                row=2, col=1,
            )
            fig.add_trace(
                go.Scatter(
                    x=both["date"], y=both["readiness_score"],
                    mode="lines+markers", name="Readiness",
                    line=dict(color=theme.SECONDARY, width=2),
                    marker=dict(size=5),
                    yaxis="y4",
                ),
                row=2, col=1,
            )

        fig.update_yaxes(title_text="Calories", row=1, col=1)
        fig.update_yaxes(title_text="Protein (g)", row=2, col=1)

        # Secondary y-axis for readiness on row 2
        fig.update_layout(
            yaxis4=dict(
                title="Readiness",
                overlaying="y3",
                side="right",
                range=[0, 100],
                showgrid=False,
            ),
        )

        theme.style_figure(fig, n=s["n"], height=550)
        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        n = s["n"]

        parts = [
            f"**{n} days** of nutrition data logged. "
            f"Average daily intake: **{s['avg_calories']:.0f} cal** "
            f"({s['avg_protein_g']:.0f}g protein, "
            f"{s['avg_carbs_g']:.0f}g carbs, "
            f"{s['avg_fat_g']:.0f}g fat)."
        ]

        if s.get("avg_protein_per_lb") is not None:
            ppl = s["avg_protein_per_lb"]
            if ppl < 0.7:
                parts.append(
                    f" Protein: **{ppl:.2f} g/lb** — below the 0.7 g/lb minimum "
                    "recommended for active recovery. Consider increasing protein intake."
                )
            elif ppl >= 1.0:
                parts.append(
                    f" Protein: **{ppl:.2f} g/lb** — excellent for muscle recovery and adaptation."
                )
            else:
                parts.append(
                    f" Protein: **{ppl:.2f} g/lb** — adequate range (0.7–1.0 g/lb target)."
                )

        if s.get("protein_readiness_r") is not None:
            r = s["protein_readiness_r"]
            if abs(r) >= 0.3:
                direction = "positively" if r > 0 else "negatively"
                parts.append(
                    f" Protein intake {direction} correlates with readiness (r={r:.2f})."
                )

        return "".join(parts)
