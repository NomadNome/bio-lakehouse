"""
Bio Insights Engine - Signature Insight: Training Load Periodization

Computes CTL (Chronic Training Load, 42-day EMA), ATL (Acute Training Load,
7-day EMA), and TSB (Training Stress Balance = CTL - ATL) from the
training_load_daily Athena view.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


def compute_ema(series: pd.Series, span: int) -> pd.Series:
    """Compute exponential moving average with given span."""
    return series.ewm(span=span, adjust=False).mean()


def classify_form(tsb: float) -> str:
    """Classify training form based on TSB value."""
    if tsb > 15:
        return "fresh"
    elif tsb >= 0:
        return "neutral"
    elif tsb >= -15:
        return "building"
    else:
        return "fatigued"


class TrainingLoadAnalyzer(InsightAnalyzer):
    """Analyzes training load periodization (CTL / ATL / TSB)."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        sql = """
        SELECT date, tss, had_workout
        FROM bio_gold.training_load_daily
        ORDER BY date
        """
        df = self.athena.execute_query(sql)

        if df.empty or len(df) < 7:
            return InsightResult(
                insight_type="training_load",
                title="Training Load Periodization",
                narrative="Insufficient data for training load analysis (need at least 7 days).",
                statistics={"n": len(df)},
                caveats=["Not enough historical data."],
                data=df,
            )

        df["date"] = pd.to_datetime(df["date"])
        df["tss"] = pd.to_numeric(df["tss"], errors="coerce").fillna(0)
        df = df.sort_values("date").reset_index(drop=True)

        if date_range:
            df = df[
                (df["date"].dt.date >= date_range.start)
                & (df["date"].dt.date <= date_range.end)
            ]

        # Compute CTL (42-day EMA) and ATL (7-day EMA)
        df["ctl"] = compute_ema(df["tss"], span=42)
        df["atl"] = compute_ema(df["tss"], span=7)
        df["tsb"] = df["ctl"] - df["atl"]

        latest = df.iloc[-1]
        form = classify_form(latest["tsb"])

        statistics = {
            "n": len(df),
            "latest_tss": round(float(latest["tss"]), 1),
            "latest_ctl": round(float(latest["ctl"]), 1),
            "latest_atl": round(float(latest["atl"]), 1),
            "latest_tsb": round(float(latest["tsb"]), 1),
            "form": form,
            "avg_tss_7d": round(float(df["tss"].tail(7).mean()), 1),
            "avg_tss_30d": round(float(df["tss"].tail(30).mean()), 1),
        }

        result = InsightResult(
            insight_type="training_load",
            title="Training Load Periodization",
            narrative="",
            statistics=statistics,
            data=df,
        )
        result.narrative = self.narrate(result)
        result.chart = self.visualize(result)

        caveats = []
        if len(df) < 42:
            caveats.append(
                f"CTL uses a 42-day EMA but only {len(df)} days are available. "
                "Values will stabilize as more data accumulates."
            )
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        s = result.statistics

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            subplot_titles=(
                "Chronic (CTL) vs Acute (ATL) Training Load",
                "Training Stress Balance (TSB) — Form Curve",
            ),
        )

        # Row 1: CTL and ATL lines
        fig.add_trace(
            go.Scatter(
                x=df["date"], y=df["ctl"],
                mode="lines", name="CTL (42-day)",
                line=dict(color=theme.PRIMARY, width=2),
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"], y=df["atl"],
                mode="lines", name="ATL (7-day)",
                line=dict(color=theme.SECONDARY, width=2),
            ),
            row=1, col=1,
        )

        # Row 2: TSB area fill
        tsb_colors = [
            theme.SUCCESS if v > 15
            else theme.ACCENT if v >= 0
            else theme.WARNING if v >= -15
            else theme.DANGER
            for v in df["tsb"]
        ]
        fig.add_trace(
            go.Bar(
                x=df["date"], y=df["tsb"],
                name="TSB",
                marker_color=tsb_colors,
                opacity=0.7,
            ),
            row=2, col=1,
        )

        # Add zone lines on TSB chart
        for val, label in [(15, "Fresh"), (0, "Neutral"), (-15, "Building")]:
            fig.add_hline(
                y=val, row=2, col=1,
                line_dash="dot",
                line_color=theme.TEXT_MUTED,
                annotation_text=label,
                annotation_position="right",
                annotation_font_color=theme.TEXT_MUTED,
                annotation_font_size=9,
            )

        theme.style_figure(fig, n=s["n"], height=600)
        fig.update_yaxes(title_text="Training Load", row=1, col=1)
        fig.update_yaxes(title_text="TSB", row=2, col=1)

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        form_descriptions = {
            "fresh": "You're in a **fresh** state (TSB > 15) — well-recovered and ready for peak performance or competition.",
            "neutral": "You're in a **neutral** state (TSB 0–15) — balanced training load with good recovery.",
            "building": "You're in a **building** state (TSB -15 to 0) — productive training stress that builds fitness.",
            "fatigued": "You're in a **fatigued** state (TSB < -15) — accumulated fatigue. Consider a recovery block.",
        }
        form_text = form_descriptions[s["form"]]

        return (
            f"**Current Form: {s['form'].title()}** — "
            f"CTL={s['latest_ctl']:.0f}, ATL={s['latest_atl']:.0f}, TSB={s['latest_tsb']:+.0f}. "
            f"{form_text} "
            f"7-day avg TSS: {s['avg_tss_7d']:.0f}, 30-day avg: {s['avg_tss_30d']:.0f}."
        )
