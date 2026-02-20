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
        df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)

        if date_range:
            df = df[
                (df["date"].dt.date >= date_range.start)
                & (df["date"].dt.date <= date_range.end)
            ]

        # Compute CTL (42-day EMA) and ATL (7-day EMA)
        df["ctl"] = compute_ema(df["tss"], span=42)
        df["atl"] = compute_ema(df["tss"], span=7)
        df["tsb"] = df["ctl"] - df["atl"]

        # Fetch RHR and HRV for recovery quality overlay
        vitals_df = self.athena.execute_query("""
            SELECT date, resting_heart_rate_bpm, hrv_ms
            FROM bio_gold.daily_readiness_performance
            WHERE resting_heart_rate_bpm IS NOT NULL
            ORDER BY date
        """)
        if not vitals_df.empty:
            vitals_df["date"] = pd.to_datetime(vitals_df["date"])
            vitals_df["resting_heart_rate_bpm"] = pd.to_numeric(
                vitals_df["resting_heart_rate_bpm"], errors="coerce"
            )
            vitals_df["hrv_ms"] = pd.to_numeric(vitals_df["hrv_ms"], errors="coerce")
            vitals_df = vitals_df.drop_duplicates(subset=["date"])
            df = df.merge(vitals_df, on="date", how="left")
        else:
            df["resting_heart_rate_bpm"] = np.nan
            df["hrv_ms"] = np.nan

        # Compute 14-day rolling baselines and recovery impairment flags
        df["baseline_rhr"] = df["resting_heart_rate_bpm"].rolling(14, min_periods=7).mean()
        df["baseline_hrv"] = df["hrv_ms"].rolling(14, min_periods=7).mean()
        df["rhr_elevated"] = df["resting_heart_rate_bpm"] > (df["baseline_rhr"] * 1.10)
        df["hrv_suppressed"] = df["hrv_ms"] < (df["baseline_hrv"] * 0.85)
        df["recovery_impaired"] = (
            (df["tsb"] < -15)
            & (df["rhr_elevated"] | df["hrv_suppressed"])
        )

        latest = df.iloc[-1]
        form = classify_form(latest["tsb"])

        last_90 = df.tail(90)
        recovery_impaired_days = int(last_90["recovery_impaired"].sum())
        recovery_impaired_recent = bool(df.tail(7)["recovery_impaired"].any())

        statistics = {
            "n": len(df),
            "latest_tss": round(float(latest["tss"]), 1),
            "latest_ctl": round(float(latest["ctl"]), 1),
            "latest_atl": round(float(latest["atl"]), 1),
            "latest_tsb": round(float(latest["tsb"]), 1),
            "form": form,
            "avg_tss_7d": round(float(df["tss"].tail(7).mean()), 1),
            "avg_tss_30d": round(float(df["tss"].tail(30).mean()), 1),
            "recovery_impaired_days": recovery_impaired_days,
            "recovery_impaired_recent": recovery_impaired_recent,
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

        # Show last 90 days for readable detail
        cutoff = df["date"].max() - pd.Timedelta(days=90)
        plot_df = df[df["date"] >= cutoff].copy()

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            subplot_titles=(
                "Chronic (CTL) vs Acute (ATL) Training Load — Last 90 Days",
                "Training Stress Balance (TSB) — Form Curve",
            ),
        )

        # Row 1: CTL and ATL lines
        fig.add_trace(
            go.Scatter(
                x=plot_df["date"], y=plot_df["ctl"],
                mode="lines", name="CTL (42-day)",
                line=dict(color=theme.PRIMARY, width=2),
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=plot_df["date"], y=plot_df["atl"],
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
            for v in plot_df["tsb"]
        ]
        fig.add_trace(
            go.Bar(
                x=plot_df["date"], y=plot_df["tsb"],
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

        # Overlay recovery-impaired markers on TSB chart
        if "recovery_impaired" in plot_df.columns:
            impaired = plot_df[plot_df["recovery_impaired"] == True]
            if not impaired.empty:
                fig.add_trace(
                    go.Scatter(
                        x=impaired["date"], y=impaired["tsb"],
                        mode="markers", name="Recovery Impaired",
                        marker=dict(
                            color=theme.DANGER, size=12, symbol="diamond",
                            line=dict(width=2, color="white"),
                        ),
                    ),
                    row=2, col=1,
                )

        theme.style_figure(fig, n=s["n"], height=600)
        # Fit y-axes to visible data with padding
        ctl_atl_max = max(plot_df["ctl"].max(), plot_df["atl"].max(), 10)
        fig.update_yaxes(title_text="Training Load", range=[0, ctl_atl_max * 1.15], row=1, col=1)
        tsb_abs_max = max(abs(plot_df["tsb"].min()), abs(plot_df["tsb"].max()), 20)
        fig.update_yaxes(title_text="TSB", range=[-tsb_abs_max * 1.15, tsb_abs_max * 1.15], row=2, col=1)

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

        base = (
            f"**Current Form: {s['form'].title()}** — "
            f"CTL={s['latest_ctl']:.0f}, ATL={s['latest_atl']:.0f}, TSB={s['latest_tsb']:+.0f}. "
            f"{form_text} "
            f"7-day avg TSS: {s['avg_tss_7d']:.0f}, 30-day avg: {s['avg_tss_30d']:.0f}."
        )

        impaired_days = s.get("recovery_impaired_days", 0)
        impaired_recent = s.get("recovery_impaired_recent", False)
        if impaired_days > 0:
            recovery_note = (
                f" In the last 90 days, **{impaired_days} day(s)** showed recovery impairment "
                "(fatigued TSB confirmed by elevated RHR or suppressed HRV)."
            )
            if impaired_recent:
                recovery_note += (
                    " **Warning:** recovery impairment detected in the last 7 days — "
                    "consider prioritizing rest."
                )
        else:
            recovery_note = (
                " No recovery impairment detected — training fatigue appears manageable "
                "with no autonomic stress signals."
            )

        return base + recovery_note
