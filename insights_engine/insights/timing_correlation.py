"""
Bio Insights Engine - Signature Insight 3e: Workout Intensity → Next-Day Recovery

Analyzes how workout intensity (total output kJ) affects next-day readiness
using the full daily_readiness_performance history. Shows individual workout
scatter points with a LOWESS trend and per-bucket box plots.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats

from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.viz import theme


_BUCKET_LABELS = ["Rest", "Light", "Moderate", "Hard", "Max"]
_BUCKET_COLORS = [theme.ACCENT, theme.PRIMARY, theme.SECONDARY, "#F59E0B", theme.DANGER]


class TimingCorrelationAnalyzer(InsightAnalyzer):
    """Analyzes workout intensity impact on next-day readiness."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        try:
            sql = """
            SELECT
                d.date,
                d.readiness_score,
                d.total_output_kj,
                d.disciplines,
                d.had_workout,
                b.readiness_score AS next_day_readiness
            FROM bio_gold.daily_readiness_performance d
            JOIN bio_gold.daily_readiness_performance b
                ON TRY(CAST(b.date AS date))
                 = date_add('day', 1, TRY(CAST(d.date AS date)))
            WHERE d.had_workout = true
              AND d.total_output_kj IS NOT NULL
              AND b.readiness_score IS NOT NULL
            ORDER BY d.date
            """
            df = self.athena.execute_query(sql)
        except Exception:
            return InsightResult(
                insight_type="timing_correlation",
                title="Workout Intensity → Next-Day Recovery",
                narrative="Insufficient data — skipping this analysis.",
                caveats=["Query failed against daily_readiness_performance."],
            )

        if len(df) < 5:
            return InsightResult(
                insight_type="timing_correlation",
                title="Workout Intensity → Next-Day Recovery",
                narrative="Insufficient data — need at least 5 workout days with next-day readiness.",
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

        df["total_output_kj"] = df["total_output_kj"].astype(float)
        df["next_day_readiness"] = df["next_day_readiness"].astype(float)
        df["readiness_score"] = df["readiness_score"].astype(float)
        df["date"] = pd.to_datetime(df["date"])

        # Intensity buckets by quintile (data-driven thresholds)
        df["bucket"] = pd.qcut(
            df["total_output_kj"], q=min(5, len(df) // 3), labels=False, duplicates="drop",
        )
        n_buckets = int(df["bucket"].max()) + 1
        labels = _BUCKET_LABELS[:n_buckets]
        df["intensity"] = df["bucket"].map(dict(enumerate(labels)))

        # Per-bucket stats
        groups = {}
        for bucket_idx, label in enumerate(labels):
            sub = df[df["bucket"] == bucket_idx]
            vals = sub["next_day_readiness"].dropna()
            if len(vals) >= 2:
                groups[label] = {
                    "mean": round(float(vals.mean()), 1),
                    "median": round(float(vals.median()), 1),
                    "std": round(float(vals.std()), 1),
                    "n": len(vals),
                    "avg_output": round(float(sub["total_output_kj"].mean()), 1),
                    "output_range": f"{sub['total_output_kj'].min():.0f}–{sub['total_output_kj'].max():.0f}",
                }

        # Spearman correlation: intensity vs next-day readiness
        rho, p_val = stats.spearmanr(df["total_output_kj"], df["next_day_readiness"])

        # Recent trend: last 14 days vs prior
        recent_mask = df["date"] >= df["date"].max() - pd.Timedelta(days=14)
        recent_mean = float(df.loc[recent_mask, "next_day_readiness"].mean()) if recent_mask.any() else None
        prior_mean = float(df.loc[~recent_mask, "next_day_readiness"].mean()) if (~recent_mask).any() else None

        statistics = {
            "groups": groups,
            "correlation": {"rho": round(float(rho), 3), "p_value": round(float(p_val), 4)},
            "total_n": len(df),
            "recent_14d_mean": round(recent_mean, 1) if recent_mean else None,
            "prior_mean": round(prior_mean, 1) if prior_mean else None,
            "metrics": [
                {"label": "Workouts Analyzed", "value": str(len(df))},
                {"label": "Correlation (ρ)", "value": f"{rho:.2f}",
                 "delta": "sig" if p_val < 0.05 else "n.s."},
                {"label": "Recent 14d Recovery",
                 "value": f"{recent_mean:.0f}" if recent_mean else "—",
                 "delta": f"{recent_mean - prior_mean:+.1f}" if recent_mean and prior_mean else None},
            ],
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

        caveats = []
        if p_val >= 0.05:
            caveats.append(f"Correlation not significant (p={p_val:.3f}).")
        for name, g in groups.items():
            if g["n"] < 10:
                caveats.append(f"{name}: only {g['n']} observations.")
        result.caveats = caveats

        return result

    def visualize(self, result: InsightResult) -> go.Figure:
        df = result.data
        groups = result.statistics["groups"]
        corr = result.statistics["correlation"]

        fig = make_subplots(
            rows=1, cols=2,
            column_widths=[0.6, 0.4],
            subplot_titles=["Each Workout → Next-Day Readiness", "By Intensity Bucket"],
            horizontal_spacing=0.08,
        )

        # ── Left: scatter with LOWESS trend ──
        recent_mask = df["date"] >= df["date"].max() - pd.Timedelta(days=14)

        # Older points (faded)
        older = df[~recent_mask]
        if len(older):
            fig.add_trace(go.Scatter(
                x=older["total_output_kj"], y=older["next_day_readiness"],
                mode="markers",
                marker=dict(color=theme.PRIMARY, size=5, opacity=0.3),
                name="Older",
                hovertemplate="%{customdata}<br>Output: %{x:.0f} kJ<br>Next-day: %{y:.0f}<extra></extra>",
                customdata=older["date"].dt.strftime("%b %d"),
            ), row=1, col=1)

        # Recent points (bright)
        recent = df[recent_mask]
        if len(recent):
            fig.add_trace(go.Scatter(
                x=recent["total_output_kj"], y=recent["next_day_readiness"],
                mode="markers",
                marker=dict(color=theme.ACCENT, size=9, opacity=0.9,
                            line=dict(width=1, color="white")),
                name="Last 14 days",
                hovertemplate="%{customdata}<br>Output: %{x:.0f} kJ<br>Next-day: %{y:.0f}<extra></extra>",
                customdata=recent["date"].dt.strftime("%b %d"),
            ), row=1, col=1)

        # LOWESS trend line
        try:
            from statsmodels.nonparametric.smoothers_lowess import lowess
            sorted_df = df.sort_values("total_output_kj")
            smoothed = lowess(
                sorted_df["next_day_readiness"], sorted_df["total_output_kj"],
                frac=0.4, it=3,
            )
            fig.add_trace(go.Scatter(
                x=smoothed[:, 0], y=smoothed[:, 1],
                mode="lines",
                line=dict(color=theme.SECONDARY, width=3),
                name="Trend",
            ), row=1, col=1)
        except ImportError:
            # Fall back to linear regression line
            m, b, _, _, _ = stats.linregress(df["total_output_kj"], df["next_day_readiness"])
            x_range = np.linspace(df["total_output_kj"].min(), df["total_output_kj"].max(), 50)
            fig.add_trace(go.Scatter(
                x=x_range, y=m * x_range + b,
                mode="lines",
                line=dict(color=theme.SECONDARY, width=3, dash="dash"),
                name="Trend",
            ), row=1, col=1)

        # ── Right: box plots per bucket ──
        labels = [l for l in _BUCKET_LABELS if l in groups]
        for i, label in enumerate(labels):
            sub = df[df["intensity"] == label]
            fig.add_trace(go.Box(
                y=sub["next_day_readiness"],
                name=label,
                marker_color=_BUCKET_COLORS[i % len(_BUCKET_COLORS)],
                boxmean=True,
                hoverinfo="y+name",
            ), row=1, col=2)

        # Correlation annotation
        sig_text = "significant" if corr["p_value"] < 0.05 else "not significant"
        fig.add_annotation(
            text=f"ρ = {corr['rho']:.2f}, p = {corr['p_value']:.3f} ({sig_text})",
            xref="paper", yref="paper", x=0.25, y=1.08,
            showarrow=False,
            font=dict(color=theme.TEXT_MUTED, size=11),
        )

        theme.style_figure(
            fig,
            n=result.statistics["total_n"],
            title="Workout Intensity → Next-Day Recovery",
        )
        fig.update_xaxes(title_text="Total Output (kJ)", row=1, col=1)
        fig.update_yaxes(title_text="Next-Day Readiness", row=1, col=1)
        fig.update_xaxes(title_text="Intensity Bucket", row=1, col=2)
        fig.update_yaxes(title_text="", row=1, col=2)
        fig.update_layout(showlegend=True, height=450)

        return fig

    def narrate(self, result: InsightResult) -> str:
        s = result.statistics
        groups = s["groups"]
        corr = s["correlation"]

        parts = []
        for name, g in groups.items():
            parts.append(f"**{name}** ({g['output_range']} kJ): avg recovery {g['mean']} (n={g['n']})")

        narrative = "Recovery by intensity bucket: " + "; ".join(parts) + ". "

        direction = "negatively" if corr["rho"] < 0 else "positively"
        strength = "weakly" if abs(corr["rho"]) < 0.3 else "moderately" if abs(corr["rho"]) < 0.5 else "strongly"
        narrative += f"Workout output is {strength} {direction} correlated with next-day readiness (ρ={corr['rho']:.2f}). "

        recent = s.get("recent_14d_mean")
        prior = s.get("prior_mean")
        if recent and prior:
            delta = recent - prior
            if abs(delta) > 1:
                direction = "up" if delta > 0 else "down"
                narrative += f"Your last 14 days of recovery averaged {recent:.0f}, {direction} {abs(delta):.1f} points from your baseline. "

        return narrative
