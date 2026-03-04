"""
Bio Insights Engine - Signature Insight: Progressive Overload Tracking

Tracks week-over-week progression in cycling output, watts, efficiency,
and HR efficiency.  Ties overload directly to CTL growth so the user
can see whether their increased training stress is translating into
measurable power gains.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from insights_engine.config import OVERLOAD_THRESHOLDS as TH
from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightAnalyzer, InsightResult
from insights_engine.insights.training_load import compute_ema
from insights_engine.viz.progressive_overload_charts import build_overload_chart


class ProgressiveOverloadAnalyzer(InsightAnalyzer):
    """Analyzes progressive overload in cycling workouts."""

    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        workout_df = self._load_workouts()
        tss_df = self._load_tss()

        # Filter to cycling workouts with power data
        df = workout_df[workout_df["total_output"] > 0].copy()

        if df.empty:
            return self._early_exit("No cycling workouts with power data found.")

        df["workout_day"] = pd.to_datetime(df["workout_day"])
        df = df.sort_values("workout_day")

        # ISO week column
        iso = df["workout_day"].dt.isocalendar()
        df["year_week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
        df["week_start"] = df["workout_day"] - pd.to_timedelta(
            df["workout_day"].dt.dayofweek, unit="D"
        )

        n_weeks = df["year_week"].nunique()
        if n_weeks < TH["min_weeks"]:
            return self._early_exit(
                f"Need at least {TH['min_weeks']} weeks of workout data for "
                f"progressive overload tracking (currently have {n_weeks})."
            )

        weekly = self._aggregate_weekly(df)
        weekly = self._compute_deltas(weekly)
        weekly = self._classify_weeks(weekly)

        # CTL context from TSS
        ctl_stats = self._compute_ctl_context(tss_df)

        # Build chart
        chart = build_overload_chart(weekly)

        # Key numbers for metric cards
        latest = weekly.iloc[-1]
        n_workouts = len(df)
        streak = self._progressing_streak(weekly)
        watts_now = round(float(latest["weekly_avg_watts"]), 0)

        # Watts delta vs 4 weeks ago
        if len(weekly) >= 4:
            watts_4w = round(float(weekly["weekly_avg_watts"].iloc[-4]), 0)
            watts_delta = round(watts_now - watts_4w, 0)
        else:
            watts_delta = None

        ctl_now = ctl_stats.get("ctl_now", 0)
        ctl_delta = ctl_stats.get("ctl_delta", 0)

        metrics = [
            {"label": "This Week", "value": latest["status"],
             "delta": None},
            {"label": "Streak", "value": f"{streak}wk" if streak else "—",
             "delta": "progressing" if streak else None},
            {"label": "Avg Watts", "value": f"{watts_now:.0f}W",
             "delta": f"{watts_delta:+.0f}W vs 4wk ago" if watts_delta is not None else None},
            {"label": "CTL", "value": f"{ctl_now:.0f}",
             "delta": f"{ctl_delta:+.1f} over 4wk" if ctl_delta else None},
        ]

        statistics = {
            "n_workouts": n_workouts,
            "n_weeks": n_weeks,
            "latest_week_status": latest["status"],
            "streak_weeks_progressing": streak,
            "ctl_current": ctl_now,
            "ctl_4wk_delta": ctl_delta,
            "metrics": metrics,
        }

        narrative = self._build_narrative(weekly, ctl_stats, n_workouts, n_weeks)

        result = InsightResult(
            insight_type="progressive_overload",
            title="Progressive Overload",
            narrative=narrative,
            statistics=statistics,
            chart=chart,
            data=weekly,
        )
        if n_weeks < 8:
            result.caveats.append(
                f"Only {n_weeks} weeks of data — trends will stabilize with more history."
            )
        return result

    # ── Data Loading ─────────────────────────────────────────────────

    def _load_workouts(self) -> pd.DataFrame:
        sql = """
        SELECT CAST(workout_date AS date) AS workout_day,
            fitness_discipline, title, instructor_name, length_minutes,
            total_output, avg_watts, avg_resistance_pct, avg_cadence_rpm,
            avg_heartrate, calories_burned, output_per_minute, workout_category
        FROM bio_silver.peloton_workouts
        WHERE workout_date IS NOT NULL
        ORDER BY workout_date
        """
        df = self.athena.execute_query(sql)
        for col in ["total_output", "avg_watts", "avg_heartrate",
                     "output_per_minute", "length_minutes", "calories_burned",
                     "avg_resistance_pct", "avg_cadence_rpm"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df

    def _load_tss(self) -> pd.DataFrame:
        sql = """
        SELECT date, tss, had_workout
        FROM bio_gold.training_load_daily
        ORDER BY date
        """
        df = self.athena.execute_query(sql)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df["tss"] = pd.to_numeric(df["tss"], errors="coerce").fillna(0)
            df = df.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        return df

    # ── Weekly Aggregation ───────────────────────────────────────────

    def _aggregate_weekly(self, df: pd.DataFrame) -> pd.DataFrame:
        weekly = (
            df.groupby(["year_week", "week_start"])
            .agg(
                weekly_total_output=("total_output", "sum"),
                weekly_avg_watts=("avg_watts", "mean"),
                weekly_avg_output_per_min=("output_per_minute", "mean"),
                weekly_workout_count=("total_output", "count"),
                weekly_avg_hr=("avg_heartrate", "mean"),
                weekly_total_minutes=("length_minutes", "sum"),
            )
            .reset_index()
            .sort_values("week_start")
            .reset_index(drop=True)
        )
        # HR efficiency: watts per bpm (higher = fitter)
        weekly["weekly_hr_efficiency"] = np.where(
            weekly["weekly_avg_hr"] > 0,
            weekly["weekly_avg_watts"] / weekly["weekly_avg_hr"],
            0,
        )
        return weekly

    def _compute_deltas(self, weekly: pd.DataFrame) -> pd.DataFrame:
        weekly["output_pct_change"] = weekly["weekly_total_output"].pct_change()
        weekly["watts_delta"] = weekly["weekly_avg_watts"].diff()
        weekly["output_per_min_delta"] = weekly["weekly_avg_output_per_min"].diff()
        weekly["hr_efficiency_delta"] = weekly["weekly_hr_efficiency"].diff()
        return weekly

    def _classify_weeks(self, weekly: pd.DataFrame) -> pd.DataFrame:
        statuses = []
        for _, row in weekly.iterrows():
            if pd.isna(row["output_pct_change"]):
                statuses.append("Baseline")
                continue

            positive = 0
            negative = 0

            if row["output_pct_change"] > TH["progression_pct"]:
                positive += 1
            elif row["output_pct_change"] < TH["regression_pct"]:
                negative += 1

            if row["watts_delta"] > TH["watts_change_threshold"]:
                positive += 1
            elif row["watts_delta"] < -TH["watts_change_threshold"]:
                negative += 1

            if row["output_per_min_delta"] > TH["output_per_min_threshold"]:
                positive += 1
            elif row["output_per_min_delta"] < -TH["output_per_min_threshold"]:
                negative += 1

            if row["hr_efficiency_delta"] > TH["hr_efficiency_threshold"]:
                positive += 1
            elif row["hr_efficiency_delta"] < -TH["hr_efficiency_threshold"]:
                negative += 1

            if positive >= 2:
                statuses.append("Progressing")
            elif negative >= 2:
                statuses.append("Regressing")
            else:
                statuses.append("Maintaining")

        weekly["status"] = statuses
        return weekly

    # ── CTL Context ──────────────────────────────────────────────────

    def _compute_ctl_context(self, tss_df: pd.DataFrame) -> dict:
        if tss_df.empty or len(tss_df) < 7:
            return {"ctl_now": 0, "ctl_4w": 0, "ctl_delta": 0}

        tss_df["ctl"] = compute_ema(tss_df["tss"], span=42)

        ctl_now = float(tss_df["ctl"].iloc[-1])
        four_weeks_ago = tss_df["date"].max() - pd.Timedelta(days=28)
        ctl_4w_rows = tss_df[tss_df["date"] <= four_weeks_ago]
        ctl_4w = float(ctl_4w_rows["ctl"].iloc[-1]) if not ctl_4w_rows.empty else ctl_now

        return {
            "ctl_now": round(ctl_now, 1),
            "ctl_4w": round(ctl_4w, 1),
            "ctl_delta": round(ctl_now - ctl_4w, 1),
        }

    # ── Narrative ────────────────────────────────────────────────────

    def _build_narrative(
        self,
        weekly: pd.DataFrame,
        ctl_stats: dict,
        n_workouts: int,
        n_weeks: int,
    ) -> str:
        latest = weekly.iloc[-1]
        status = latest["status"]

        # Overall trend
        prog = (weekly["status"] == "Progressing").sum()
        reg = (weekly["status"] == "Regressing").sum()
        if prog > reg:
            trend = "trending upward"
        elif reg > prog:
            trend = "trending downward"
        else:
            trend = "holding steady"

        # CTL interpretation
        ctl_d = ctl_stats.get("ctl_delta", 0)
        if ctl_d > 2:
            ctl_note = "CTL is rising — your fitness base is growing."
        elif ctl_d < -2:
            ctl_note = "CTL is dropping — consider increasing volume or check recovery."
        else:
            ctl_note = "CTL is stable."

        return (
            f"Over {n_weeks} weeks ({n_workouts} rides), output is **{trend}**. "
            f"This week: **{status}**. {ctl_note}"
        )

    # ── Helpers ──────────────────────────────────────────────────────

    def _progressing_streak(self, weekly: pd.DataFrame) -> int:
        streak = 0
        for status in reversed(weekly["status"].tolist()):
            if status == "Progressing":
                streak += 1
            else:
                break
        return streak

    def _early_exit(self, message: str) -> InsightResult:
        return InsightResult(
            insight_type="progressive_overload",
            title="Progressive Overload",
            narrative=message,
            statistics={},
            caveats=["Insufficient data."],
        )

    def visualize(self, result: InsightResult) -> go.Figure:
        return result.chart

    def narrate(self, result: InsightResult) -> str:
        return result.narrative
