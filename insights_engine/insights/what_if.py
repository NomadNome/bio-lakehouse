"""
Bio Insights Engine - What-If Simulator

Interactive scenario modeling using personal historical data.
Predicts readiness, energy state, and overtraining risk based on
sleep, workout type, intensity, and consecutive workout days.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy import stats

from insights_engine.config import (
    ENERGY_THRESHOLDS,
    INTENSITY_OUTPUT_DEFAULTS,
    WORKOUT_TSS_ESTIMATES,
)
from insights_engine.core.athena_client import AthenaClient
from insights_engine.config import GOLD_DB


@dataclass
class Scenario:
    sleep_score: int = 80
    workout_type: str = "rest"
    workout_intensity: str = "none"
    consecutive_workout_days: int = 0
    training_stress_balance: float | None = None


@dataclass
class SimulationResult:
    predicted_readiness: float = 0.0
    confidence_range: tuple = (0.0, 0.0)
    energy_state: str = "moderate"
    overtraining_risk: str = "low"
    recommendation: str = ""
    comparison_to_baseline: float = 0.0
    supporting_data: dict = field(default_factory=dict)


@dataclass
class DayPlan:
    day_offset: int = 1
    sleep_score: int = 80
    workout_type: str = "rest"
    workout_intensity: str = "none"


@dataclass
class DayProjection:
    day_offset: int = 1
    date_label: str = ""
    predicted_readiness: float = 0.0
    confidence_range: tuple = (0.0, 0.0)
    energy_state: str = "moderate"
    overtraining_risk: str = "low"
    recommendation: str = ""
    consecutive_workout_days: int = 0
    estimated_tss: float = 0.0
    projected_ctl: float = 0.0
    projected_atl: float = 0.0
    projected_tsb: float = 0.0


@dataclass
class MultiDayResult:
    projections: list = field(default_factory=list)
    baseline_readiness_7d: float = 0.0
    starting_ctl: float = 0.0
    starting_atl: float = 0.0
    plan_summary: str = ""


class WhatIfSimulator:
    """Projects outcomes for user-defined scenarios using historical data."""

    def __init__(self, athena: AthenaClient):
        self.athena = athena
        self._models: dict | None = None

    def load_historical_models(self) -> dict:
        """Query gold views and build lookup tables for simulation."""
        if self._models is not None:
            return self._models

        models: dict = {}

        # 1. Sleep → readiness regression from sleep_performance_prediction
        sleep_df = self.athena.execute_query(f"""
            SELECT
                prev_night_sleep AS sleep_score,
                sleep_quality,
                next_day_readiness
            FROM {GOLD_DB}.sleep_performance_prediction
            WHERE prev_night_sleep IS NOT NULL
              AND next_day_readiness IS NOT NULL
        """)
        models["sleep_regression"] = self._fit_sleep_regression(sleep_df)
        models["sleep_buckets"] = self._build_sleep_buckets(sleep_df)

        # 2. Workout intensity → next-day readiness change. This is still
        # observational, but its temporal direction is valid: workout first,
        # following-day readiness second. The old same-day readiness buckets
        # suffered from selection bias (high-readiness days invite workouts).
        workout_df = self.athena.execute_query(f"""
            SELECT
                intensity,
                readiness_delta_d1
            FROM {GOLD_DB}.workout_recovery_windows
            WHERE readiness_delta_d1 IS NOT NULL
        """)
        models["workout_intensity_effects"] = self._build_workout_effects(workout_df)

        # 3. Baseline stats from dashboard_30day
        baseline_df = self.athena.execute_query(f"""
            SELECT
                readiness_score,
                sleep_score,
                total_output_kj,
                had_workout,
                readiness_7day_avg
            FROM {GOLD_DB}.dashboard_30day
            WHERE readiness_score IS NOT NULL
        """)
        models["baseline"] = self._build_baseline(baseline_df)

        # 4. Overtraining risk — latest entry with workout count
        risk_df = self.athena.execute_query(f"""
            SELECT
                workouts_last_3_days,
                overtraining_risk
            FROM {GOLD_DB}.overtraining_risk
            WHERE date IS NOT NULL
            ORDER BY date DESC
            LIMIT 1
        """)
        models["current_streak"] = self._extract_streak(risk_df)

        # 5. TSS history for seeding CTL/ATL in multi-day planning
        tss_df = self.athena.execute_query(f"""
            SELECT date, tss
            FROM {GOLD_DB}.training_load_daily
            WHERE tss IS NOT NULL
            ORDER BY date
        """)
        models["tss_history"] = self._compute_starting_loads(tss_df)

        self._models = models
        return models

    def simulate(self, scenario: Scenario) -> SimulationResult:
        """Run a what-if simulation for the given scenario."""
        models = self.load_historical_models()
        reg = models["sleep_regression"]
        baseline = models["baseline"]

        # Step 1: base readiness from sleep score via regression
        if reg["valid"]:
            base_readiness = reg["slope"] * scenario.sleep_score + reg["intercept"]
        else:
            base_readiness = baseline["mean_readiness"]

        # Step 2: apply a shrunk, observational next-day workout association.
        workout_delta = self._get_workout_delta(
            scenario.workout_intensity, models["workout_intensity_effects"]
        )

        # Step 3: feed current/projected CTL-ATL form into readiness. This was
        # previously displayed after the fact but did not influence the plan.
        if scenario.training_stress_balance is None:
            loads = models.get("tss_history", {"ctl": 0.0, "atl": 0.0})
            tsb = loads["ctl"] - loads["atl"]
        else:
            tsb = scenario.training_stress_balance
        form_adjustment = self._training_load_adjustment(tsb)
        adjusted_readiness = base_readiness + workout_delta + form_adjustment

        # Step 4: adjust for consecutive workouts (overtraining penalty)
        overtraining_penalty = self._overtraining_penalty(
            scenario.consecutive_workout_days
        )
        predicted_readiness = max(0, min(100, adjusted_readiness + overtraining_penalty))

        # Step 5: outcome range from matching sleep bucket
        historical_range = self._get_confidence_range(
            scenario.sleep_score, predicted_readiness, models["sleep_buckets"]
        )

        # Step 6: classify energy state
        energy_state = self._classify_energy(predicted_readiness, scenario.sleep_score)

        # Step 7: overtraining risk level
        overtraining_risk = self._classify_overtraining_risk(
            scenario.consecutive_workout_days
        )

        # Step 8: recommendation
        recommendation = self._make_recommendation(
            energy_state, overtraining_risk, scenario
        )

        # Step 9: comparison to baseline
        comparison = round(predicted_readiness - baseline["avg_readiness_7d"], 1)

        # Supporting data
        bucket_key = self._sleep_bucket_key(scenario.sleep_score)
        bucket_data = models["sleep_buckets"].get(bucket_key, {})

        return SimulationResult(
            predicted_readiness=round(predicted_readiness, 1),
            confidence_range=(round(historical_range[0], 1), round(historical_range[1], 1)),
            energy_state=energy_state,
            overtraining_risk=overtraining_risk,
            recommendation=recommendation,
            comparison_to_baseline=comparison,
            supporting_data={
                "regression_r": reg.get("r", None),
                "regression_n": reg.get("n", 0),
                "sleep_bucket": bucket_key,
                "bucket_n": bucket_data.get("n", 0),
                "bucket_mean_readiness": bucket_data.get("mean", None),
                "baseline_7d_readiness": baseline["avg_readiness_7d"],
                "workout_delta": round(workout_delta, 1),
                "workout_delta_method": "observed_next_day_change_shrunk_to_zero",
                "training_stress_balance": round(float(tsb), 1),
                "training_load_adjustment": round(form_adjustment, 1),
                "overtraining_penalty": round(overtraining_penalty, 1),
                "total_historical_days": baseline["total_days"],
            },
        )

    # ── Internal model builders ─────────────────────────────────────────

    @staticmethod
    def _fit_sleep_regression(df: pd.DataFrame) -> dict:
        if len(df) < 5:
            return {"valid": False, "slope": 0, "intercept": 0, "r": 0, "n": len(df)}
        x = df["sleep_score"].astype(float).values
        y = df["next_day_readiness"].astype(float).values
        slope, intercept, r, p, stderr = stats.linregress(x, y)
        return {
            "valid": True,
            "slope": slope,
            "intercept": intercept,
            "r": r,
            "p": p,
            "stderr": stderr,
            "n": len(df),
        }

    @staticmethod
    def _build_sleep_buckets(df: pd.DataFrame) -> dict:
        buckets = {}
        if "sleep_quality" not in df.columns or df.empty:
            return buckets
        for quality, group in df.groupby("sleep_quality"):
            readiness = group["next_day_readiness"].astype(float)
            buckets[str(quality)] = {
                "mean": round(float(readiness.mean()), 1),
                "std": round(float(readiness.std()), 1) if len(group) > 1 else 10.0,
                "n": len(group),
            }
        return buckets

    @staticmethod
    def _build_workout_effects(df: pd.DataFrame) -> dict:
        effects = {}
        if df.empty:
            return effects
        numeric = df.copy()
        numeric["readiness_delta_d1"] = pd.to_numeric(
            numeric["readiness_delta_d1"], errors="coerce"
        )
        numeric = numeric.dropna(subset=["intensity", "readiness_delta_d1"])
        for intensity, group in numeric.groupby(numeric["intensity"].astype(str).str.lower()):
            raw_mean = float(group["readiness_delta_d1"].mean())
            n = len(group)
            # Small groups are noisy; ten pseudo-observations at zero keep an
            # apparent effect from dominating the scenario score.
            shrunk_mean = raw_mean * n / (n + 10)
            effects[intensity] = {
                "mean_next_day_delta": round(shrunk_mean, 2),
                "raw_mean_next_day_delta": round(raw_mean, 2),
                "n": n,
            }
        return effects

    @staticmethod
    def _build_baseline(df: pd.DataFrame) -> dict:
        if df.empty:
            return {
                "mean_readiness": 75.0,
                "mean_sleep": 75.0,
                "avg_readiness_7d": 75.0,
                "total_days": 0,
            }
        return {
            "mean_readiness": round(float(df["readiness_score"].mean()), 1),
            "mean_sleep": round(float(df["sleep_score"].mean()), 1),
            "avg_readiness_7d": round(
                float(df["readiness_7day_avg"].dropna().iloc[-1])
                if not df["readiness_7day_avg"].dropna().empty
                else float(df["readiness_score"].mean()),
                1,
            ),
            "total_days": len(df),
        }

    @staticmethod
    def _extract_streak(df: pd.DataFrame) -> dict:
        if df.empty:
            return {"consecutive_workout_days": 0, "risk_level": "low"}
        row = df.iloc[0]
        return {
            "consecutive_workout_days": int(row.get("workouts_last_3_days", 0)),
            "risk_level": str(row.get("overtraining_risk", "low_risk")).lower().replace("_risk", ""),
        }

    # ── Simulation helpers ──────────────────────────────────────────────

    def _get_workout_delta(self, intensity: str, effects: dict) -> float:
        intensity = intensity.lower()
        if intensity == "none":
            return 0.0
        observed = effects.get(intensity, effects.get("light") if intensity == "low" else None)
        if observed is not None:
            return float(observed["mean_next_day_delta"])

        # Conservative fallback used only when there are no observed recovery
        # windows for an intensity. These are explicitly assumptions, not
        # learned causal effects.
        return {"low": -0.5, "light": -0.5, "moderate": -1.5, "high": -3.0}.get(
            intensity, -1.0
        )

    @staticmethod
    def _training_load_adjustment(tsb: float) -> float:
        """Bounded readiness adjustment from CTL-ATL form balance."""
        return float(np.clip(float(tsb) * 0.2, -6.0, 3.0))

    @staticmethod
    def _overtraining_penalty(consecutive_days: int) -> float:
        if consecutive_days <= 2:
            return 0.0
        elif consecutive_days <= 4:
            return -2.0 * (consecutive_days - 2)
        elif consecutive_days <= 6:
            return -4.0 - 3.0 * (consecutive_days - 4)
        else:
            return -10.0 - 2.0 * (consecutive_days - 6)

    def _get_confidence_range(
        self, sleep_score: int, predicted: float, buckets: dict
    ) -> tuple:
        bucket_key = self._sleep_bucket_key(sleep_score)
        bucket = buckets.get(bucket_key, {})
        std = bucket.get("std", 10.0)
        return (predicted - std, predicted + std)

    @staticmethod
    def _sleep_bucket_key(sleep_score: int) -> str:
        if sleep_score >= 88:
            return "Excellent (88+)"
        elif sleep_score >= 75:
            return "Good (75-87)"
        elif sleep_score >= 60:
            return "Fair (60-74)"
        else:
            return "Poor (<60)"

    @staticmethod
    def _classify_energy(readiness: float, sleep_score: int) -> str:
        if (
            readiness >= ENERGY_THRESHOLDS["peak"]["readiness"]
            and sleep_score >= ENERGY_THRESHOLDS["peak"]["sleep"]
        ):
            return "peak"
        if (
            readiness >= ENERGY_THRESHOLDS["high"]["readiness"]
            and sleep_score >= ENERGY_THRESHOLDS["high"]["sleep"]
        ):
            return "high"
        if (
            readiness >= ENERGY_THRESHOLDS["moderate"]["readiness"]
            and sleep_score >= ENERGY_THRESHOLDS["moderate"]["sleep"]
        ):
            return "moderate"
        if readiness >= ENERGY_THRESHOLDS["low"]["readiness"]:
            return "low"
        return "recovery_needed"

    @staticmethod
    def _classify_overtraining_risk(consecutive_days: int) -> str:
        if consecutive_days <= 3:
            return "low"
        elif consecutive_days <= 5:
            return "moderate"
        else:
            return "high"

    @staticmethod
    def _make_recommendation(
        energy_state: str, overtraining_risk: str, scenario: Scenario
    ) -> str:
        if overtraining_risk == "high":
            return (
                "Your consecutive workout streak suggests high overtraining risk. "
                "A rest day or light recovery session is strongly recommended."
            )
        if energy_state in ("recovery_needed", "low"):
            return (
                "Predicted energy is low. Consider a rest day or light activity "
                "to allow recovery before pushing harder."
            )
        if energy_state == "peak":
            return (
                "Conditions look ideal for a high-intensity session. "
                "This is a great day to push your limits."
            )
        if energy_state == "high":
            return (
                "Good conditions for a solid workout. Moderate-to-high intensity "
                "should be well-tolerated."
            )
        if overtraining_risk == "moderate":
            return (
                "You're in a moderate training streak. Consider alternating "
                "intensity or adding a recovery day soon."
            )
        return (
            "Moderate energy predicted. A moderate workout should be fine, "
            "but listen to your body."
        )

    # ── Multi-day planning ──────────────────────────────────────────────

    @staticmethod
    def _compute_starting_loads(tss_df: pd.DataFrame) -> dict:
        """Compute latest CTL/ATL from TSS history using EMA."""
        if tss_df.empty:
            return {"ctl": 0.0, "atl": 0.0}
        tss = tss_df["tss"].astype(float)
        ctl_alpha = 2.0 / (42 + 1)
        atl_alpha = 2.0 / (7 + 1)
        ctl = 0.0
        atl = 0.0
        for val in tss:
            ctl = ctl + (val - ctl) * ctl_alpha
            atl = atl + (val - atl) * atl_alpha
        return {"ctl": round(ctl, 1), "atl": round(atl, 1)}

    @staticmethod
    def _estimate_tss(workout_type: str, intensity: str) -> float:
        """Lookup estimated TSS from config table."""
        wtype = workout_type.lower()
        inten = intensity.lower()
        if wtype == "rest" or inten == "none":
            return 0.0
        type_map = WORKOUT_TSS_ESTIMATES.get(wtype, {})
        return float(type_map.get(inten, type_map.get("moderate", 0)))

    def simulate_multi_day(self, plans: list) -> MultiDayResult:
        """Cascading multi-day readiness projection.

        Each day feeds into the next: consecutive workout days accumulate,
        training load builds, and rest days trigger recovery.
        """
        from datetime import date, timedelta

        models = self.load_historical_models()
        baseline = models["baseline"]
        streak = models["current_streak"]
        tss_hist = models["tss_history"]

        ctl = tss_hist["ctl"]
        atl = tss_hist["atl"]
        consecutive = streak["consecutive_workout_days"]

        today = date.today()
        projections = []

        sorted_plans = sorted(plans, key=lambda p: p.day_offset)

        for plan in sorted_plans:
            # Update consecutive workout days
            is_workout = plan.workout_type.lower() != "rest" and plan.workout_intensity.lower() != "none"
            if is_workout:
                consecutive += 1
            else:
                consecutive = 0

            # Estimate TSS and forward-propagate CTL/ATL before predicting the
            # following day's readiness, so the projected form is an input.
            tss = self._estimate_tss(plan.workout_type, plan.workout_intensity)
            ctl = ctl + (tss - ctl) * (2.0 / (42 + 1))
            atl = atl + (tss - atl) * (2.0 / (7 + 1))
            tsb = ctl - atl

            scenario = Scenario(
                sleep_score=plan.sleep_score,
                workout_type=plan.workout_type,
                workout_intensity=plan.workout_intensity,
                consecutive_workout_days=consecutive,
                training_stress_balance=tsb,
            )
            result = self.simulate(scenario)

            # Widen the scenario range by 5% per day offset to reflect that
            # longer-horizon planning is less constrained by observed data.
            base_lo, base_hi = result.confidence_range
            spread = (base_hi - base_lo) / 2
            widened = spread * (1 + 0.05 * plan.day_offset)
            lo = max(0, result.predicted_readiness - widened)
            hi = min(100, result.predicted_readiness + widened)

            proj_date = today + timedelta(days=plan.day_offset)
            date_label = proj_date.strftime("%a %b %-d")

            projections.append(DayProjection(
                day_offset=plan.day_offset,
                date_label=date_label,
                predicted_readiness=result.predicted_readiness,
                confidence_range=(round(lo, 1), round(hi, 1)),
                energy_state=result.energy_state,
                overtraining_risk=result.overtraining_risk,
                recommendation=result.recommendation,
                consecutive_workout_days=consecutive,
                estimated_tss=round(tss, 0),
                projected_ctl=round(ctl, 1),
                projected_atl=round(atl, 1),
                projected_tsb=round(tsb, 1),
            ))

        summary = self._summarize_plan(projections, baseline["avg_readiness_7d"])

        return MultiDayResult(
            projections=projections,
            baseline_readiness_7d=baseline["avg_readiness_7d"],
            starting_ctl=tss_hist["ctl"],
            starting_atl=tss_hist["atl"],
            plan_summary=summary,
        )

    @staticmethod
    def _summarize_plan(projections: list, baseline: float) -> str:
        """Natural-language summary of the multi-day plan."""
        if not projections:
            return "No days planned."
        first = projections[0].predicted_readiness
        last = projections[-1].predicted_readiness
        n = len(projections)

        trend = "stays steady"
        if last > first + 3:
            trend = "trends upward"
        elif last < first - 3:
            trend = "trends downward"

        low_days = [p for p in projections if p.energy_state in ("low", "recovery_needed")]
        low_warning = ""
        if low_days:
            names = ", ".join(p.date_label for p in low_days)
            low_warning = f" Low energy on {names} — consider rest."

        ctl_start = projections[0].projected_ctl
        ctl_end = projections[-1].projected_ctl

        return (
            f"Over the {n}-day plan, readiness {trend} from "
            f"{first:.0f} to {last:.0f}.{low_warning} "
            f"CTL moves {ctl_start:.0f} → {ctl_end:.0f}."
        )
