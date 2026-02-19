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

from insights_engine.config import ENERGY_THRESHOLDS, INTENSITY_OUTPUT_DEFAULTS
from insights_engine.core.athena_client import AthenaClient


@dataclass
class Scenario:
    sleep_score: int = 80
    workout_type: str = "rest"
    workout_intensity: str = "none"
    consecutive_workout_days: int = 0


@dataclass
class SimulationResult:
    predicted_readiness: float = 0.0
    confidence_range: tuple = (0.0, 0.0)
    energy_state: str = "moderate"
    overtraining_risk: str = "low"
    recommendation: str = ""
    comparison_to_baseline: float = 0.0
    supporting_data: dict = field(default_factory=dict)


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
        sleep_df = self.athena.execute_query("""
            SELECT
                prev_night_sleep AS sleep_score,
                sleep_quality,
                next_day_readiness
            FROM bio_gold.sleep_performance_prediction
            WHERE prev_night_sleep IS NOT NULL
              AND next_day_readiness IS NOT NULL
        """)
        models["sleep_regression"] = self._fit_sleep_regression(sleep_df)
        models["sleep_buckets"] = self._build_sleep_buckets(sleep_df)

        # 2. Workout type → readiness from workout_type_optimization
        workout_df = self.athena.execute_query("""
            SELECT
                workout_type,
                readiness_bucket,
                avg_readiness_in_bucket,
                sample_days
            FROM bio_gold.workout_type_optimization
            WHERE avg_readiness_in_bucket IS NOT NULL
        """)
        models["workout_type_effects"] = self._build_workout_effects(workout_df)

        # 3. Baseline stats from dashboard_30day
        baseline_df = self.athena.execute_query("""
            SELECT
                readiness_score,
                sleep_score,
                total_output_kj,
                had_workout,
                readiness_7day_avg
            FROM bio_gold.dashboard_30day
            WHERE readiness_score IS NOT NULL
        """)
        models["baseline"] = self._build_baseline(baseline_df)

        # 4. Overtraining risk — latest entry with workout count
        risk_df = self.athena.execute_query("""
            SELECT
                workouts_last_3_days,
                overtraining_risk
            FROM bio_gold.overtraining_risk
            WHERE date IS NOT NULL
            ORDER BY date DESC
            LIMIT 1
        """)
        models["current_streak"] = self._extract_streak(risk_df)

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

        # Step 2: adjust for workout type
        workout_delta = self._get_workout_delta(
            scenario.workout_type, models["workout_type_effects"], baseline
        )
        adjusted_readiness = base_readiness + workout_delta

        # Step 3: adjust for consecutive workouts (overtraining penalty)
        overtraining_penalty = self._overtraining_penalty(
            scenario.consecutive_workout_days
        )
        predicted_readiness = max(0, min(100, adjusted_readiness + overtraining_penalty))

        # Step 4: confidence range from matching sleep bucket
        confidence = self._get_confidence_range(
            scenario.sleep_score, predicted_readiness, models["sleep_buckets"]
        )

        # Step 5: classify energy state
        energy_state = self._classify_energy(predicted_readiness, scenario.sleep_score)

        # Step 6: overtraining risk level
        overtraining_risk = self._classify_overtraining_risk(
            scenario.consecutive_workout_days
        )

        # Step 7: recommendation
        recommendation = self._make_recommendation(
            energy_state, overtraining_risk, scenario
        )

        # Step 8: comparison to baseline
        comparison = round(predicted_readiness - baseline["avg_readiness_7d"], 1)

        # Supporting data
        bucket_key = self._sleep_bucket_key(scenario.sleep_score)
        bucket_data = models["sleep_buckets"].get(bucket_key, {})

        return SimulationResult(
            predicted_readiness=round(predicted_readiness, 1),
            confidence_range=(round(confidence[0], 1), round(confidence[1], 1)),
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
        for _, row in df.iterrows():
            wtype = str(row.get("workout_type", "unknown")).lower()
            readiness = float(row["avg_readiness_in_bucket"])
            count = int(row.get("sample_days", 1))
            if wtype not in effects:
                effects[wtype] = {"weighted_sum": 0.0, "total_count": 0}
            effects[wtype]["weighted_sum"] += readiness * count
            effects[wtype]["total_count"] += count
        # Compute weighted averages
        for wtype in effects:
            total = effects[wtype]["total_count"]
            effects[wtype]["mean_readiness"] = (
                round(effects[wtype]["weighted_sum"] / total, 1) if total > 0 else 0
            )
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

    def _get_workout_delta(self, workout_type: str, effects: dict, baseline: dict) -> float:
        wtype = workout_type.lower()
        if wtype == "rest" or wtype == "rest day":
            rest_data = effects.get("rest day", effects.get("rest", {}))
            if rest_data.get("mean_readiness"):
                return rest_data["mean_readiness"] - baseline["mean_readiness"]
            return 1.5  # rest days typically give a small readiness boost

        if wtype in effects and effects[wtype].get("mean_readiness"):
            return effects[wtype]["mean_readiness"] - baseline["mean_readiness"]

        # Fallback: higher intensity → larger negative delta
        intensity_penalties = {"none": 0, "low": -1.0, "moderate": -2.0, "high": -4.0}
        return intensity_penalties.get("moderate", -2.0)

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
