"""
Bio Insights Engine - Correlation Discovery Engine

Automated scanner that tests all N-choose-2 metric pairs at lags 0-3 days
using Spearman rank correlation with Bonferroni correction. Also scans for
threshold/conditional effects using Mann-Whitney U tests.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import date, datetime

import numpy as np
import pandas as pd
from scipy import stats

from insights_engine.config import DISCOVERY_CONFIG
from insights_engine.core.athena_client import AthenaClient


# ── Metric Registry ─────────────────────────────────────────────────────

METRIC_REGISTRY = {
    "oura": [
        "readiness_score", "sleep_score", "deep_sleep_score",
        "rem_sleep_score", "total_sleep_score", "sleep_efficiency_score",
        "hrv_balance_score", "temperature_deviation",
    ],
    "healthkit": [
        "resting_heart_rate_bpm", "hrv_ms", "vo2_max",
        "respiratory_rate", "blood_oxygen_pct",
    ],
    "activity": [
        "steps", "active_calories", "total_output_kj",
        "had_workout", "total_workout_minutes",
    ],
    "body": [
        "weight_lbs", "body_fat_pct",
    ],
    "nutrition": [
        "daily_calories", "protein_g", "carbs_g", "fat_g",
    ],
    "derived": [
        "combined_wellness_score",
    ],
}

ALL_METRICS = [m for group in METRIC_REGISTRY.values() for m in group]

# Known-obvious pairs to exclude from results (not from scanning)
TRIVIAL_PAIRS = {
    frozenset({"readiness_score", "sleep_score"}),
    frozenset({"readiness_score", "hrv_balance_score"}),
    frozenset({"readiness_score", "combined_wellness_score"}),
    frozenset({"sleep_score", "combined_wellness_score"}),
    frozenset({"sleep_score", "total_sleep_score"}),
    frozenset({"sleep_score", "sleep_efficiency_score"}),
    frozenset({"sleep_score", "deep_sleep_score"}),
    frozenset({"sleep_score", "rem_sleep_score"}),
    frozenset({"deep_sleep_score", "rem_sleep_score"}),
    frozenset({"deep_sleep_score", "total_sleep_score"}),
    frozenset({"rem_sleep_score", "total_sleep_score"}),
    frozenset({"daily_calories", "carbs_g"}),
    frozenset({"daily_calories", "fat_g"}),
    frozenset({"daily_calories", "protein_g"}),
    frozenset({"protein_g", "carbs_g"}),
    frozenset({"protein_g", "fat_g"}),
    frozenset({"carbs_g", "fat_g"}),
    frozenset({"active_calories", "total_output_kj"}),
    frozenset({"active_calories", "steps"}),
    frozenset({"active_calories", "total_workout_minutes"}),
    frozenset({"total_output_kj", "total_workout_minutes"}),
    frozenset({"resting_heart_rate_bpm", "hrv_ms"}),
    frozenset({"hrv_ms", "hrv_balance_score"}),
    frozenset({"weight_lbs", "body_fat_pct"}),
    frozenset({"total_sleep_score", "combined_wellness_score"}),
    frozenset({"readiness_score", "total_sleep_score"}),
    frozenset({"readiness_score", "deep_sleep_score"}),
    frozenset({"readiness_score", "rem_sleep_score"}),
    frozenset({"sleep_efficiency_score", "total_sleep_score"}),
    frozenset({"sleep_efficiency_score", "deep_sleep_score"}),
}


# ── Dataclasses ──────────────────────────────────────────────────────────

@dataclass
class CorrelationFinding:
    metric_a: str
    metric_b: str
    lag: int
    rho: float
    p_value: float
    p_corrected: float
    n_samples: int
    strength: str       # "weak", "moderate", "strong", "very_strong"
    confidence: float   # abs(rho) * (1 - p_corrected)
    narrative: str = ""
    is_new: bool = False

    def to_dict(self) -> dict:
        return {
            "metric_a": self.metric_a,
            "metric_b": self.metric_b,
            "lag": self.lag,
            "rho": round(self.rho, 4),
            "p_value": round(self.p_value, 6),
            "p_corrected": round(self.p_corrected, 6),
            "n_samples": self.n_samples,
            "strength": self.strength,
            "confidence": round(self.confidence, 4),
            "narrative": self.narrative,
            "is_new": self.is_new,
        }

    @classmethod
    def from_dict(cls, d: dict) -> CorrelationFinding:
        return cls(**d)


@dataclass
class ThresholdFinding:
    trigger_metric: str
    outcome_metric: str
    threshold: float
    direction: str          # "above" or "below"
    mean_above: float
    mean_below: float
    delta: float
    p_value: float
    n_above: int
    n_below: int
    confidence: float
    narrative: str = ""
    is_new: bool = False

    def to_dict(self) -> dict:
        return {
            "trigger_metric": self.trigger_metric,
            "outcome_metric": self.outcome_metric,
            "threshold": round(self.threshold, 2),
            "direction": self.direction,
            "mean_above": round(self.mean_above, 2),
            "mean_below": round(self.mean_below, 2),
            "delta": round(self.delta, 2),
            "p_value": round(self.p_value, 6),
            "n_above": self.n_above,
            "n_below": self.n_below,
            "confidence": round(self.confidence, 4),
            "narrative": self.narrative,
            "is_new": self.is_new,
        }

    @classmethod
    def from_dict(cls, d: dict) -> ThresholdFinding:
        return cls(**d)


@dataclass
class DiscoveryResult:
    run_date: str
    lookback_days: int
    data_range_start: str
    data_range_end: str
    total_rows: int
    pairs_tested: int
    correlations: list[CorrelationFinding] = field(default_factory=list)
    thresholds: list[ThresholdFinding] = field(default_factory=list)
    top_finding_narrative: str = ""

    def to_dict(self) -> dict:
        return {
            "run_date": self.run_date,
            "lookback_days": self.lookback_days,
            "data_range_start": self.data_range_start,
            "data_range_end": self.data_range_end,
            "total_rows": self.total_rows,
            "pairs_tested": self.pairs_tested,
            "correlations": [c.to_dict() for c in self.correlations],
            "thresholds": [t.to_dict() for t in self.thresholds],
            "top_finding_narrative": self.top_finding_narrative,
        }

    @classmethod
    def from_dict(cls, d: dict) -> DiscoveryResult:
        return cls(
            run_date=d["run_date"],
            lookback_days=d["lookback_days"],
            data_range_start=d["data_range_start"],
            data_range_end=d["data_range_end"],
            total_rows=d["total_rows"],
            pairs_tested=d["pairs_tested"],
            correlations=[CorrelationFinding.from_dict(c) for c in d.get("correlations", [])],
            thresholds=[ThresholdFinding.from_dict(t) for t in d.get("thresholds", [])],
            top_finding_narrative=d.get("top_finding_narrative", ""),
        )


# ── Helpers ──────────────────────────────────────────────────────────────

def _classify_strength(rho: float) -> str:
    """Classify correlation strength by absolute rho."""
    r = abs(rho)
    if r >= 0.7:
        return "very_strong"
    elif r >= 0.5:
        return "strong"
    elif r >= 0.3:
        return "moderate"
    return "weak"


def _format_metric(name: str) -> str:
    """Human-readable metric name."""
    return name.replace("_", " ").replace("bpm", "(bpm)").replace("pct", "(%)").title()


def _build_correlation_narrative(metric_a: str, metric_b: str, rho: float, lag: int) -> str:
    """Generate natural-language narrative for a correlation finding."""
    a_label = _format_metric(metric_a)
    b_label = _format_metric(metric_b)
    direction = "positively" if rho > 0 else "inversely"
    strength = _classify_strength(rho)

    lag_text = ""
    if lag > 0:
        lag_text = f" with a {lag}-day lag"

    return (
        f"{a_label} is {strength}ly {direction} correlated with "
        f"{b_label} (rho={rho:+.2f}{lag_text})."
    )


def _build_threshold_narrative(trigger: str, outcome: str, threshold: float,
                               mean_above: float, mean_below: float) -> str:
    """Generate natural-language narrative for a threshold finding."""
    t_label = _format_metric(trigger)
    o_label = _format_metric(outcome)
    delta = mean_above - mean_below
    direction = "higher" if delta > 0 else "lower"
    return (
        f"When {t_label} >= {threshold:.0f}, next-day {o_label} is "
        f"{abs(delta):.1f} points {direction} on average "
        f"({mean_above:.1f} vs {mean_below:.1f})."
    )


# ── Core Engine ──────────────────────────────────────────────────────────

class CorrelationDiscoveryEngine:
    """Scans all metric pairs for significant correlations and threshold effects."""

    def __init__(self, athena: AthenaClient):
        self.athena = athena
        self.cfg = DISCOVERY_CONFIG

    def discover(
        self,
        lookback_days: int = None,
        min_rho: float = None,
        alpha: float = None,
        prior_results: DiscoveryResult = None,
    ) -> DiscoveryResult:
        """Run full correlation discovery scan."""
        lookback_days = lookback_days or self.cfg["default_lookback_days"]
        min_rho = min_rho or self.cfg["default_min_rho"]
        alpha = alpha or self.cfg["default_alpha"]
        max_lags = self.cfg["max_lags"]

        # Load data
        df = self._load_all_metrics(lookback_days)
        if df.empty:
            return DiscoveryResult(
                run_date=date.today().isoformat(),
                lookback_days=lookback_days,
                data_range_start="",
                data_range_end="",
                total_rows=0,
                pairs_tested=0,
                top_finding_narrative="No data available for discovery scan.",
            )

        # Identify valid numeric columns with enough data
        min_samples = self.cfg["min_samples_correlation"]
        valid_cols = [
            c for c in ALL_METRICS
            if c in df.columns and df[c].notna().sum() >= min_samples
        ]

        # Build pairs, excluding trivial
        all_pairs = list(itertools.combinations(valid_cols, 2))
        pairs = [
            (a, b) for a, b in all_pairs
            if frozenset({a, b}) not in TRIVIAL_PAIRS
        ]

        # Bonferroni correction
        num_tests = len(pairs) * max_lags
        bonferroni_alpha = alpha / max(num_tests, 1)

        # Scan correlations
        correlations = self._scan_correlations(df, pairs, max_lags, bonferroni_alpha, min_rho)

        # Scan threshold effects (separate Bonferroni for threshold tests)
        thresholds = self._scan_thresholds(df, valid_cols, alpha)

        # Mark new findings if prior results provided
        if prior_results:
            prior_corr_keys = {
                (c.metric_a, c.metric_b, c.lag) for c in prior_results.correlations
            }
            prior_thresh_keys = {
                (t.trigger_metric, t.outcome_metric) for t in prior_results.thresholds
            }
            for c in correlations:
                if (c.metric_a, c.metric_b, c.lag) not in prior_corr_keys:
                    c.is_new = True
            for t in thresholds:
                if (t.trigger_metric, t.outcome_metric) not in prior_thresh_keys:
                    t.is_new = True

        # Sort by confidence
        correlations.sort(key=lambda c: c.confidence, reverse=True)
        thresholds.sort(key=lambda t: t.confidence, reverse=True)

        # Cap results
        correlations = correlations[:self.cfg["max_results_correlations"]]
        thresholds = thresholds[:self.cfg["max_results_thresholds"]]

        # Build top narrative
        top_narrative = self._build_top_narrative(correlations, thresholds)

        # Date range from data
        dates = pd.to_datetime(df["date"])
        data_start = dates.min().strftime("%Y-%m-%d")
        data_end = dates.max().strftime("%Y-%m-%d")

        return DiscoveryResult(
            run_date=date.today().isoformat(),
            lookback_days=lookback_days,
            data_range_start=data_start,
            data_range_end=data_end,
            total_rows=len(df),
            pairs_tested=len(pairs),
            correlations=correlations,
            thresholds=thresholds,
            top_finding_narrative=top_narrative,
        )

    def _load_all_metrics(self, lookback_days: int) -> pd.DataFrame:
        """Load all metrics from the Gold daily table."""
        cols = ", ".join(["date"] + ALL_METRICS)
        sql = f"""
        SELECT {cols}
        FROM bio_gold.daily_readiness_performance
        WHERE COALESCE(
                TRY(CAST(date AS date)),
                TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
              ) >= CURRENT_DATE - INTERVAL '{lookback_days}' DAY
        ORDER BY date
        """
        df = self.athena.execute_query(sql)

        # Coerce numeric columns
        for col in ALL_METRICS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Convert had_workout boolean
        if "had_workout" in df.columns:
            df["had_workout"] = df["had_workout"].apply(
                lambda x: 1.0 if str(x).lower() in ("true", "1", "1.0") else 0.0
            )

        return df

    def _scan_correlations(
        self,
        df: pd.DataFrame,
        pairs: list[tuple[str, str]],
        max_lags: int,
        bonferroni_alpha: float,
        min_rho: float,
    ) -> list[CorrelationFinding]:
        """Scan all pairs at lags 0 to max_lags-1 using Spearman."""
        findings = []
        min_samples = self.cfg["min_samples_correlation"]

        for metric_a, metric_b in pairs:
            for lag in range(max_lags):
                if lag == 0:
                    a_vals = df[metric_a]
                    b_vals = df[metric_b]
                else:
                    a_vals = df[metric_a].iloc[:-lag]
                    b_vals = df[metric_b].iloc[lag:]

                # Align and drop NaN
                mask = a_vals.notna().values & b_vals.notna().values
                a_clean = a_vals.values[mask]
                b_clean = b_vals.values[mask]

                if len(a_clean) < min_samples:
                    continue

                # Check variance
                if np.std(a_clean) == 0 or np.std(b_clean) == 0:
                    continue

                rho, p_value = stats.spearmanr(a_clean, b_clean)

                if np.isnan(rho):
                    continue

                p_corrected = min(p_value * max(len(pairs) * max_lags, 1), 1.0)

                if abs(rho) < min_rho or p_corrected >= 0.05:
                    continue

                confidence = abs(rho) * (1 - p_corrected)
                strength = _classify_strength(rho)
                narrative = _build_correlation_narrative(metric_a, metric_b, rho, lag)

                findings.append(CorrelationFinding(
                    metric_a=metric_a,
                    metric_b=metric_b,
                    lag=lag,
                    rho=rho,
                    p_value=p_value,
                    p_corrected=p_corrected,
                    n_samples=len(a_clean),
                    strength=strength,
                    confidence=confidence,
                    narrative=narrative,
                ))

        return findings

    def _scan_thresholds(
        self,
        df: pd.DataFrame,
        valid_cols: list[str],
        alpha: float,
    ) -> list[ThresholdFinding]:
        """Scan for threshold/conditional effects using Mann-Whitney U."""
        findings = []
        min_samples = self.cfg["min_samples_threshold"]
        min_delta = 1.0

        # Trigger metrics: activity/body metrics that have meaningful thresholds
        trigger_metrics = [
            "total_output_kj", "steps", "active_calories",
            "daily_calories", "protein_g", "total_workout_minutes",
            "deep_sleep_score", "total_sleep_score",
        ]
        # Outcome metrics: recovery/readiness indicators
        outcome_metrics = [
            "readiness_score", "sleep_score", "hrv_ms",
            "resting_heart_rate_bpm", "combined_wellness_score",
        ]

        trigger_cols = [c for c in trigger_metrics if c in valid_cols]
        outcome_cols = [c for c in outcome_metrics if c in valid_cols]

        # Bonferroni for threshold tests
        num_threshold_tests = max(len(trigger_cols) * len(outcome_cols), 1)
        threshold_alpha = alpha / num_threshold_tests

        for trigger in trigger_cols:
            threshold = df[trigger].quantile(0.75)
            if pd.isna(threshold) or threshold == 0:
                continue

            for outcome in outcome_cols:
                if trigger == outcome:
                    continue

                # Use next-day outcome (lag 1)
                trigger_vals = df[trigger].iloc[:-1]
                outcome_vals = df[outcome].iloc[1:]

                mask = trigger_vals.notna().values & outcome_vals.notna().values
                t_clean = trigger_vals.values[mask]
                o_clean = outcome_vals.values[mask]

                above_mask = t_clean >= threshold
                below_mask = ~above_mask

                group_above = o_clean[above_mask]
                group_below = o_clean[below_mask]

                if len(group_above) < min_samples or len(group_below) < min_samples:
                    continue

                mean_above = float(np.mean(group_above))
                mean_below = float(np.mean(group_below))
                delta = mean_above - mean_below

                if abs(delta) < min_delta:
                    continue

                try:
                    _, p_value = stats.mannwhitneyu(
                        group_above, group_below, alternative="two-sided"
                    )
                except ValueError:
                    continue

                if p_value >= threshold_alpha:
                    continue

                confidence = abs(delta) / max(abs(mean_below), 1) * (1 - p_value)
                direction = "above" if delta != 0 else "above"
                narrative = _build_threshold_narrative(
                    trigger, outcome, threshold, mean_above, mean_below
                )

                findings.append(ThresholdFinding(
                    trigger_metric=trigger,
                    outcome_metric=outcome,
                    threshold=threshold,
                    direction=direction,
                    mean_above=mean_above,
                    mean_below=mean_below,
                    delta=delta,
                    p_value=p_value,
                    n_above=len(group_above),
                    n_below=len(group_below),
                    confidence=confidence,
                    narrative=narrative,
                ))

        return findings

    def _build_top_narrative(
        self,
        correlations: list[CorrelationFinding],
        thresholds: list[ThresholdFinding],
    ) -> str:
        """Build a one-sentence narrative for the #1 discovery (used in morning briefing)."""
        # Prefer the highest-confidence finding across both types
        best_corr = correlations[0] if correlations else None
        best_thresh = thresholds[0] if thresholds else None

        if best_corr and best_thresh:
            if best_corr.confidence >= best_thresh.confidence:
                return best_corr.narrative
            return best_thresh.narrative
        elif best_corr:
            return best_corr.narrative
        elif best_thresh:
            return best_thresh.narrative
        return "No significant discoveries found in this scan."
