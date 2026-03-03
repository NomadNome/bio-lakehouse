"""
Experiment Analyzer — Bayesian & Difference-in-Differences Analysis

Primary: Normal-Normal conjugate Bayesian analysis (scipy only, no PyMC)
Secondary: Difference-in-differences with parallel trends validation
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
from scipy import stats

from insights_engine.experiments.tracker import Intervention


@dataclass
class BayesianResult:
    """Result of Bayesian Normal-Normal conjugate analysis."""
    pre_mean: float
    pre_std: float
    post_mean: float
    post_std: float
    n_pre: int
    n_post: int
    posterior_mean_effect: float
    credible_interval_95: tuple[float, float]
    prob_positive: float
    cohens_d: float
    verdict: str


@dataclass
class DiDResult:
    """Result of Difference-in-Differences analysis."""
    pre_trend_slope: float
    pre_trend_r2: float
    counterfactual_post_mean: float
    actual_post_mean: float
    did_effect: float
    parallel_trends_valid: bool
    warning: str | None


def _get_metric_data(
    athena, metric_col: str, start_date: str, end_date: str
) -> pd.DataFrame:
    """Fetch daily metric data from Gold layer."""
    query = f"""
        SELECT date, {metric_col}
        FROM bio_gold.daily_readiness_performance
        WHERE {metric_col} IS NOT NULL
          AND COALESCE(
                TRY(CAST(date AS date)),
                TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
              ) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ORDER BY date
    """
    df = athena.execute_query(query)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")
        df = df.dropna(subset=[metric_col])
    return df


def get_pre_post_data(
    athena,
    intervention: Intervention,
    metric_col: str,
    pre_days: int = 14,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split metric data into pre-intervention and post-intervention periods."""
    intv_start = date.fromisoformat(intervention.start_date)

    if intervention.end_date:
        intv_end = date.fromisoformat(intervention.end_date)
        # Post period = start through end + washout
        post_end = intv_end + timedelta(days=intervention.washout_days)
    else:
        # Still active — post period is start through today
        intv_end = date.today()
        post_end = intv_end

    pre_start = intv_start - timedelta(days=pre_days)

    pre_df = _get_metric_data(
        athena, metric_col,
        pre_start.isoformat(),
        (intv_start - timedelta(days=1)).isoformat(),
    )

    post_df = _get_metric_data(
        athena, metric_col,
        intv_start.isoformat(),
        post_end.isoformat(),
    )

    return pre_df, post_df


def bayesian_analysis(
    pre_values: np.ndarray,
    post_values: np.ndarray,
) -> BayesianResult:
    """
    Normal-Normal conjugate Bayesian analysis.

    Uses a weakly informative prior centered on the pre-period mean.
    """
    pre_mean = float(np.mean(pre_values))
    pre_std = float(np.std(pre_values, ddof=1)) if len(pre_values) > 1 else 1.0
    post_mean = float(np.mean(post_values))
    post_std = float(np.std(post_values, ddof=1)) if len(post_values) > 1 else 1.0
    n_pre = len(pre_values)
    n_post = len(post_values)

    # Prior: N(pre_mean, pre_std²)
    prior_mean = pre_mean
    prior_var = pre_std ** 2

    # Likelihood: post observations
    likelihood_var = post_std ** 2 / max(n_post, 1)

    # Posterior (conjugate update)
    posterior_var = 1.0 / (1.0 / prior_var + 1.0 / likelihood_var) if prior_var > 0 else likelihood_var
    posterior_mean = posterior_var * (prior_mean / prior_var + post_mean / likelihood_var) if prior_var > 0 else post_mean

    posterior_std = np.sqrt(posterior_var)
    effect = posterior_mean - prior_mean

    # 95% credible interval on the effect
    ci_low = float(effect - 1.96 * posterior_std)
    ci_high = float(effect + 1.96 * posterior_std)

    # P(effect > 0)
    if posterior_std > 0:
        prob_positive = float(1.0 - stats.norm.cdf(0, loc=effect, scale=posterior_std))
    else:
        prob_positive = 1.0 if effect > 0 else 0.0

    # Cohen's d
    pooled_std = np.sqrt(((n_pre - 1) * pre_std**2 + (n_post - 1) * post_std**2) / max(n_pre + n_post - 2, 1))
    cohens_d = float(effect / pooled_std) if pooled_std > 0 else 0.0

    # Verdict
    if abs(cohens_d) < 0.2:
        verdict = "Negligible effect"
    elif ci_low > 0:
        verdict = "Likely positive effect" if cohens_d > 0.5 else "Small positive effect"
    elif ci_high < 0:
        verdict = "Likely negative effect" if cohens_d < -0.5 else "Small negative effect"
    else:
        verdict = "Inconclusive — effect crosses zero"

    return BayesianResult(
        pre_mean=round(pre_mean, 2),
        pre_std=round(pre_std, 2),
        post_mean=round(post_mean, 2),
        post_std=round(post_std, 2),
        n_pre=n_pre,
        n_post=n_post,
        posterior_mean_effect=round(effect, 2),
        credible_interval_95=(round(ci_low, 2), round(ci_high, 2)),
        prob_positive=round(prob_positive, 3),
        cohens_d=round(cohens_d, 2),
        verdict=verdict,
    )


def did_analysis(
    pre_df: pd.DataFrame,
    post_df: pd.DataFrame,
    metric_col: str,
) -> DiDResult:
    """
    Difference-in-Differences analysis.

    Fits a linear trend on the pre-period, extrapolates counterfactual,
    then computes the DiD effect.
    """
    warning = None

    # Pre-period trend
    pre_vals = pre_df[metric_col].values
    pre_x = np.arange(len(pre_vals))

    if len(pre_vals) < 3:
        return DiDResult(
            pre_trend_slope=0.0, pre_trend_r2=0.0,
            counterfactual_post_mean=float(np.mean(pre_vals)) if len(pre_vals) > 0 else 0.0,
            actual_post_mean=float(np.mean(post_df[metric_col].values)),
            did_effect=0.0,
            parallel_trends_valid=False,
            warning="Too few pre-period observations (< 3) for DiD analysis",
        )

    slope, intercept, r_value, _, _ = stats.linregress(pre_x, pre_vals)
    r2 = r_value ** 2

    # Parallel trends validation
    parallel_valid = r2 > 0.3
    if not parallel_valid:
        warning = "Pre-period trend is non-linear — DiD results may be unreliable"

    # Counterfactual: extrapolate pre-period trend into post-period
    n_post = len(post_df)
    post_x = np.arange(len(pre_vals), len(pre_vals) + n_post)
    counterfactual = intercept + slope * post_x
    counterfactual_mean = float(np.mean(counterfactual))

    actual_post_mean = float(np.mean(post_df[metric_col].values))
    did_effect = actual_post_mean - counterfactual_mean

    return DiDResult(
        pre_trend_slope=round(float(slope), 4),
        pre_trend_r2=round(float(r2), 3),
        counterfactual_post_mean=round(counterfactual_mean, 2),
        actual_post_mean=round(actual_post_mean, 2),
        did_effect=round(did_effect, 2),
        parallel_trends_valid=parallel_valid,
        warning=warning,
    )


# Available metrics for experiment analysis (Bayesian intervention)
ANALYSIS_METRICS = {
    "readiness_score": "Readiness Score",
    "sleep_score": "Sleep Score",
    "resting_heart_rate_bpm": "Resting Heart Rate",
    "hrv_ms": "HRV (ms)",
    "combined_wellness_score": "Wellness Score",
    "total_output_kj": "Training Output (kJ)",
    "daily_calories": "Daily Calories",
    "protein_g": "Protein (g)",
    "carbs_g": "Carbs (g)",
    "fat_g": "Fat (g)",
    "fiber_g": "Fiber (g)",
}

# Correlation analysis — input (independent) variables
CORRELATION_INPUT_METRICS = {
    "daily_calories": "Daily Calories",
    "protein_g": "Protein (g)",
    "carbs_g": "Carbs (g)",
    "fat_g": "Fat (g)",
    "fiber_g": "Fiber (g)",
    "protein_pct": "Protein %",
    "carb_pct": "Carb %",
    "fat_pct": "Fat %",
    "total_output_kj": "Training Output (kJ)",
    "steps": "Steps",
    "active_calories": "Active Calories",
    "mindfulness_minutes": "Mindfulness Minutes",
}

# Correlation analysis — outcome (dependent) variables
CORRELATION_OUTCOME_METRICS = {
    "readiness_score": "Readiness Score",
    "sleep_score": "Sleep Score",
    "combined_wellness_score": "Wellness Score",
    "resting_heart_rate_bpm": "Resting Heart Rate",
    "hrv_ms": "HRV (ms)",
    "deep_sleep_score": "Deep Sleep Score",
    "total_output_kj": "Training Output",
}


@dataclass
class CorrelationResult:
    """Result of Pearson correlation analysis between two metrics."""
    pearson_r: float
    p_value: float
    r_squared: float
    n_observations: int
    interpretation: str
    rolling_r_values: list[float]
    rolling_r_dates: list[str]
    scatter_x: list[float]
    scatter_y: list[float]
    scatter_dates: list[str]
    regression_slope: float
    regression_intercept: float


def _interpret_correlation(r: float, p: float) -> str:
    """Generate human-readable interpretation of correlation strength."""
    abs_r = abs(r)
    direction = "positive" if r > 0 else "negative"
    sig = "statistically significant" if p < 0.05 else "not statistically significant"

    if abs_r < 0.1:
        strength = "negligible"
    elif abs_r < 0.3:
        strength = "weak"
    elif abs_r < 0.5:
        strength = "moderate"
    elif abs_r < 0.7:
        strength = "strong"
    else:
        strength = "very strong"

    return f"{strength.capitalize()} {direction} correlation (r={r:.3f}, {sig} at p={p:.4f})"


def correlation_analysis(
    athena,
    input_col: str,
    outcome_col: str,
    start_date: str,
    end_date: str,
    rolling_window: int = 14,
    lag_days: int = 0,
) -> "CorrelationResult | None":
    """
    Compute Pearson correlation between two metrics from the Gold layer.

    If lag_days > 0, shifts the outcome column forward by N days
    (today's input → tomorrow's outcome).
    """
    query = f"""
        SELECT date, {input_col}, {outcome_col}
        FROM bio_gold.daily_readiness_performance
        WHERE {input_col} IS NOT NULL
          AND {outcome_col} IS NOT NULL
          AND COALESCE(
                TRY(CAST(date AS date)),
                TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
              ) BETWEEN DATE '{start_date}' AND DATE '{end_date}'
        ORDER BY date
    """
    df = athena.execute_query(query)

    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df[input_col] = pd.to_numeric(df[input_col], errors="coerce")
    df[outcome_col] = pd.to_numeric(df[outcome_col], errors="coerce")
    df = df.dropna(subset=[input_col, outcome_col])

    if len(df) < 5:
        return None

    # Apply lag: shift outcome backward so today's input aligns with future outcome
    if lag_days > 0:
        df = df.sort_values("date").reset_index(drop=True)
        df[outcome_col] = df[outcome_col].shift(-lag_days)
        df = df.dropna(subset=[outcome_col])

    if len(df) < 5:
        return None

    x = df[input_col].values
    y = df[outcome_col].values

    # Pearson r + p-value
    r, p_val = stats.pearsonr(x, y)

    # OLS regression line
    slope, intercept, _, _, _ = stats.linregress(x, y)

    # Rolling correlation
    df_sorted = df.sort_values("date").reset_index(drop=True)
    rolling_corr = (
        df_sorted[input_col]
        .rolling(window=rolling_window, min_periods=max(rolling_window // 2, 5))
        .corr(df_sorted[outcome_col])
    )
    valid_rolling = rolling_corr.dropna()
    rolling_r_values = valid_rolling.tolist()
    rolling_r_dates = df_sorted.loc[valid_rolling.index, "date"].dt.strftime("%Y-%m-%d").tolist()

    return CorrelationResult(
        pearson_r=round(float(r), 4),
        p_value=round(float(p_val), 6),
        r_squared=round(float(r ** 2), 4),
        n_observations=len(df),
        interpretation=_interpret_correlation(float(r), float(p_val)),
        rolling_r_values=rolling_r_values,
        rolling_r_dates=rolling_r_dates,
        scatter_x=x.tolist(),
        scatter_y=y.tolist(),
        scatter_dates=df_sorted["date"].dt.strftime("%Y-%m-%d").tolist(),
        regression_slope=round(float(slope), 6),
        regression_intercept=round(float(intercept), 4),
    )
