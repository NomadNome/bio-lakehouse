"""
Feature Selection Module for Readiness Predictor

Loads all columns from the feature table, excludes leaky features,
and selects top features via mutual information + correlation filtering.

Usage:
    python -m models.readiness_predictor.feature_selection
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_regression

# Features that leak same-day or target information
LEAKY_FEATURES = [
    "readiness_score",               # same-day target proxy
    "next_day_readiness",            # literal target column
    "combined_wellness_score",       # derived from readiness
]

# Non-feature columns (identifiers, target)
NON_FEATURE_COLS = ["date", "next_day_readiness"]

TARGET_COL = "next_day_readiness"


def load_feature_data() -> pd.DataFrame:
    """Load feature data from Athena."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from insights_engine.core.athena_client import AthenaClient

    athena = AthenaClient()
    df = athena.execute_query("""
        SELECT *
        FROM bio_gold.feature_readiness_daily
        ORDER BY date
    """)
    return df


def get_candidate_features(df: pd.DataFrame) -> list[str]:
    """Return all numeric columns excluding leaky and non-feature columns."""
    exclude = set(LEAKY_FEATURES + NON_FEATURE_COLS)
    candidates = []
    for col in df.columns:
        if col in exclude:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().sum() > 10:
            candidates.append(col)
    return candidates


def mutual_information_scores(
    df: pd.DataFrame, features: list[str], target: str = TARGET_COL
) -> dict[str, float]:
    """Compute mutual information between each feature and the target."""
    df_clean = df[features + [target]].copy()
    for col in features + [target]:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")
    df_clean = df_clean.dropna()

    if len(df_clean) < 20:
        print(f"WARNING: Only {len(df_clean)} complete rows for MI scoring.")

    X = df_clean[features].values
    y = df_clean[target].values

    mi_scores = mutual_info_regression(X, y, random_state=42)
    return dict(zip(features, mi_scores))


def correlation_filter(
    df: pd.DataFrame, features: list[str], mi_scores: dict[str, float],
    threshold: float = 0.85,
) -> list[str]:
    """Drop features with pairwise |r| > threshold, keeping the one with higher MI."""
    df_numeric = df[features].copy()
    for col in features:
        df_numeric[col] = pd.to_numeric(df_numeric[col], errors="coerce")

    corr_matrix = df_numeric.corr().abs()
    to_drop = set()

    for i, feat_i in enumerate(features):
        if feat_i in to_drop:
            continue
        for j, feat_j in enumerate(features):
            if j <= i or feat_j in to_drop:
                continue
            if corr_matrix.loc[feat_i, feat_j] > threshold:
                # Drop the one with lower MI
                if mi_scores.get(feat_i, 0) >= mi_scores.get(feat_j, 0):
                    to_drop.add(feat_j)
                else:
                    to_drop.add(feat_i)

    return [f for f in features if f not in to_drop]


def select_features(
    df: pd.DataFrame | None = None,
    top_k: int = 8,
    corr_threshold: float = 0.85,
) -> tuple[list[str], dict]:
    """
    Full feature selection pipeline.

    Returns:
        (selected_features, metadata_dict)
    """
    if df is None:
        df = load_feature_data()

    # Get candidate features
    candidates = get_candidate_features(df)
    print(f"Candidate features ({len(candidates)}): {candidates}")

    # Ensure numeric
    for col in candidates + [TARGET_COL]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df_valid = df.dropna(subset=[TARGET_COL])

    # Mutual information scoring
    mi_scores = mutual_information_scores(df_valid, candidates)
    sorted_mi = sorted(mi_scores.items(), key=lambda x: x[1], reverse=True)

    print("\nMutual Information Scores:")
    for feat, score in sorted_mi:
        print(f"  {feat:30s} {score:.4f}")

    # Correlation filtering
    filtered = correlation_filter(df_valid, candidates, mi_scores, corr_threshold)

    # Rank by MI and take top_k
    filtered_ranked = sorted(filtered, key=lambda f: mi_scores.get(f, 0), reverse=True)
    selected = filtered_ranked[:top_k]

    print(f"\nSelected features ({len(selected)}):")
    for feat in selected:
        print(f"  {feat:30s} MI={mi_scores[feat]:.4f}")

    metadata = {
        "all_candidates": candidates,
        "mi_scores": {k: round(v, 4) for k, v in mi_scores.items()},
        "corr_filtered_out": [f for f in candidates if f not in filtered],
        "selected_features": selected,
        "leaky_excluded": LEAKY_FEATURES,
    }

    return selected, metadata


if __name__ == "__main__":
    features, meta = select_features()
    print(f"\nFinal feature set: {features}")
