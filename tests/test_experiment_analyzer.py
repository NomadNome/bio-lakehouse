"""Tests for statistically honest intervention analysis labels and estimates."""

import numpy as np
import pandas as pd

from insights_engine.experiments.analyzer import interrupted_time_series_analysis


def test_interrupted_series_reports_post_trend_deviation():
    pre = pd.DataFrame({"metric": np.arange(10, dtype=float)})
    # The projected values would be 10, 11, 12; observed values are +5.
    post = pd.DataFrame({"metric": [15.0, 16.0, 17.0]})

    result = interrupted_time_series_analysis(pre, post, "metric")

    assert result.estimated_deviation == 5.0
    assert result.pretrend_fit_reliable is True
    assert "no untreated control group" in result.warning


def test_interrupted_series_does_not_claim_parallel_trends():
    pre = pd.DataFrame({"metric": [1.0, 4.0, 2.0, 5.0, 1.0]})
    post = pd.DataFrame({"metric": [3.0, 3.0]})

    result = interrupted_time_series_analysis(pre, post, "metric")

    assert result.pretrend_fit_reliable is False
    assert "linear pre-trend fit is weak" in result.warning
