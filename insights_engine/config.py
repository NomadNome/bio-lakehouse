"""
Bio Insights Engine - Configuration
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (Bio Lakehouse/)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# AWS Configuration
AWS_CONFIG = {
    "athena_database": os.environ.get("BIO_ATHENA_DATABASE", "bio_gold"),
    "athena_results_bucket": os.environ.get("BIO_ATHENA_RESULTS_BUCKET", ""),
    "gold_bucket": os.environ.get("BIO_S3_GOLD_BUCKET", ""),
    "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
}

# Claude API Configuration
CLAUDE_CONFIG = {
    "api_key_env": "ANTHROPIC_API_KEY",
    "nl_to_sql_model": "claude-sonnet-4-6",
    "insight_narrator_model": "claude-sonnet-4-6",
    "max_tokens": 2048,
    "temperature": 0.0,  # Deterministic SQL generation
}

# Chart Styling (from PRD)
CHART_CONFIG = {
    "color_palette": {
        "primary": "#6366F1",       # Indigo
        "secondary": "#EC4899",     # Pink
        "accent": "#14B8A6",        # Teal
        "warning": "#F59E0B",       # Amber
        "danger": "#EF4444",        # Red
        "success": "#22C55E",       # Green
        "background": "#1E1E2E",    # Dark
        "surface": "#2D2D3F",       # Dark surface
        "text": "#E2E8F0",          # Light gray
        "text_muted": "#94A3B8",    # Muted gray
    },
    "font_family": "Inter, system-ui, sans-serif",
    "chart_height": 400,
    "export_width": 1200,
    "export_height": 600,
    "export_scale": 2,  # Retina
}

# Energy state thresholds (from optimization views)
ENERGY_THRESHOLDS = {
    "peak": {"readiness": 85, "sleep": 88, "hrv": 75},
    "high": {"readiness": 85, "sleep": 80},
    "moderate": {"readiness": 70, "sleep": 65},
    "low": {"readiness": 50},
}

# Readiness-to-output ratio zones
RATIO_ZONES = {
    "overreaching": {"min": 4.0, "label": "Overreaching", "action": "Consider rest"},
    "high_performance": {"min": 2.5, "max": 4.0, "label": "High Performance"},
    "moderate": {"min": 1.5, "max": 2.5, "label": "Moderate"},
    "undertrained": {"max": 1.5, "label": "Undertrained / Recovery Day"},
}

# Default intensity-to-output mapping (overridden at runtime with actual percentiles)
INTENSITY_OUTPUT_DEFAULTS = {
    "none": 0,
    "low": 150,
    "moderate": 300,
    "high": 500,
}

# Readiness zone bands for multi-day projection chart backgrounds
READINESS_ZONE_BANDS = [
    {"label": "Peak",     "y0": 85, "y1": 100, "color": "rgba(34,197,94,0.08)"},
    {"label": "High",     "y0": 75, "y1": 85,  "color": "rgba(20,184,166,0.08)"},
    {"label": "Moderate", "y0": 60, "y1": 75,  "color": "rgba(99,102,241,0.08)"},
    {"label": "Low",      "y0": 45, "y1": 60,  "color": "rgba(245,158,11,0.08)"},
    {"label": "Recovery", "y0": 0,  "y1": 45,  "color": "rgba(239,68,68,0.08)"},
]

# Estimated TSS by workout type and intensity for multi-day planning
WORKOUT_TSS_ESTIMATES = {
    "cycling":              {"low": 35,  "moderate": 65,  "high": 110},
    "strength":             {"low": 25,  "moderate": 50,  "high": 80},
    "cycling_and_strength": {"low": 50,  "moderate": 90,  "high": 150},
    "rest":                 {"low": 0,   "moderate": 0,   "high": 0},
}

# Body composition goal (set via env to keep personal targets out of code)
BODY_FAT_GOAL_PCT = float(os.environ.get("BIO_BF_GOAL_PCT", "0"))

OVERLOAD_THRESHOLDS = {
    "min_weeks": 4,
    "progression_pct": 0.02,           # >2% weekly output growth = Progressing
    "regression_pct": -0.05,           # >5% decline = Regressing
    "watts_change_threshold": 2,       # >2W = meaningful
    "output_per_min_threshold": 0.1,   # >0.1 kJ/min = meaningful
    "hr_efficiency_threshold": 0.02,   # >0.02 W/bpm = meaningful
    "duration_bucket_size": 5,         # Round class length to nearest 5 min
}

# Correlation Discovery Engine configuration
DISCOVERY_CONFIG = {
    "default_lookback_days": 180,
    "default_min_rho": 0.25,
    "default_alpha": 0.05,
    "max_lags": 4,                     # Lags 0, 1, 2, 3
    "min_samples_correlation": 20,
    "min_samples_threshold": 10,
    "max_results_correlations": 50,
    "max_results_thresholds": 20,
    "s3_prefix": "discoveries/weekly",
}

# Athena views available in bio_gold
GOLD_VIEWS = [
    "daily_readiness_performance",
    "dashboard_30day",
    "workout_recommendations",
    "energy_state",
    "workout_type_optimization",
    "sleep_performance_prediction",
    "readiness_performance_correlation",
    "weekly_trends",
    "overtraining_risk",
    "training_load_daily",
]
