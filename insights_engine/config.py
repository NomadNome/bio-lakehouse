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
    "athena_results_bucket": os.environ.get(
        "BIO_ATHENA_RESULTS_BUCKET",
        "bio-lakehouse-athena-results-000000000000",
    ),
    "gold_bucket": os.environ.get(
        "BIO_S3_GOLD_BUCKET", "bio-lakehouse-gold-000000000000"
    ),
    "aws_region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
}

# Claude API Configuration
CLAUDE_CONFIG = {
    "api_key_env": "ANTHROPIC_API_KEY",
    "nl_to_sql_model": "claude-sonnet-4-20250514",
    "insight_narrator_model": "claude-sonnet-4-20250514",
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
