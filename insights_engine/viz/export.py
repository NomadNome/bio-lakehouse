"""
Bio Insights Engine - Chart Export

Static PNG export for weekly reports.
"""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

from insights_engine.config import CHART_CONFIG


def export_png(fig: go.Figure, path: str | Path) -> Path:
    """Export a Plotly figure to a static PNG (retina-quality)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(
        str(path),
        width=CHART_CONFIG["export_width"],
        height=CHART_CONFIG["export_height"],
        scale=CHART_CONFIG["export_scale"],
    )
    return path
