"""
Bio Insights Engine - Plotly Dark Theme

Portfolio-quality chart styling from the PRD color palette.
"""

from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

from insights_engine.config import CHART_CONFIG

_palette = CHART_CONFIG["color_palette"]


def bio_layout(**overrides) -> dict:
    """Return a base Plotly layout dict with the Bio theme applied."""
    layout = dict(
        template="plotly_dark",
        paper_bgcolor=_palette["background"],
        plot_bgcolor=_palette["background"],
        font=dict(
            family=CHART_CONFIG["font_family"],
            color=_palette["text"],
            size=13,
        ),
        title_font=dict(size=18, color=_palette["text"]),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=_palette["text_muted"]),
        ),
        xaxis=dict(
            gridcolor=_palette["surface"],
            zerolinecolor=_palette["surface"],
        ),
        yaxis=dict(
            gridcolor=_palette["surface"],
            zerolinecolor=_palette["surface"],
        ),
        height=CHART_CONFIG["chart_height"],
        margin=dict(l=60, r=30, t=60, b=50),
    )
    layout.update(overrides)
    return layout


def style_figure(fig: go.Figure, n: int = None, **layout_overrides) -> go.Figure:
    """Apply Bio theme to an existing Plotly figure.

    If *n* is provided, adds a data-source attribution annotation.
    """
    fig.update_layout(**bio_layout(**layout_overrides))
    # Data source attribution (PRD requirement)
    attribution = "Data: Oura Ring + Peloton"
    if n is not None:
        attribution += f" | n={n}"
    fig.add_annotation(
        text=attribution,
        xref="paper", yref="paper",
        x=1.0, y=-0.12,
        showarrow=False,
        font=dict(color=_palette["text_muted"], size=9),
        xanchor="right",
    )
    return fig


# Color accessors for chart builders
PRIMARY = _palette["primary"]
SECONDARY = _palette["secondary"]
ACCENT = _palette["accent"]
WARNING = _palette["warning"]
DANGER = _palette["danger"]
SUCCESS = _palette["success"]
TEXT = _palette["text"]
TEXT_MUTED = _palette["text_muted"]
SURFACE = _palette["surface"]

SERIES_COLORS = [PRIMARY, SECONDARY, ACCENT, WARNING, DANGER, SUCCESS]
