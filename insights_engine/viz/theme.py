"""
Bio Insights Engine - Plotly Theme (mode-aware)

Chart styling for both dark and light surfaces. All chart builders access
colors attribute-style at render time (``theme.PRIMARY``, ``theme.SERIES_COLORS``),
so ``set_dark_mode()`` re-points the module exports and every subsequent
figure picks up the active mode. Headless renderers (weekly report, kaleido
exports) never call ``set_dark_mode()`` and keep the dark default.

Categorical slots and status colors are defined in config.CHART_CONFIG —
see the note there about CVD validation and status-color reservation.
"""

from __future__ import annotations

import plotly.graph_objects as go

from insights_engine.config import CHART_CONFIG

_PALETTES = {
    "dark": CHART_CONFIG["color_palette"],
    "light": CHART_CONFIG["color_palette_light"],
}
_mode = "dark"


def palette() -> dict:
    """The palette for the currently active mode."""
    return _PALETTES[_mode]


def is_dark() -> bool:
    """Whether the active chart mode is dark."""
    return _mode == "dark"


def set_dark_mode(dark: bool) -> None:
    """Switch the active chart mode (called by app.py from the UI toggle)."""
    global _mode
    _mode = "dark" if dark else "light"
    _refresh_exports()


def bio_layout(**overrides) -> dict:
    """Return a base Plotly layout dict with the Bio theme applied."""
    p = palette()
    layout = dict(
        template="plotly_dark" if _mode == "dark" else "plotly_white",
        paper_bgcolor=p["background"],
        plot_bgcolor=p["background"],
        font=dict(
            family=CHART_CONFIG["font_family"],
            color=p["text"],
            size=13,
        ),
        title_font=dict(size=18, color=p["text"]),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=p["text_muted"]),
        ),
        xaxis=dict(
            gridcolor=p["surface"],
            zerolinecolor=p["surface"],
        ),
        yaxis=dict(
            gridcolor=p["surface"],
            zerolinecolor=p["surface"],
        ),
        height=CHART_CONFIG["chart_height"],
        # b=70 keeps the attribution annotation clear of x-axis tick labels
        margin=dict(l=60, r=30, t=60, b=70),
    )
    layout.update(overrides)
    return layout


def style_figure(fig: go.Figure, n: int = None, **layout_overrides) -> go.Figure:
    """Apply Bio theme to an existing Plotly figure.

    If *n* is provided, adds a data-source attribution annotation.
    """
    fig.update_layout(**bio_layout(**layout_overrides))
    # Data source attribution (PRD requirement)
    attribution = "Data: Oura Ring + Peloton + Apple Health"
    if n is not None:
        attribution += f" | n={n}"
    fig.add_annotation(
        text=attribution,
        xref="paper", yref="paper",
        x=1.0, y=-0.12,
        showarrow=False,
        font=dict(color=palette()["text_muted"], size=9),
        xanchor="right",
    )
    return fig


def _refresh_exports() -> None:
    """Re-point module-level color exports at the active palette."""
    global PRIMARY, SECONDARY, ACCENT, EXTRA1, EXTRA2
    global WARNING, DANGER, SUCCESS, TEXT, TEXT_MUTED, SURFACE, BACKGROUND
    global SERIES_COLORS
    p = palette()
    PRIMARY = p["primary"]
    SECONDARY = p["secondary"]
    ACCENT = p["accent"]
    EXTRA1 = p["extra1"]
    EXTRA2 = p["extra2"]
    WARNING = p["warning"]
    DANGER = p["danger"]
    SUCCESS = p["success"]
    TEXT = p["text"]
    TEXT_MUTED = p["text_muted"]
    SURFACE = p["surface"]
    BACKGROUND = p["background"]
    # Categorical rotation only — status colors are deliberately excluded.
    SERIES_COLORS = [PRIMARY, SECONDARY, ACCENT, EXTRA1, EXTRA2]


_refresh_exports()
