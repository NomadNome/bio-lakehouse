"""
Bio Insights Engine - Insight Analyzer Base

Abstract base class and shared dataclass for all signature insights.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date

import pandas as pd
import plotly.graph_objects as go

from insights_engine.core.athena_client import AthenaClient


@dataclass
class DateRange:
    start: date
    end: date


@dataclass
class InsightResult:
    insight_type: str
    title: str
    narrative: str
    statistics: dict = field(default_factory=dict)
    chart: go.Figure | None = None
    caveats: list[str] = field(default_factory=list)
    data: pd.DataFrame = field(default_factory=pd.DataFrame)


class InsightAnalyzer(ABC):
    """Base class for all signature insights."""

    def __init__(self, athena: AthenaClient):
        self.athena = athena

    @abstractmethod
    def analyze(self, date_range: DateRange | None = None) -> InsightResult:
        ...

    @abstractmethod
    def visualize(self, result: InsightResult) -> go.Figure:
        ...

    @abstractmethod
    def narrate(self, result: InsightResult) -> str:
        ...
