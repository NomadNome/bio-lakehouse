"""Tests for weekly report components."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from jinja2 import Environment, FileSystemLoader

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

TEMPLATES_DIR = PROJECT_ROOT / "insights_engine" / "reports" / "templates"
PROMPTS_DIR = PROJECT_ROOT / "insights_engine" / "prompts"


class TestHTMLTemplate:
    def test_template_exists(self):
        path = TEMPLATES_DIR / "weekly.html"
        assert path.exists()

    def test_template_renders(self):
        env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
        template = env.get_template("weekly.html")
        html = template.render(
            week_start="2026-02-10",
            week_end="2026-02-16",
            generated_at="2026-02-17 08:00",
            key_metrics=[
                {"value": "82", "label": "Avg Readiness", "trend_class": "trend-up"},
                {"value": "85", "label": "Avg Sleep", "trend_class": "trend-up"},
            ],
            narrative_html="<p>Great week overall.</p>",
            insights=[
                {
                    "title": "Sleep Correlation",
                    "narrative": "Moderate positive correlation.",
                    "caveats": ["Small sample size."],
                    "icon": "🛌",
                    "color": "#6366F1",
                },
            ],
            stat_notes="All analyses based on 84 days of data.",
        )
        assert "<!DOCTYPE html>" in html
        assert "2026-02-10" in html
        assert "2026-02-16" in html
        assert "Avg Readiness" in html
        assert "Sleep Correlation" in html
        assert "Small sample size." in html

    def test_template_renders_empty_insights(self):
        env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
        template = env.get_template("weekly.html")
        html = template.render(
            week_start="2026-02-10",
            week_end="2026-02-16",
            generated_at="2026-02-17 08:00",
            key_metrics=[],
            narrative_html="<p>No data this week.</p>",
            insights=[],
            stat_notes="No data available.",
        )
        assert "<!DOCTYPE html>" in html
        assert "No data this week." in html


class TestNarratorPrompt:
    def test_prompt_exists(self):
        path = PROMPTS_DIR / "insight_narrator.txt"
        assert path.exists()
        content = path.read_text()
        assert len(content) > 100
        assert "weekly" in content.lower()


class TestReportGeneratorInit:
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_generator_creates_with_mock(self):
        from insights_engine.reports.weekly_report import WeeklyReportGenerator

        mock_athena = MagicMock()
        gen = WeeklyReportGenerator(mock_athena)
        assert len(gen.analyzers) == 5

    def test_generator_raises_without_api_key(self):
        from insights_engine.reports.weekly_report import WeeklyReportGenerator

        mock_athena = MagicMock()
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
                WeeklyReportGenerator(mock_athena)


class TestDelivery:
    def test_save_local(self, tmp_path):
        from insights_engine.reports.delivery import save_local

        html = "<html><body>Test report</body></html>"
        path = save_local(html, output_dir=tmp_path)
        assert path.exists()
        assert path.read_text() == html
