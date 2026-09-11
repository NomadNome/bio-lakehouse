"""Tests for weekly report components."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
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
        assert len(gen.analyzers) == 10

    def test_generator_raises_without_api_key(self):
        from insights_engine.reports.weekly_report import WeeklyReportGenerator

        mock_athena = MagicMock()
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
                WeeklyReportGenerator(mock_athena)


class TestReportGenerationSafety:
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_key_metrics_tolerate_null_athena_aggregates(self):
        from insights_engine.reports.weekly_report import WeeklyReportGenerator

        mock_athena = MagicMock()
        mock_athena.execute_query.return_value = pd.DataFrame([{
            "avg_readiness": 80.0,
            "avg_sleep": 79.0,
            "workout_days": None,
            "total_output": None,
            "data_days": None,
            "avg_mindfulness": None,
            "mindfulness_days": None,
            "avg_calories": None,
            "nutrition_days": None,
        }])
        gen = WeeklyReportGenerator(mock_athena)

        metrics = gen._get_key_metrics(
            pd.Timestamp("2026-09-01").date(),
            pd.Timestamp("2026-09-07").date(),
        )

        by_label = {metric["label"]: metric["value"] for metric in metrics}
        assert by_label["Workout Days"] == "0"
        assert by_label["Total Output (kJ)"] == "0"
        assert by_label["Data Days"] == "0/7"

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_narrator_disables_thinking_and_returns_text_block(self):
        from insights_engine.reports.weekly_report import WeeklyReportGenerator

        gen = WeeklyReportGenerator(MagicMock())
        gen._client = MagicMock()
        gen._client.messages.create.return_value = SimpleNamespace(content=[
            SimpleNamespace(type="thinking", thinking="internal"),
            SimpleNamespace(type="text", text="  A useful weekly summary.  "),
        ])

        narrative = gen._generate_narrative([], pd.Timestamp("2026-09-01").date(), pd.Timestamp("2026-09-07").date())

        assert narrative == "A useful weekly summary."
        assert gen._client.messages.create.call_args.kwargs["thinking"] == {"type": "disabled"}

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_narrator_retries_one_empty_response(self):
        from insights_engine.reports.weekly_report import WeeklyReportGenerator

        gen = WeeklyReportGenerator(MagicMock())
        gen._client = MagicMock()
        gen._client.messages.create.side_effect = [
            SimpleNamespace(content=[SimpleNamespace(type="thinking", thinking="internal")]),
            SimpleNamespace(content=[SimpleNamespace(type="text", text="Recovered summary")]),
        ]

        narrative = gen._generate_narrative([], pd.Timestamp("2026-09-01").date(), pd.Timestamp("2026-09-07").date())

        assert narrative == "Recovered summary"
        assert gen._client.messages.create.call_count == 2


class TestDelivery:
    def test_save_local(self, tmp_path):
        from insights_engine.reports.delivery import save_local

        html = "<html><body>Test report</body></html>"
        path = save_local(html, output_dir=tmp_path)
        assert path.exists()
        assert path.read_text() == html
