"""
Bio Insights Engine - Weekly Report Generator

Orchestrates all 5 insight analyzers, generates a narrative via Claude,
and renders an HTML report.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import anthropic
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from insights_engine.config import CLAUDE_CONFIG
from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.base import DateRange, InsightResult
from insights_engine.insights.sleep_readiness import SleepReadinessAnalyzer
from insights_engine.insights.workout_recovery import WorkoutRecoveryAnalyzer
from insights_engine.insights.readiness_trend import ReadinessTrendAnalyzer
from insights_engine.insights.anomaly_detection import AnomalyDetectionAnalyzer
from insights_engine.insights.timing_correlation import TimingCorrelationAnalyzer

TEMPLATES_DIR = Path(__file__).parent / "templates"
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

INSIGHT_ICONS = {
    "sleep_readiness": "🛌",
    "workout_recovery": "💪",
    "readiness_trend": "📈",
    "anomaly_detection": "⚠️",
    "timing_correlation": "⏱️",
}

INSIGHT_COLORS = {
    "sleep_readiness": "#6366F1",
    "workout_recovery": "#EC4899",
    "readiness_trend": "#14B8A6",
    "anomaly_detection": "#F59E0B",
    "timing_correlation": "#22C55E",
}


@dataclass
class ReportResult:
    html: str
    insights: list[InsightResult]
    narrative: str
    metadata: dict = field(default_factory=dict)


class WeeklyReportGenerator:
    """Runs all insight analyzers and compiles a weekly HTML report."""

    def __init__(self, athena: AthenaClient, model: str = None):
        self.athena = athena
        self.model = model or CLAUDE_CONFIG["insight_narrator_model"]
        api_key = os.environ.get(CLAUDE_CONFIG["api_key_env"])
        if not api_key:
            raise ValueError(f"Set {CLAUDE_CONFIG['api_key_env']} environment variable")
        self.client = anthropic.Anthropic(api_key=api_key)

        self.analyzers = [
            SleepReadinessAnalyzer(athena),
            WorkoutRecoveryAnalyzer(athena),
            ReadinessTrendAnalyzer(athena),
            AnomalyDetectionAnalyzer(athena),
            TimingCorrelationAnalyzer(athena),
        ]

    def generate(self, week_ending: date = None) -> ReportResult:
        """Run all analyzers, generate narrative, render HTML."""
        start_time = time.time()

        if week_ending is None:
            week_ending = date.today()
        week_start = week_ending - timedelta(days=6)
        date_range = DateRange(start=week_start, end=week_ending)

        # Run all analyzers (use full date range for context, not just this week)
        print("Running insight analyzers...")
        insights = []
        for analyzer in self.analyzers:
            name = analyzer.__class__.__name__
            print(f"  {name}...")
            try:
                result = analyzer.analyze()
                insights.append(result)
            except Exception as e:
                print(f"  WARNING: {name} failed: {e}")
                insights.append(InsightResult(
                    insight_type=getattr(analyzer, "insight_type", "unknown"),
                    title=name,
                    narrative=f"Analysis unavailable: {e}",
                ))

        # Get key metrics for this week
        key_metrics = self._get_key_metrics(week_start, week_ending)

        # Generate narrative via Claude
        print("Generating narrative via Claude...")
        narrative = self._generate_narrative(insights, week_start, week_ending)

        # Render HTML
        print("Rendering HTML...")
        html = self._render_html(
            insights=insights,
            narrative=narrative,
            key_metrics=key_metrics,
            week_start=week_start,
            week_ending=week_ending,
        )

        elapsed = time.time() - start_time
        print(f"Report generated in {elapsed:.1f}s")

        return ReportResult(
            html=html,
            insights=insights,
            narrative=narrative,
            metadata={
                "week_start": str(week_start),
                "week_end": str(week_ending),
                "generation_time_sec": round(elapsed, 1),
                "insight_count": len(insights),
                "generated_at": datetime.now().isoformat(),
            },
        )

    def _get_key_metrics(self, week_start: date, week_end: date) -> list[dict]:
        """Query key summary metrics for the week."""
        sql = f"""
        SELECT
            ROUND(AVG(readiness_score), 1) AS avg_readiness,
            ROUND(AVG(sleep_score), 1) AS avg_sleep,
            SUM(CASE WHEN had_workout = true THEN 1 ELSE 0 END) AS workout_days,
            ROUND(SUM(total_output_kj), 0) AS total_output,
            COUNT(*) AS data_days
        FROM bio_gold.dashboard_30day
        WHERE COALESCE(
                TRY(CAST(date AS date)),
                TRY(date_parse(date, '%Y-%m-%d %H:%i:%s'))
              ) BETWEEN DATE '{week_start}' AND DATE '{week_end}'
        """
        try:
            df = self.athena.execute_query(sql)
            if df.empty:
                return []

            row = df.iloc[0]
            avg_r = row.get("avg_readiness")
            avg_s = row.get("avg_sleep")

            def trend_class(val, good_threshold=82, bad_threshold=70):
                if val is None or pd.isna(val):
                    return ""
                val = float(val)
                if val >= good_threshold:
                    return "trend-up"
                elif val <= bad_threshold:
                    return "trend-down"
                return "trend-stable"

            return [
                {"value": f"{avg_r:.0f}" if pd.notna(avg_r) else "—", "label": "Avg Readiness", "trend_class": trend_class(avg_r)},
                {"value": f"{avg_s:.0f}" if pd.notna(avg_s) else "—", "label": "Avg Sleep Score", "trend_class": trend_class(avg_s, 85, 70)},
                {"value": f"{int(row.get('workout_days', 0))}", "label": "Workout Days", "trend_class": ""},
                {"value": f"{int(row.get('total_output', 0)):,}", "label": "Total Output (kJ)", "trend_class": ""},
                {"value": f"{int(row.get('data_days', 0))}/7", "label": "Data Days", "trend_class": ""},
            ]
        except Exception as e:
            print(f"  WARNING: Could not get key metrics: {e}")
            return []

    def _generate_narrative(
        self, insights: list[InsightResult], week_start: date, week_end: date
    ) -> str:
        """Use Claude to generate a cohesive weekly narrative."""
        system_prompt = (PROMPTS_DIR / "insight_narrator.txt").read_text()

        # Build context from insight results
        insight_summaries = []
        for r in insights:
            summary = f"### {r.title}\n"
            summary += f"Narrative: {r.narrative}\n"
            if r.statistics:
                summary += f"Statistics: {r.statistics}\n"
            if r.caveats:
                summary += f"Caveats: {'; '.join(r.caveats)}\n"
            insight_summaries.append(summary)

        user_prompt = f"""Generate the weekly bio-optimization report for {week_start} to {week_end}.

Here are the analysis results from each insight module:

{"".join(insight_summaries)}

Write the full report narrative following the structure in your instructions."""

        response = self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            temperature=0.3,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return response.content[0].text.strip()

    def _render_html(
        self,
        insights: list[InsightResult],
        narrative: str,
        key_metrics: list[dict],
        week_start: date,
        week_ending: date,
    ) -> str:
        """Render the Jinja2 HTML template."""
        env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
        template = env.get_template("weekly.html")

        # Convert narrative markdown to basic HTML paragraphs
        narrative_html = ""
        for line in narrative.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("## ") or line.startswith("### "):
                narrative_html += f"<p><strong>{line.lstrip('#').strip()}</strong></p>\n"
            elif line.startswith("- ") or line.startswith("* "):
                narrative_html += f"<p>→ {line[2:]}</p>\n"
            else:
                narrative_html += f"<p>{line}</p>\n"

        # Build insight cards
        insight_cards = []
        for r in insights:
            insight_cards.append({
                "title": r.title,
                "narrative": r.narrative,
                "caveats": r.caveats,
                "icon": INSIGHT_ICONS.get(r.insight_type, "📊"),
                "color": INSIGHT_COLORS.get(r.insight_type, "#6366F1"),
            })

        # Statistical notes
        all_caveats = []
        for r in insights:
            all_caveats.extend(r.caveats)
        stat_notes = " ".join(all_caveats) if all_caveats else "All analyses based on available Oura + Peloton data."

        html = template.render(
            week_start=str(week_start),
            week_end=str(week_ending),
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            key_metrics=key_metrics,
            narrative_html=narrative_html,
            insights=insight_cards,
            stat_notes=stat_notes,
        )
        return html
