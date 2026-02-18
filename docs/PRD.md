# PRD: Bio-Optimization Insights Engine

**Version:** 1.0
**Author:** Chris Jenkins
**Date:** 2026-02-17
**Status:** Draft
**Repository:** `NomadNome/bio-lakehouse` (extends existing)

---

## Executive Summary

The Bio-Optimization Insights Engine transforms an existing AWS bio-lakehouse (S3 → Glue → Athena) from a passive data store into an active intelligence product. Users ask natural language health questions ("How does my sleep affect next-day readiness?"), get SQL-backed answers with visualizations, and receive automated weekly insight reports. This project demonstrates the full arc from data engineering to AI-powered analytics — a portfolio centerpiece for data/AI platform roles at Anthropic, OpenAI, and Databricks.

---

## Problem Statement

Chris has ~3 months of biometric data (Oura Ring + Peloton) flowing through a well-architected lakehouse with bronze/silver/gold layers. The gold layer exposes `dashboard_30day` and `workout_recommendations` views via Athena. But:

1. **Querying requires SQL expertise** — no conversational interface exists
2. **Insights are manual** — Chris must write queries and interpret results himself
3. **No trend detection or anomaly alerting** — patterns go unnoticed
4. **No visualization layer** — raw query results, not portfolio-quality charts
5. **The lakehouse story stops at "I built a data pipeline"** — it doesn't reach "I built an intelligence product"

---

## Target User

**Primary:** Chris Jenkins (solo user, ~90 days of personal biometric data)
**Secondary:** Portfolio reviewers — hiring managers at Anthropic, OpenAI, Databricks evaluating technical depth, AI integration skill, and end-to-end product thinking

---

## Detailed Feature Specifications

### Feature 1: Natural Language Query Interface (NL-to-SQL)

**Description:** A Streamlit chat interface where the user types health questions in plain English. Claude API translates to Athena SQL, executes the query, and returns a formatted answer with optional visualization.

**Acceptance Criteria:**
- [ ] User types a question → system returns a natural language answer within 15 seconds
- [ ] Generated SQL is displayed in a collapsible "Show SQL" panel
- [ ] 7 out of 10 benchmark questions produce correct, executable SQL on first attempt
- [ ] System handles ambiguous queries gracefully (asks clarifying questions or states assumptions)
- [ ] Query history persists within a session
- [ ] Error states (invalid SQL, empty results, Athena timeout) display user-friendly messages

**Benchmark Questions (test suite):**
1. "What was my average readiness score last week?"
2. "Show my sleep duration trend over the past 30 days"
3. "Which workout type gives me the best next-day readiness?"
4. "What's my average HRV on days after cycling vs strength training?"
5. "How many workouts did I do in January?"
6. "What's the correlation between my sleep score and readiness?"
7. "Show me days where my readiness dropped below 70"
8. "What's my average Peloton output for cycling workouts?"
9. "Compare my weekday vs weekend sleep duration"
10. "What was my best readiness week and what did I do differently?"

**Implementation Details:**

```python
# Core function signature
async def nl_to_sql(
    question: str,
    schema_context: str,       # Athena table/view DDLs
    conversation_history: list, # Prior Q&A for context
    model: str = "claude-sonnet-4-20250514"
) -> NLToSQLResult:
    """
    Returns:
        NLToSQLResult(
            sql: str,
            explanation: str,
            assumptions: list[str],
            confidence: float  # 0-1 self-assessed
        )
    """
```

**Prompt Engineering Strategy:**
- System prompt includes full Athena schema DDL for `bio_gold` database
- Include 5-8 few-shot examples of question → SQL pairs
- Instruct Claude to state assumptions explicitly
- Instruct Claude to prefer existing gold views over raw silver tables
- Include statistical caveats prompt: "With ~90 data points, note sample size limitations"

**Schema Context (injected into system prompt):**
```sql
-- bio_gold.dashboard_30day: 30-day rolling view
-- Columns: date, readiness_score, sleep_score, sleep_duration_hrs,
--          hrv_avg, resting_hr, steps, activity_score,
--          workout_count, workout_type, peloton_output,
--          peloton_avg_hr, peloton_duration_min

-- bio_gold.workout_recommendations: derived recommendations
-- Columns: date, recommended_intensity, reasoning,
--          recent_readiness_avg, recent_sleep_avg, workout_streak
```

---

### Feature 2: Automated Weekly Insight Report

**Description:** Every Monday at 7:00 AM EST, the system queries the gold layer, runs the 5 signature analyses, generates a narrative report via Claude API, and delivers it.

**Acceptance Criteria:**
- [ ] Report generates automatically every Monday at 7:00 AM EST
- [ ] Report contains all 5 signature insights (see Feature 3)
- [ ] At least 1 insight per week is "non-obvious" (not just restating averages)
- [ ] Report includes inline Plotly charts rendered as static images
- [ ] Report is saved as HTML to S3 gold bucket and optionally emailed
- [ ] Generation completes in under 60 seconds
- [ ] If data is missing or stale (>3 days), report flags it

**Report Structure:**
```
1. Week in Review (date range, data completeness)
2. Key Metrics Summary (readiness avg, sleep avg, workout count, HRV trend)
3. Signature Insight #1: Sleep → Readiness Correlation (this week)
4. Signature Insight #2: Workout Type → Recovery Analysis
5. Signature Insight #3: Weekly Readiness Trend (rolling average chart)
6. Signature Insight #4: Anomaly Detection (flags)
7. Signature Insight #5: Supplement/Timing Correlation
8. Recommendations for Next Week
9. Statistical Notes (sample sizes, confidence intervals, caveats)
```

**Delivery Mechanism:**
```python
# Cron entry (macOS launchd or crontab)
# 0 7 * * 1 /usr/local/bin/python3 /path/to/weekly_report.py

def generate_weekly_report(
    week_start: date,
    week_end: date,
    output_format: str = "html"  # "html" | "markdown"
) -> ReportResult:
    """
    Returns:
        ReportResult(
            html: str,
            s3_url: str,
            insights: list[Insight],
            generation_time_sec: float
        )
    """
```

---

### Feature 3: Five Signature Insights

Each insight is a reusable analysis module with a standard interface.

#### 3a. Sleep → Readiness Correlation

**Description:** Pearson correlation between sleep metrics (duration, score, deep sleep %) and next-day readiness score.

**Acceptance Criteria:**
- [ ] Displays correlation coefficient (r), p-value, sample size (n)
- [ ] Scatter plot with regression line (Plotly)
- [ ] Narrative interpretation: "Your sleep duration has a [strong/moderate/weak] positive correlation (r=X, p=Y, n=Z) with next-day readiness"
- [ ] Explicitly states if n < 30: "Note: With only N data points, this correlation should be interpreted cautiously"

**SQL:**
```sql
SELECT
    a.date AS sleep_date,
    a.sleep_duration_hrs,
    a.sleep_score,
    b.readiness_score AS next_day_readiness
FROM bio_gold.dashboard_30day a
JOIN bio_gold.dashboard_30day b
    ON b.date = date_add('day', 1, a.date)
WHERE a.sleep_duration_hrs IS NOT NULL
    AND b.readiness_score IS NOT NULL
ORDER BY a.date;
```

#### 3b. Workout Type → Recovery Analysis

**Description:** Compare next-day readiness scores segmented by workout type (cycling vs strength vs rest day).

**Acceptance Criteria:**
- [ ] Box plot or grouped bar chart showing readiness distribution by prior-day workout type
- [ ] Reports mean, median, std dev, and count per group
- [ ] Mann-Whitney U test (non-parametric, appropriate for small samples) between groups
- [ ] Narrative: "After cycling workouts, your average next-day readiness is X vs Y after strength training (p=Z)"

#### 3c. Weekly Readiness Trends with Rolling Averages

**Description:** Time series of daily readiness with 7-day and 14-day rolling averages overlaid.

**Acceptance Criteria:**
- [ ] Line chart: daily readiness (dots), 7-day MA (solid line), 14-day MA (dashed line)
- [ ] Trend direction indicator (↑ improving, → stable, ↓ declining) based on slope of 14-day MA
- [ ] Highlights weeks that were notably above or below personal baseline

#### 3d. Anomaly Detection

**Description:** Flag days where readiness drops >1.5 standard deviations below personal mean, or where expected workouts were missed.

**Acceptance Criteria:**
- [ ] Anomalies displayed as highlighted points on the trend chart
- [ ] Each anomaly includes context: "Readiness dropped to X on [date]. Prior night sleep was Y hours (vs your avg of Z)"
- [ ] Missed workout detection: identifies days with no Peloton data that break a workout streak of 3+ days
- [ ] Summary count: "X anomalies detected in the past 30 days"

#### 3e. Supplement/Timing Correlation

**Description:** Analyze whether workout timing (morning vs evening, inferred from Peloton timestamps) correlates with readiness or recovery metrics.

**Acceptance Criteria:**
- [ ] Segments workouts by time-of-day bucket (morning: before noon, afternoon/evening: after noon)
- [ ] Compares next-day readiness between buckets
- [ ] If no timestamp data available, report states "Insufficient timing data — skipping this analysis"
- [ ] Extendable: designed so supplement data can be added as a future bronze source

---

### Feature 4: Visualization Layer

**Description:** Portfolio-quality interactive charts using Plotly, rendered in Streamlit and as static images for reports.

**Acceptance Criteria:**
- [ ] Consistent color scheme across all charts (defined in `config.py`)
- [ ] All charts have proper titles, axis labels, and legends
- [ ] Charts are responsive in Streamlit
- [ ] Static PNG export for weekly reports (Plotly `write_image`)
- [ ] Dark mode support (toggle in Streamlit sidebar)
- [ ] Charts include data source attribution: "Data: Oura Ring + Peloton | n=X"

**Chart Inventory:**
| Chart | Type | Feature |
|-------|------|---------|
| Sleep vs Readiness Scatter | Scatter + regression | 3a |
| Recovery by Workout Type | Box plot | 3b |
| Readiness Trend | Line + rolling avg | 3c |
| Anomaly Timeline | Line + highlighted points | 3d |
| Workout Timing Analysis | Grouped bar | 3e |
| Weekly Summary Dashboard | Multi-panel | Report |
| NL Query Result | Dynamic (bar/line/table) | Feature 1 |

**Styling Config:**
```python
CHART_CONFIG = {
    "color_palette": {
        "primary": "#6366F1",      # Indigo
        "secondary": "#EC4899",    # Pink
        "accent": "#14B8A6",       # Teal
        "warning": "#F59E0B",      # Amber
        "background": "#1E1E2E",   # Dark
        "text": "#E2E8F0",         # Light gray
    },
    "font_family": "Inter, system-ui, sans-serif",
    "chart_height": 400,
    "export_width": 1200,
    "export_height": 600,
    "export_scale": 2,  # Retina
}
```

---

### Feature 5: Infrastructure Integration

**Description:** Connects to the existing bio-lakehouse — no new AWS services, databases, or infrastructure required.

**Acceptance Criteria:**
- [ ] Reads from existing Athena `bio_gold` and `bio_silver` databases
- [ ] Uses existing S3 buckets for report storage
- [ ] AWS credentials via existing CLI profile (no new IAM roles for MVP)
- [ ] Athena query results use existing results bucket: `bio-lakehouse-athena-results-<AWS_ACCOUNT_ID>`
- [ ] No modifications to existing Glue jobs or bronze/silver/gold pipelines

---

## Technical Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     PRESENTATION LAYER                       │
│                                                              │
│  ┌──────────────────┐    ┌─────────────────────────────┐    │
│  │   Streamlit UI    │    │   Weekly Report (HTML/PNG)   │    │
│  │  - Chat interface │    │   - Cron: Mon 7am EST        │    │
│  │  - Viz dashboard  │    │   - Saved to S3 gold         │    │
│  │  - Query history  │    │                              │    │
│  └────────┬─────────┘    └──────────────┬──────────────┘    │
│           │                              │                    │
├───────────┼──────────────────────────────┼────────────────────┤
│           │       INTELLIGENCE LAYER     │                    │
│           ▼                              ▼                    │
│  ┌──────────────────────────────────────────────────────┐    │
│  │              Core Engine (Python)                      │    │
│  │                                                        │    │
│  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  │    │
│  │  │ NL-to-SQL   │  │  Insight     │  │ Viz Engine  │  │    │
│  │  │ (Claude API)│  │  Analyzers   │  │ (Plotly)    │  │    │
│  │  │             │  │  (5 modules) │  │             │  │    │
│  │  └──────┬──────┘  └──────┬───────┘  └─────────────┘  │    │
│  │         │                │                             │    │
│  │         ▼                ▼                             │    │
│  │  ┌──────────────────────────────────────────────┐     │    │
│  │  │         Athena Query Manager                  │     │    │
│  │  │  - Query execution + caching                  │     │    │
│  │  │  - Result parsing (pandas DataFrame)          │     │    │
│  │  │  - Schema introspection                       │     │    │
│  │  └──────────────────────┬───────────────────────┘     │    │
│  └─────────────────────────┼─────────────────────────────┘    │
│                            │                                   │
├────────────────────────────┼───────────────────────────────────┤
│                            │    EXISTING BIO-LAKEHOUSE         │
│                            ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                    Amazon Athena                          │  │
│  │  ┌─────────────┐  ┌──────────────────────────────────┐  │  │
│  │  │ bio_silver   │  │ bio_gold                         │  │  │
│  │  │  - oura_*    │  │  - dashboard_30day (VIEW)        │  │  │
│  │  │  - peloton_* │  │  - workout_recommendations (VIEW)│  │  │
│  │  └─────────────┘  └──────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────┘  │
│                            │                                   │
│  ┌─────────────────────────┼───────────────────────────────┐  │
│  │              Amazon S3  ▼                                │  │
│  │  ┌─────────┐  ┌──────────┐  ┌───────────────────────┐  │  │
│  │  │ Bronze  │  │  Silver  │  │  Gold                  │  │  │
│  │  │ (raw)   │→ │(cleaned) │→ │(enriched + reports/)   │  │  │
│  │  └─────────┘  └──────────┘  └───────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
bio-lakehouse/
├── insights_engine/
│   ├── __init__.py
│   ├── app.py                    # Streamlit entry point
│   ├── config.py                 # Chart config, AWS config, prompts
│   ├── core/
│   │   ├── __init__.py
│   │   ├── nl_to_sql.py          # Claude NL-to-SQL translation
│   │   ├── athena_client.py      # Athena query execution + caching
│   │   ├── schema_manager.py     # DDL introspection for prompt context
│   │   └── result_formatter.py   # DataFrame → natural language answer
│   ├── insights/
│   │   ├── __init__.py
│   │   ├── base.py               # InsightAnalyzer ABC
│   │   ├── sleep_readiness.py    # Signature Insight 3a
│   │   ├── workout_recovery.py   # Signature Insight 3b
│   │   ├── readiness_trend.py    # Signature Insight 3c
│   │   ├── anomaly_detection.py  # Signature Insight 3d
│   │   └── timing_correlation.py # Signature Insight 3e
│   ├── viz/
│   │   ├── __init__.py
│   │   ├── charts.py             # All Plotly chart builders
│   │   ├── theme.py              # Color palette, fonts
│   │   └── export.py             # Static image export
│   ├── reports/
│   │   ├── __init__.py
│   │   ├── weekly_report.py      # Report orchestrator
│   │   ├── templates/
│   │   │   └── weekly.html       # Jinja2 HTML template
│   │   └── delivery.py           # S3 upload + optional email
│   └── prompts/
│       ├── nl_to_sql_system.txt  # System prompt for NL-to-SQL
│       ├── nl_to_sql_examples.txt# Few-shot examples
│       └── insight_narrator.txt  # Prompt for insight narration
├── scripts/
│   ├── run_weekly_report.py      # Cron entry point
│   └── benchmark_nl_to_sql.py   # Test harness for 10 benchmark Qs
├── tests/
│   ├── test_nl_to_sql.py
│   ├── test_insights.py
│   ├── test_athena_client.py
│   └── test_weekly_report.py
├── requirements.txt
└── README.md
```

---

## Data Model / Schema

### Existing Athena Views (read-only)

```sql
-- bio_gold.dashboard_30day
CREATE OR REPLACE VIEW dashboard_30day AS
SELECT
    d.date,
    d.readiness_score,
    d.sleep_score,
    d.sleep_duration_hrs,
    d.deep_sleep_pct,
    d.rem_sleep_pct,
    d.hrv_avg,
    d.resting_hr,
    d.steps,
    d.activity_score,
    w.workout_count,
    w.workout_type,
    w.peloton_output,
    w.peloton_avg_hr,
    w.peloton_duration_min,
    w.peloton_calories
FROM bio_silver.oura_daily d
LEFT JOIN bio_silver.peloton_workouts w
    ON d.date = w.workout_date;

-- bio_gold.workout_recommendations
CREATE OR REPLACE VIEW workout_recommendations AS
SELECT
    date,
    readiness_score,
    CASE
        WHEN avg_readiness_3d >= 80 THEN 'high'
        WHEN avg_readiness_3d >= 65 THEN 'moderate'
        ELSE 'low'
    END AS recommended_intensity,
    avg_readiness_3d AS recent_readiness_avg,
    avg_sleep_3d AS recent_sleep_avg,
    consecutive_workout_days AS workout_streak
FROM bio_silver.derived_recommendations;
```

### New: Insight Cache Table (local SQLite, not Athena)

```sql
CREATE TABLE insight_cache (
    id TEXT PRIMARY KEY,           -- hash of insight_type + date_range
    insight_type TEXT NOT NULL,    -- 'sleep_readiness', 'workout_recovery', etc.
    date_range_start DATE,
    date_range_end DATE,
    result_json TEXT NOT NULL,     -- serialized InsightResult
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ttl_hours INTEGER DEFAULT 24
);
```

### New: Query Log (local SQLite)

```sql
CREATE TABLE query_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    natural_language_query TEXT,
    generated_sql TEXT,
    execution_time_ms INTEGER,
    row_count INTEGER,
    success BOOLEAN,
    error_message TEXT
);
```

---

## API Design (Internal Function Signatures)

```python
# === Core ===

class AthenaClient:
    def __init__(self, database: str = "bio_gold", 
                 results_bucket: str = "bio-lakehouse-athena-results-<AWS_ACCOUNT_ID>"):
        ...
    
    async def execute_query(self, sql: str, timeout_sec: int = 30) -> pd.DataFrame:
        """Execute SQL against Athena, return DataFrame."""
        ...
    
    def get_schema(self, database: str = None) -> dict[str, list[Column]]:
        """Return {table_name: [Column(name, type, comment)]} for prompt context."""
        ...


class NLToSQLEngine:
    def __init__(self, athena: AthenaClient, model: str = "claude-sonnet-4-20250514"):
        ...
    
    async def translate(self, question: str, 
                        history: list[dict] = None) -> NLToSQLResult:
        """Translate natural language to SQL."""
        ...
    
    async def ask(self, question: str, 
                  history: list[dict] = None) -> AnswerResult:
        """End-to-end: translate, execute, format answer."""
        ...


# === Insights ===

class InsightAnalyzer(ABC):
    """Base class for all signature insights."""
    
    @abstractmethod
    async def analyze(self, date_range: DateRange) -> InsightResult:
        ...
    
    @abstractmethod
    def visualize(self, result: InsightResult) -> go.Figure:
        ...
    
    @abstractmethod
    def narrate(self, result: InsightResult) -> str:
        """Generate human-readable narrative."""
        ...


@dataclass
class InsightResult:
    insight_type: str
    title: str
    narrative: str
    statistics: dict          # r, p-value, n, means, etc.
    chart: go.Figure | None
    caveats: list[str]        # Statistical limitations
    data: pd.DataFrame        # Underlying data for transparency


# === Reports ===

class WeeklyReportGenerator:
    def __init__(self, analyzers: list[InsightAnalyzer], 
                 athena: AthenaClient):
        ...
    
    async def generate(self, week_ending: date = None) -> ReportResult:
        """Run all analyzers, compile report."""
        ...
    
    async def deliver(self, report: ReportResult, 
                      targets: list[str] = ["s3"]) -> DeliveryResult:
        """Upload to S3 gold bucket under reports/ prefix."""
        ...


@dataclass
class ReportResult:
    html: str
    markdown: str
    insights: list[InsightResult]
    metadata: dict  # generation_time, data_completeness, date_range
```

---

## Non-Functional Requirements

### Performance
- NL-to-SQL response: < 15 seconds end-to-end (Claude API + Athena query)
- Weekly report generation: < 60 seconds
- Streamlit page load: < 3 seconds
- Athena query caching: identical queries within 1 hour return cached results

### Security
- AWS credentials via environment variables or CLI profile (never hardcoded)
- Claude API key via environment variable `ANTHROPIC_API_KEY`
- No PII beyond Chris's own biometric data (single-user system)
- S3 reports bucket has no public access
- Streamlit runs locally only (no public deployment for this project)

### Accessibility
- All charts include alt-text descriptions
- Color palette passes WCAG AA contrast ratio
- Streamlit UI is keyboard-navigable
- Chart color choices work for common colorblindness (no red/green reliance)

### Observability
- Query log captures all NL-to-SQL translations with timing
- Weekly report logs success/failure to CloudWatch or local file
- Error states surface user-friendly messages (no raw stack traces in UI)

---

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Small sample size (n≈90) limits statistical significance | High | Medium | Report all confidence intervals, flag low-n caveats explicitly, use non-parametric tests |
| NL-to-SQL generates incorrect queries | Medium | High | Few-shot examples, schema context injection, display SQL for verification, benchmark test suite |
| Athena cold-start latency degrades UX | Medium | Low | Cache frequent queries, show loading spinner with "Querying your data lake..." |
| Claude API rate limits during report generation | Low | Medium | Sequential insight generation (not parallel), retry with exponential backoff |
| Gold layer schema changes break queries | Low | High | Schema introspection at startup, version-pin view DDLs in config |
| Correlations are spurious with 3 months of data | High | Medium | Explicitly label as "observational, not causal", require p < 0.05 and n > 20 to call "significant" |

---

## Milestones and Timeline

### Phase 1: NL-to-SQL + Weekly Report Foundation (Days 1-7)

| Day | Milestone | Deliverable |
|-----|-----------|-------------|
| 1 | Project scaffolding | Directory structure, requirements.txt, config.py, AthenaClient with execute + schema |
| 2 | NL-to-SQL engine | Prompt engineering, NLToSQLEngine.translate(), 3 benchmark questions passing |
| 3 | NL-to-SQL refinement | All 10 benchmark questions tested, ≥7 passing, query logging |
| 4 | Streamlit chat UI | Chat interface, SQL display, session history |
| 5 | Insight analyzers (3a, 3b) | Sleep→readiness correlation, workout→recovery analysis |
| 6 | Insight analyzers (3c, 3d, 3e) | Readiness trends, anomaly detection, timing correlation |
| 7 | Weekly report v1 | Report orchestrator, HTML template, first generated report |

### Phase 2: Visualization + Polish + Delivery (Days 8-14)

| Day | Milestone | Deliverable |
|-----|-----------|-------------|
| 8 | Plotly chart suite | All 7 chart types implemented with theme |
| 9 | Charts integrated into Streamlit + report | Interactive in app, static PNG in report |
| 10 | Cron delivery | `run_weekly_report.py`, S3 upload, launchd/crontab config |
| 11 | Testing | Unit tests for insights, integration test for NL-to-SQL benchmark |
| 12 | README + architecture diagram | Portfolio-ready documentation |
| 13 | Polish | Dark mode, error handling, edge cases, loading states |
| 14 | Demo recording | Screen recording for portfolio, LinkedIn post draft |

---

## Out of Scope (v1)

- **Multi-user support** — single user only
- **Real-time streaming** — batch/query only
- **Mobile app** — Streamlit web only
- **New data sources** — only Oura + Peloton (no Apple Health, Whoop, etc.)
- **Predictive modeling / ML** — descriptive and correlational analytics only
- **Public deployment** — runs locally
- **Automated data ingestion** — uses existing Glue jobs
- **Natural language write-back** — read-only queries

---

## Future Enhancements (v2 Backlog)

1. **Predictive readiness model** — "Based on tonight's sleep, your predicted readiness tomorrow is X" (requires more data, likely 6+ months)
2. **Apple Health integration** — additional bronze source for steps, weight, nutrition
3. **Supplement tracking** — manual input form for supplements/caffeine → correlate with readiness
4. **Claude Computer Use for Peloton** — auto-extract workout data without CSV export
5. **Public Streamlit Cloud deployment** — demo mode with synthetic data
6. **Voice interface** — ask health questions via voice (Whisper → NL-to-SQL)
7. **Slack/Discord delivery** — weekly report sent to a channel
8. **Goal tracking** — set readiness/sleep targets, track progress, get coaching
9. **A/B experiment framework** — "Try morning workouts for 2 weeks, evening for 2 weeks, compare"
10. **Multi-model comparison** — benchmark Claude vs GPT-4 vs Llama for NL-to-SQL accuracy

---

## Appendix: Environment Setup

```bash
# Requirements
pip install streamlit plotly anthropic boto3 pandas scipy jinja2

# Environment variables
export ANTHROPIC_API_KEY="<YOUR_ANTHROPIC_API_KEY>"
export AWS_PROFILE="default"  # or specific profile
export BIO_ATHENA_DATABASE="bio_gold"
export BIO_S3_GOLD_BUCKET="bio-lakehouse-gold-<AWS_ACCOUNT_ID>"
export BIO_ATHENA_RESULTS_BUCKET="bio-lakehouse-athena-results-<AWS_ACCOUNT_ID>"

# Run
cd bio-lakehouse/insights_engine
streamlit run app.py

# Run weekly report manually
python scripts/run_weekly_report.py --week-ending 2026-02-16
```

---

---

## CRITICAL ADDITIONS: Missing Optimization Views

**AUDIT NOTE (Feb 17):** Claude Code identified that while the core medallion architecture is deployed, the following **optimization views are essential** to the insights engine. These are **not stretch goals** — they are core to differentiating a data pipeline from an intelligence product.

### A. `energy_state_classification` View

Identifies your "125% Energy State" — the peak performance zone.

```sql
CREATE OR REPLACE VIEW bio_gold.energy_state_classification AS
SELECT
  date,
  readiness_score,
  sleep_score,
  hrv_value,
  CASE
    WHEN readiness_score >= 85 AND sleep_score >= 88 AND hrv_value >= 75 THEN 'peak_performance'
    WHEN readiness_score >= 80 AND sleep_score >= 85 AND hrv_value >= 70 THEN 'high_performance'
    WHEN readiness_score >= 70 AND sleep_score >= 80 THEN 'moderate_performance'
    WHEN readiness_score < 70 OR sleep_score < 75 THEN 'recovery_mode'
  END as energy_state
FROM bio_gold.dashboard_30day
ORDER BY date DESC;
```

**Purpose:** Identify peak days for scheduling interviews or high-intensity workouts.

**Acceptance Criteria:**
- [ ] Correctly classifies days into 4 energy states
- [ ] Returns 5-10 "peak_performance" days in 3-month dataset

### B. `workout_type_optimization` Table

Historical analysis: "When my readiness is 70-84, which workout gave me the best output?"

```sql
CREATE TABLE IF NOT EXISTS bio_gold.workout_type_optimization AS
SELECT
  readiness_bucket,
  workout_type,
  COUNT(*) as workout_count,
  AVG(total_output_kj) as avg_output,
  MAX(total_output_kj) as peak_output,
  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_output_kj) as p75_output
FROM (
  SELECT
    CASE
      WHEN readiness_score >= 85 THEN '85+'
      WHEN readiness_score >= 75 THEN '75-84'
      WHEN readiness_score >= 65 THEN '65-74'
      ELSE '<65'
    END as readiness_bucket,
    disciplines as workout_type,
    total_output_kj,
    date
  FROM bio_gold.dashboard_30day
  WHERE had_workout = true
) grouped
GROUP BY readiness_bucket, workout_type
ORDER BY readiness_bucket DESC, avg_output DESC;
```

**Purpose:** Enable data-driven workout selection based on daily readiness.

**Acceptance Criteria:**
- [ ] Shows which discipline maximizes output per readiness band
- [ ] Breakdowns by ≥2 readiness buckets and ≥3 workout types

### C. `readiness_output_zone_mapping` View

Labeled interpretation zones for readiness-to-output ratio.

```sql
CREATE OR REPLACE VIEW bio_gold.readiness_output_zone_mapping AS
SELECT
  date,
  readiness_to_output_ratio,
  CASE
    WHEN readiness_to_output_ratio > 4.0 THEN 'overreaching'
    WHEN readiness_to_output_ratio BETWEEN 2.5 AND 4.0 THEN 'high_performance'
    WHEN readiness_to_output_ratio BETWEEN 1.5 AND 2.5 THEN 'moderate_performance'
    WHEN readiness_to_output_ratio < 1.5 THEN 'undertrained'
  END as performance_zone
FROM bio_gold.dashboard_30day
WHERE readiness_to_output_ratio IS NOT NULL
ORDER BY date DESC;
```

**Purpose:** Provide instant interpretation of the readiness-to-output metric.

### D. `sleep_to_output_prediction` View

Does Tuesday sleep predict Wednesday workout output?

```sql
CREATE OR REPLACE VIEW bio_gold.sleep_to_output_prediction AS
SELECT
  a.date as sleep_date,
  b.date as workout_date,
  a.sleep_score,
  b.total_output_kj
FROM bio_gold.dashboard_30day a
LEFT JOIN bio_gold.dashboard_30day b
  ON b.date = a.date + INTERVAL '1 day'
WHERE b.had_workout = true
ORDER BY workout_date DESC;
```

**Acceptance Criteria:**
- [ ] Covers all workouts with preceding sleep data
- [ ] Reveals patterns (e.g., "88+ sleep → 30% higher output")

### E. `readiness_performance_correlation` View

Correlation coefficients between readiness/sleep/HRV and output.

```sql
CREATE OR REPLACE VIEW bio_gold.readiness_performance_correlation AS
SELECT
  'readiness_to_output' as metric,
  CORR(readiness_score, total_output_kj) as correlation,
  COUNT(*) as sample_count
FROM bio_gold.dashboard_30day
WHERE had_workout = true AND total_output_kj IS NOT NULL

UNION ALL

SELECT 'sleep_to_output', CORR(sleep_score, total_output_kj), COUNT(*)
FROM bio_gold.dashboard_30day
WHERE had_workout = true AND total_output_kj IS NOT NULL

UNION ALL

SELECT 'hrv_to_output', CORR(hrv_value, total_output_kj), COUNT(*)
FROM bio_gold.dashboard_30day
WHERE had_workout = true AND total_output_kj IS NOT NULL;
```

**Purpose:** Answer "Does my HRV or sleep matter more for workout output?"

---

**REVISED TIMELINE:**

Phase 1 (Days 1-3): Create all 5 optimization views in Athena + validate with data
Phase 2 (Days 4-7): Integrate views into NL-to-SQL engine + Streamlit dashboard
Phase 3 (Days 8-10): Visualization + weekly report delivery
Phase 4 (Days 11-14): Polish, testing, documentation

These views are the **core differentiator** of the project. Without them, you have an ETL pipeline. With them, you have a personal optimization engine.

---

*This document is intended to be picked up by Claude Code and built end-to-end. All function signatures, file paths, and configurations are implementation-ready.*
