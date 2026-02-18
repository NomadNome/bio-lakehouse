# Bio-Optimization Data Lakehouse & Insights Engine

A serverless health-analytics platform on AWS that transforms raw biometric data from Oura Ring and Peloton into an AI-powered intelligence product. Ask natural-language health questions, get SQL-backed answers with visualizations, and receive automated weekly insight reports.

## Architecture

```
                     PRESENTATION LAYER
  ┌──────────────────┐    ┌─────────────────────────────┐
  │   Streamlit UI    │    │   Weekly Report (HTML/PNG)   │
  │  - Chat (NL→SQL) │    │   - Cron: Mon 7am EST        │
  │  - Insight charts │    │   - Saved to S3 gold         │
  │  - Report viewer  │    │                              │
  └────────┬─────────┘    └──────────────┬──────────────┘
           │       INTELLIGENCE LAYER    │
           ▼                             ▼
  ┌──────────────────────────────────────────────────────┐
  │              Insights Engine (Python)                  │
  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  │
  │  │ NL-to-SQL   │  │  5 Insight   │  │ Viz Engine  │  │
  │  │ (Claude API)│  │  Analyzers   │  │ (Plotly)    │  │
  │  └──────┬──────┘  └──────┬───────┘  └─────────────┘  │
  │         ▼                ▼                             │
  │  ┌──────────────────────────────────────────────┐     │
  │  │         AthenaClient (query + cache)          │     │
  │  └──────────────────────┬───────────────────────┘     │
  └─────────────────────────┼─────────────────────────────┘
                            │    DATA LAKEHOUSE
                            ▼
  ┌─────────┐  ┌──────────┐  ┌──────────────────────────┐
  │ Bronze  │→ │  Silver  │→ │  Gold (9 views/tables)   │
  │ S3 raw  │  │ S3 clean │  │  Athena + QuickSight     │
  └─────────┘  └──────────┘  └──────────────────────────┘
       ▲            ▲                    ▲
       │            │                    │
    Lambda       Glue ETL          EventBridge
    trigger      PySpark           daily 2am UTC
```

## Key Features

- **Natural Language Queries** -- Ask health questions in plain English ("Am I overtraining?"), get SQL-backed answers in seconds via Claude Sonnet
- **5 Signature Insights** -- Sleep-readiness correlation, workout recovery analysis, readiness trends, anomaly detection, intensity impact
- **Automated Weekly Reports** -- Claude-narrated HTML reports with key metrics, delivered to S3 every Monday
- **9 Gold Layer Views** -- Pre-computed analytics: energy states, workout optimization, overtraining risk, correlations, weekly trends
- **Interactive Dashboard** -- Streamlit app with dark-themed Plotly charts, collapsible SQL, data tables
- **Medallion Architecture** -- Bronze/Silver/Gold data layers with CloudFormation IaC, Glue ETL, Lambda ingestion triggers

## Tech Stack

| Layer | Technology |
|-------|------------|
| Infrastructure | AWS CloudFormation, S3, Lambda, DynamoDB, EventBridge |
| ETL | AWS Glue (PySpark) |
| Query | Amazon Athena (Presto/Trino SQL) |
| AI | Claude Sonnet (NL-to-SQL, narrative generation) |
| Analytics | Python, pandas, SciPy (Pearson, Mann-Whitney U) |
| Visualization | Plotly, Amazon QuickSight |
| App | Streamlit |
| Reports | Jinja2 HTML templates |

## Project Structure

```
bio-lakehouse/
├── infrastructure/cloudformation/
│   ├── bronze-stack.yaml
│   ├── silver-stack.yaml
│   └── gold-stack.yaml
├── lambda/ingestion_trigger/handler.py
├── glue/
│   ├── bio_etl_utils.py
│   ├── oura_normalizer.py
│   ├── peloton_normalizer.py
│   └── readiness_aggregator.py
├── athena/views.sql                   # 9 gold layer views
├── insights_engine/
│   ├── app.py                         # Streamlit entry point
│   ├── config.py
│   ├── core/
│   │   ├── athena_client.py           # Query execution + caching
│   │   └── nl_to_sql.py              # Claude NL-to-SQL translation
│   ├── insights/
│   │   ├── base.py                    # InsightAnalyzer ABC
│   │   ├── sleep_readiness.py         # 3a: Sleep → Readiness
│   │   ├── workout_recovery.py        # 3b: Workout → Recovery
│   │   ├── readiness_trend.py         # 3c: Trends + Rolling Avg
│   │   ├── anomaly_detection.py       # 3d: Anomaly Flags
│   │   └── timing_correlation.py      # 3e: Intensity Impact
│   ├── viz/
│   │   ├── theme.py                   # Plotly dark theme
│   │   └── export.py                  # Static PNG export
│   ├── reports/
│   │   ├── weekly_report.py           # Report orchestrator
│   │   ├── delivery.py                # S3 upload
│   │   └── templates/weekly.html      # Jinja2 template
│   └── prompts/
│       ├── nl_to_sql_system.txt
│       ├── nl_to_sql_examples.txt
│       └── insight_narrator.txt
├── scripts/
│   └── run_weekly_report.py           # CLI / cron entry point
├── tests/                             # 43 unit tests
├── quicksight/setup_guide.md
└── requirements.txt
```

## Quick Start

**Prerequisites:** Python 3.9+, AWS CLI configured, Anthropic API key.

```bash
# Install dependencies
pip install -r insights_engine/requirements.txt

# Set environment variables
export ANTHROPIC_API_KEY="sk-ant-..."
export AWS_PROFILE="default"

# Launch the Streamlit app
python -m streamlit run insights_engine/app.py

# Generate a weekly report
python scripts/run_weekly_report.py --week-ending 2026-02-16

# Run tests
pytest tests/
```

## NL-to-SQL Benchmarks

All benchmark questions tested end-to-end against live Athena data:

| Question | View Used | Confidence | Time |
|----------|-----------|-----------|------|
| "What was my average readiness score last week?" | `dashboard_30day` | 95% | 19.8s |
| "What's the correlation between my sleep and readiness?" | `readiness_performance_correlation` | 95% | 21.2s |
| "Show me days where my readiness dropped below 70" | `energy_state` | 95% | 7.2s |
| "Am I overtraining?" | `overtraining_risk` | 90% | 21.4s |

The NL-to-SQL engine uses Claude Sonnet with live schema DDL injection, 10 few-shot examples, and Presto/Trino SQL rules. System prompt is hydrated at runtime with the actual Athena schema (~1,500 tokens).

## Signature Insights

Each insight module implements `analyze()`, `visualize()`, and `narrate()`:

| Insight | Analysis | Statistical Test | Chart |
|---------|----------|-----------------|-------|
| Sleep → Readiness | Pearson correlation between sleep score and next-day readiness | r, p-value, linear regression | Scatter + regression line |
| Workout → Recovery | Next-day readiness segmented by workout type (cycling, strength, rest) | Mann-Whitney U test | Box plot |
| Readiness Trends | Daily readiness with 7-day and 14-day rolling averages | Linear regression on 14-day MA for trend direction | Line + rolling averages |
| Anomaly Detection | Flags days >1.5 std devs below personal mean, missed workout streaks | Z-score threshold | Timeline + highlighted anomalies |
| Intensity Impact | Next-day readiness by workout intensity bucket (high vs low output) | Mann-Whitney U test | Grouped bar chart |

## Weekly Report

Automated HTML reports run all 5 analyzers, generate a cohesive narrative via Claude, and render with a dark-themed Jinja2 template. Reports are saved locally and uploaded to S3.

```bash
# Generate manually
python scripts/run_weekly_report.py --week-ending 2026-02-16

# Local only (no S3 upload)
python scripts/run_weekly_report.py --local-only

# Cron setup (Mondays 7am EST)
# 0 7 * * 1 /usr/local/bin/python3 /path/to/scripts/run_weekly_report.py
```

## Data Sources

| Source | Data | Volume | Date Range |
|--------|------|--------|------------|
| Oura Ring | Sleep score, readiness, HRV, resting HR, activity | ~90 days | Nov 2025 -- Feb 2026 |
| Peloton | Cycling/strength workouts, output (kJ), watts, heart rate | 833 workouts | May 2021 -- Feb 2026 |

## Gold Layer Views

| View | Description |
|------|-------------|
| `daily_readiness_performance` | Core table joining Oura + Peloton by date with readiness-to-output ratio |
| `dashboard_30day` | 30-day rolling view with 7-day and 30-day averages |
| `workout_recommendations` | Workout intensity recommendations based on recent readiness |
| `energy_state` | Classifies days into peak/high/moderate/low energy states |
| `workout_type_optimization` | Historical output by readiness bucket and workout discipline |
| `sleep_performance_prediction` | Sleep score to next-day readiness and output prediction |
| `readiness_performance_correlation` | Pearson correlations segmented by readiness level |
| `weekly_trends` | Week-over-week progression with trend indicators |
| `overtraining_risk` | Overtraining risk flags based on readiness, HRV, and workout frequency |

---

Built with [Claude](https://anthropic.com) | Data pipeline on AWS | Analytics by Athena + Plotly
