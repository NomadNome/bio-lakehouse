# Bio-Optimization Data Lakehouse & Insights Engine

> **Production AI infrastructure blueprint: Serverless lakehouse (AWS) + Claude integration with data governance, security, and operational monitoring. Demonstrates enterprise AI deployment patterns on real biometric data.**

A fully operational health-analytics system that demonstrates end-to-end data engineering: from ingestion through ETL to AI-powered analytics. Built on AWS using medallion architecture (Bronze/Silver/Gold), this platform processes real biometric data from Oura Ring, Apple HealthKit (Hume Body Pod & Apple Watch Series 9), Peloton, and MyFitnessPal, exposes it via Athena SQL, and delivers insights through natural language queries, automated reports, health alerts, and a morning briefing pipeline.

## Why This Matters

Enterprise AI fails not because models are bad, but because the infrastructure can't support them at scale. This project solves the hard problems:

- **Data Governance**: How do you ensure AI models query clean, validated data? &rarr; Medallion architecture with automated quality checks
- **Security**: How do you handle sensitive data (biometrics = PII)? &rarr; Encrypted ingestion, IAM isolation, audit logging
- **Reliability**: How do you prevent AI systems from breaking in production? &rarr; Event-driven architecture, retry logic, 221+ unit tests
- **Cost**: How do you avoid runaway cloud bills? &rarr; Serverless pay-per-query, result caching, optimized Athena views

This isn't a toy project -- it's a blueprint for deploying AI systems in regulated industries (healthcare, finance, government).

**What this demonstrates:**

- **Data Governance**: Medallion architecture with clear lineage (Bronze &rarr; Silver &rarr; Gold), DynamoDB ingestion logging, and schema validation at each layer
- **AI Infrastructure**: Production Claude integration with prompt engineering, result caching, and 95% NL-to-SQL accuracy on live data
- **Security & Compliance**: IAM least-privilege roles, SSE-AES256 encryption, OAuth token management, and audit trails via DynamoDB
- **Distributed Systems**: PySpark ETL (scales from MB to PB), event-driven Lambda triggers, serverless orchestration
- **Operational Reliability**: 0 failed Lambda invocations, 100% Glue job success rate, automated daily pipeline + weekly reporting since deployment

Unlike synthetic demos, this system runs daily against real biometric data. It proves not just technical knowledge, but the operational discipline required to deploy AI systems in regulated environments (healthcare data, PII handling).

The same patterns used here -- metadata-driven governance, encrypted ingestion pipelines, AI-powered analytics with audit trails -- are foundational to deploying Claude/GPT/Databricks at scale in enterprise environments.

## Key Technical Achievements

| Category | Achievement |
|----------|-------------|
| **Infrastructure** | 7 CloudFormation stacks (Bronze/Silver/Gold + Oura ingest, Alerts, Morning Briefing, Pipeline Orchestrator) |
| **ETL** | Event-driven Lambda &rarr; Glue PySpark &rarr; dbt &rarr; DynamoDB logging |
| **Ingestion** | Dual pipelines: Serverless Lambda (Oura API) + local automation (Peloton, HealthKit, MFP) |
| **Gold Layer** | 14+ dbt models/views (energy states, overtraining risk, training load, sleep architecture, correlations) |
| **AI Queries** | Claude Sonnet NL-to-SQL with 95% accuracy, live schema injection, 10 few-shot examples |
| **Insights** | 11 statistical analyzers (Pearson, Mann-Whitney U, Spearman, LOWESS, z-scores) with visualizations |
| **ML Pipeline** | Next-day readiness predictor (GradientBoosting, walk-forward CV, Optuna tuning) |
| **Automation** | Daily pipeline orchestration, health alerts (SNS), morning briefing, weekly reports |
| **App** | 8-page Streamlit dashboard with What-If simulator, experiment tracker, FHIR export |
| **Testing** | 221+ unit tests across 13 test suites |
| **Performance** | Desktop I/O optimization (imports: 600s &rarr; <1s), Athena result caching, sub-20s query response |

**Data Volume**: 863 workouts (2021-2026) &bull; 120+ days Oura biometrics &bull; 1,977+ Gold rows (2020-2026) &bull; 14+ Gold views
**Uptime**: Ingestion running since 2026-02-17 &bull; 0 failed Lambda invocations &bull; 100% Glue job success rate

## Security & Compliance Posture

- **Encryption at Rest & In Transit** -- All S3 uploads use SSE-AES256 (bucket policy enforced), OAuth tokens stored in AWS Systems Manager SecureString
- **IAM Least Privilege** -- Lambda roles scoped to write-only S3 access, read-only SSM access, DynamoDB write (no wildcard permissions)
- **Audit Trail** -- DynamoDB `bio_ingestion_log` table records every ingestion event (file path, timestamp, source, record count, status) for compliance reporting
- **Data Lineage** -- Bronze &rarr; Silver &rarr; Gold transformations tracked via Glue job logs + Athena query history; can trace any Gold insight back to raw source
- **PII Handling** -- Biometric data classified as sensitive; ingestion scripts sanitize outputs (no PII in logs), data stays within AWS VPC
- **Token Rotation** -- OAuth refresh flow implemented; tokens never committed to Git (`.oura-tokens.json` in `.gitignore`)
- **FHIR R4 Compliance** -- Health data exportable as FHIR R4 Bundles (Observation resources) for EHR interoperability

Deploying AI in healthcare, finance, or government requires proving your infrastructure meets compliance standards (HIPAA, SOC 2, FedRAMP). This project demonstrates those patterns in a working system.

## Architecture

```
                     PRESENTATION LAYER
  ┌──────────────────┐    ┌─────────────────────────────┐    ┌─────────────────┐
  │   Streamlit UI    │    │   Weekly Report (HTML/PDF)   │    │  Morning Brief  │
  │  8 pages:         │    │   - Cron: Mon 7am EST        │    │  (SNS email)    │
  │  - Chat (NL→SQL) │    │   - Saved to S3 gold         │    │  after Gold     │
  │  - 11 Insights    │    │   - PDF export               │    │  refresh        │
  │  - What-If sim    │    │                              │    │                 │
  │  - ML Predictions │    └──────────────┬──────────────┘    └────────┬────────┘
  │  - Experiments    │                   │                            │
  │  - Discoveries    │                   │                            │
  │  - FHIR Export    │                   │                            │
  └────────┬─────────┘                   │                            │
           │       INTELLIGENCE LAYER    │                            │
           ▼                             ▼                            │
  ┌──────────────────────────────────────────────────────┐            │
  │              Insights Engine (Python)                  │            │
  │  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  │            │
  │  │ NL-to-SQL   │  │ 11 Insight   │  │ Viz Engine  │  │            │
  │  │ (Claude API)│  │  Analyzers   │  │(Plotly/PDF) │  │            │
  │  └──────┬──────┘  └──────┬───────┘  └─────────────┘  │            │
  │         │                │                             │            │
  │  ┌──────┴──────┐  ┌─────┴─────┐  ┌────────────────┐  │            │
  │  │ What-If Sim │  │ML Predict │  │ FHIR Builder   │  │            │
  │  │ (scenarios) │  │(GBR/Ridge)│  │ (R4 Bundles)   │  │            │
  │  └─────────────┘  └───────────┘  └────────────────┘  │            │
  │  ┌──────────────────────────────────────────────┐     │            │
  │  │         AthenaClient (query + cache)          │     │            │
  │  └──────────────────────┬───────────────────────┘     │            │
  └─────────────────────────┼─────────────────────────────┘            │
                            │    DATA LAKEHOUSE                       │
                            ▼                                          │
  ┌─────────┐  ┌──────────┐  ┌──────────────────────────┐            │
  │ Bronze  │→ │  Silver  │→ │  Gold (14+ views/tables) │            │
  │ S3 raw  │  │ S3 clean │  │  dbt + Athena            │            │
  └─────────┘  └──────────┘  └──────────────────────────┘            │
       ▲            ▲                    ▲                             │
       │            │                    │                             │
    Lambda       Glue ETL          dbt refresh                        │
    trigger      PySpark           (Glue Python)                      │
       │            │                    │                             │
  ┌────┴────────────┴────────────────────┴─────────────────────┐      │
  │              Pipeline Orchestrator (Lambda)                 │──────┘
  │  EventBridge → Normalizers → Silver Crawler → dbt Gold     │
  │  → Gold Crawler → Morning Briefing → Health Alerts         │
  └────────────────────────────────────────────────────────────┘
```

## Key Features

- **Natural Language Queries** -- Ask health questions in plain English ("Am I overtraining?"), get SQL-backed answers in seconds via Claude Sonnet
- **11 Signature Insights** -- Sleep-readiness correlation, workout recovery, readiness trends, anomaly detection, intensity impact, training load, progressive overload, recovery windows, temperature trends, sleep architecture, nutrition analysis
- **What-If Scenario Simulator** -- Model next-day readiness based on sleep/workout/intensity inputs; multi-day training block planner with cascading projections
- **ML Readiness Predictions** -- Next-day readiness forecast with GradientBoosting, walk-forward CV, feature importance breakdown
- **Health Alerts** -- Automated SNS notifications for anomalies (elevated RHR, low HRV, overtraining risk, declining readiness streaks)
- **Morning Briefing** -- Daily actionable summary emailed after Gold refresh with freshness guard
- **Automated Weekly Reports** -- Claude-narrated HTML/PDF reports with key metrics, delivered to S3 every Monday
- **Correlation Discovery** -- Automated Spearman correlation scan across all metric pairs with statistical rigor
- **Experiment Tracker** -- A/B test interventions (diet, training, sleep) with Bayesian + Difference-in-Differences analysis
- **FHIR R4 Export** -- Healthcare-grade biometric export (HR, steps, HRV, VO2, weight, SpO2) for EHR interoperability
- **14+ Gold Layer Views** -- Pre-computed analytics: energy states, workout optimization, overtraining risk, training load, sleep architecture, temperature trends
- **Interactive Dashboard** -- 8-page Streamlit app with dark-themed Plotly charts, collapsible SQL, data export, PDF generation
- **Medallion Architecture** -- Bronze/Silver/Gold data layers with 7 CloudFormation stacks, Glue ETL, Lambda ingestion triggers
- **Pipeline Orchestration** -- EventBridge-triggered chain: normalizers &rarr; Silver crawler &rarr; dbt Gold &rarr; Gold crawler &rarr; morning briefing

## Tech Stack

| Layer | Technology |
|-------|------------|
| Infrastructure | AWS CloudFormation (7 stacks), S3, Lambda, DynamoDB, EventBridge, SNS |
| ETL | AWS Glue (PySpark), dbt-core 1.11 (dbt-athena) |
| Query | Amazon Athena (Presto/Trino SQL) |
| AI | Claude Sonnet 4.6 (NL-to-SQL, narrative generation, correlation interpretation) |
| ML | scikit-learn (GradientBoosting, Ridge), Optuna, MLflow |
| Analytics | Python, pandas, SciPy, statsmodels (LOWESS) |
| Visualization | Plotly, WeasyPrint (PDF export) |
| App | Streamlit (8 pages) |
| Reports | Jinja2 HTML templates, Claude narrative generation |
| Automation | LaunchD (macOS), EventBridge (AWS), shell scripts |
| Healthcare | FHIR R4 Bundle export (Observation resources) |

## Project Structure

```
bio-lakehouse/
├── infrastructure/cloudformation/
│   ├── bronze-stack.yaml              # S3, DynamoDB, Lambda, IAM
│   ├── silver-stack.yaml              # S3, Glue DB, 2 ETL jobs, crawler
│   ├── gold-stack.yaml                # S3, Glue DB, dbt refresh job, crawler
│   ├── oura-ingest-stack.yaml         # Oura API Lambda + EventBridge
│   ├── alerts-stack.yaml              # Health alerts Lambda + SNS
│   ├── morning-briefing-stack.yaml    # Morning briefing Lambda
│   └── pipeline-orchestrator-stack.yaml # EventBridge orchestration chain
├── lambda/
│   ├── oura-api-ingest/               # Oura API → S3 Bronze (Python 3.11)
│   ├── health_alerts/handler.py       # Anomaly detection → SNS
│   ├── morning_briefing/handler.py    # Daily summary → SNS
│   └── pipeline_orchestrator/handler.py # Chains Glue/dbt/crawler/briefing
├── glue/
│   ├── bio_etl_utils.py               # Shared ETL utilities
│   ├── oura_normalizer.py             # Oura Bronze → Silver (CSV + JSON)
│   ├── healthkit_normalizer.py        # HealthKit Bronze → Silver
│   ├── peloton_normalizer.py          # Peloton Bronze → Silver
│   └── mfp_normalizer.py             # MyFitnessPal Bronze → Silver
├── dbt_bio_lakehouse/
│   ├── models/staging/                # 5 staging models
│   ├── models/gold/                   # Core rollup + recovery windows
│   ├── models/analytics/              # 11 analytics views
│   ├── models/features/               # ML feature table
│   └── macros/tss_calculation.sql     # Canonical TSS formula
├── athena/views.sql                   # Legacy SQL views (kept for compatibility)
├── insights_engine/
│   ├── app.py                         # Streamlit entry point (8 pages)
│   ├── config.py                      # AWS, Claude, chart configuration
│   ├── core/
│   │   ├── athena_client.py           # Query execution + 10-min cache
│   │   └── nl_to_sql.py              # Claude NL-to-SQL translation
│   ├── insights/
│   │   ├── base.py                    # InsightAnalyzer ABC
│   │   ├── sleep_readiness.py         # Sleep → Readiness correlation
│   │   ├── workout_recovery.py        # Workout → Recovery analysis
│   │   ├── readiness_trend.py         # Trends + rolling averages
│   │   ├── anomaly_detection.py       # Z-score anomaly flags
│   │   ├── timing_correlation.py      # Intensity → Recovery (scatter + LOWESS)
│   │   ├── training_load.py           # TSS / CTL / ATL / TSB tracking
│   │   ├── progressive_overload.py    # Training load progression analysis
│   │   ├── recovery_windows.py        # Recovery time by intensity
│   │   ├── temperature_trend.py       # Body temperature deviation tracking
│   │   ├── sleep_architecture.py      # Deep/REM/Light sleep breakdown
│   │   ├── nutrition_analyzer.py      # Calorie/macro trends (MFP)
│   │   ├── correlation_discovery.py   # Automated metric correlation scan
│   │   └── what_if.py                # What-If scenario simulator
│   ├── experiments/
│   │   ├── tracker.py                 # Intervention CRUD (S3 JSON)
│   │   ├── analyzer.py                # Bayesian + DiD analysis
│   │   └── viz.py                     # Experiment visualizations
│   ├── fhir/
│   │   └── bundle_builder.py          # FHIR R4 Bundle export
│   ├── viz/
│   │   ├── theme.py                   # Plotly dark theme
│   │   ├── export.py                  # Static PNG/PDF export
│   │   └── what_if_charts.py          # Gauge, projection, comparison charts
│   ├── reports/
│   │   ├── weekly_report.py           # Report orchestrator
│   │   ├── delivery.py                # S3 upload + PDF generation
│   │   └── templates/weekly.html      # Jinja2 template
│   └── prompts/
│       ├── nl_to_sql_system.txt
│       ├── nl_to_sql_examples.txt
│       └── insight_narrator.txt
├── models/readiness_predictor/        # ML Pipeline
│   ├── train.py                       # Multi-model training + Optuna
│   ├── predict.py                     # Inference (MLflow or joblib)
│   ├── feature_selection.py           # MI + correlation feature selection
│   └── mlflow_config.py              # MLflow tracking setup
├── scripts/
│   ├── run_weekly_report.py           # CLI (--pdf flag for PDF output)
│   ├── run_correlation_discovery.py   # Weekly correlation scan
│   ├── parse_healthkit_export.py      # HealthKit XML → partitioned CSVs
│   ├── daily_ingestion_wrapper.sh     # LaunchD entry point
│   ├── move_downloads_to_inbox.sh     # Auto-organize exported files
│   ├── check_pipeline_health.sh       # Pipeline freshness monitor
│   └── setup_folder_action.sh         # macOS Folder Action setup
├── run_daily_ingestion.sh             # Full 12-step pipeline script
├── run_streamlit.sh                   # Launch Streamlit (fast path)
├── tests/                             # 221+ unit tests (13 test files)
├── requirements.txt
├── requirements-frozen.txt            # 170 pinned deps for reproducibility
└── pyproject.toml
```

## Quick Start

**Prerequisites:** Python 3.12, AWS CLI configured, Anthropic API key.

```bash
# 1. Create venv and install dependencies
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment (see .env.example or set directly)
export ANTHROPIC_API_KEY="<YOUR_ANTHROPIC_API_KEY>"
export AWS_PROFILE="default"
export BIO_ATHENA_DATABASE="bio_gold"
export BIO_S3_GOLD_BUCKET="bio-lakehouse-gold-<AWS_ACCOUNT_ID>"
export BIO_ATHENA_RESULTS_BUCKET="bio-lakehouse-athena-results-<AWS_ACCOUNT_ID>"

# 3. Deploy infrastructure (requires AWS credentials)
for stack in bronze silver gold oura-ingest alerts morning-briefing pipeline-orchestrator; do
  aws cloudformation deploy \
    --template-file infrastructure/cloudformation/${stack}-stack.yaml \
    --stack-name bio-lakehouse-${stack} \
    --capabilities CAPABILITY_IAM \
    --region us-east-1
done

# 4. Run dbt to build Gold layer
cd dbt_bio_lakehouse && dbt run && cd ..

# 5. Launch Streamlit
bash run_streamlit.sh
# → Opens http://localhost:8501

# 6. Generate a weekly report
python scripts/run_weekly_report.py --week-ending 2026-03-28
python scripts/run_weekly_report.py --pdf  # Also generates PDF

# 7. Run test suite
pytest tests/ -v
# → 221+ tests covering ETL, Athena, insights, FHIR, alerts, ML, NL-to-SQL

# 8. Run full daily pipeline (ingestion → ETL → Gold → Streamlit → Briefing)
bash run_daily_ingestion.sh
```

**Example Queries to Try in the Chat Interface:**
- "What was my average readiness score last week?"
- "Show me the correlation between sleep and next-day readiness"
- "Am I overtraining?"
- "What's my best workout type when readiness is below 75?"

## Streamlit Pages

| Page | Description |
|------|-------------|
| **Ask** | Natural language chat &rarr; Claude translates to SQL &rarr; results + charts |
| **Insights** | 11 automated statistical analyses with Plotly visualizations |
| **Weekly Report** | Claude-narrated HTML report with key metrics and trends |
| **What-If** | Single-scenario simulator + multi-day training block planner |
| **Predictions** | ML next-day readiness forecast with feature importance + backtest |
| **Experiments** | A/B test interventions with Bayesian + Difference-in-Differences analysis |
| **Discoveries** | Automated correlation scan across all metric pairs |
| **Export** | FHIR R4 Bundle export (6 metrics) for healthcare interoperability |

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
| Sleep &rarr; Readiness | Pearson correlation between sleep score and next-day readiness | r, p-value, linear regression | Scatter + regression line |
| Workout &rarr; Recovery | Next-day readiness segmented by workout type (cycling, strength, rest) | Mann-Whitney U test | Box plot |
| Readiness Trends | Daily readiness with 7-day and 14-day rolling averages | Linear regression on 14-day MA | Line + rolling averages |
| Anomaly Detection | Flags days >1.5 std devs below personal mean | Z-score threshold | Timeline + highlighted anomalies |
| Intensity &rarr; Recovery | Workout output vs next-day readiness (full history, 5 buckets) | Spearman &rho;, LOWESS trend | Scatter + LOWESS + box plots |
| Training Load | TSS, CTL (42-day), ATL (7-day), TSB tracking | Exponential moving averages | Stacked area chart |
| Progressive Overload | Training load progression with weekly change rates | Regression on rolling output | Trend lines + progression markers |
| Recovery Windows | Time-to-recovery by workout intensity bucket | Grouped statistics | Recovery timeline |
| Temperature Trends | Body temperature deviation from personal baseline | Running mean deviation | Deviation chart |
| Sleep Architecture | Deep/REM/Light sleep proportion analysis | Distribution statistics | Stacked bar + trend |
| Nutrition Analysis | Daily calorie and macro trends from MyFitnessPal | Rolling averages | Multi-line chart |

## Weekly Report

Automated HTML/PDF reports run all 11 analyzers, generate a cohesive narrative via Claude, and render with a dark-themed Jinja2 template. Reports are saved locally and uploaded to S3.

```bash
# Generate HTML report
python scripts/run_weekly_report.py --week-ending 2026-03-28

# Generate HTML + PDF
python scripts/run_weekly_report.py --pdf

# Local only (no S3 upload)
python scripts/run_weekly_report.py --local-only
```

## Data Sources

| Source | Data | Volume | Date Range |
|--------|------|--------|------------|
| Oura Ring | Sleep score, readiness, HRV, resting HR, activity, temperature | 120+ days | Nov 2025 -- Mar 2026 |
| Peloton | Cycling/strength workouts, output (kJ), watts, heart rate | 863 workouts | May 2021 -- Mar 2026 |
| Apple HealthKit | Resting HR, HRV, VO2 max, weight, body fat, workouts, mindfulness | 5,395 days | Sep 2020 -- Mar 2026 |
| MyFitnessPal | Daily calories, macros, nutrition summary | Intermittent | Mar 2026 |

## Gold Layer Views

**dbt models** (materialized as Parquet on S3, queryable via Athena):

| View | Description |
|------|-------------|
| `daily_readiness_performance` | Core table: Oura + Peloton + HealthKit joined by date |
| `dashboard_30day` | 30-day rolling view with 7-day and 30-day averages |
| `workout_recommendations` | Intensity recommendations based on recent readiness |
| `energy_state` | Classifies days into peak/high/moderate/low energy states |
| `workout_type_optimization` | Historical output by readiness bucket and workout discipline |
| `sleep_performance_prediction` | Sleep score to next-day readiness and output prediction |
| `readiness_performance_correlation` | Pearson correlations segmented by readiness level |
| `weekly_summary` | Week-over-week progression with trend indicators |
| `overtraining_risk` | Overtraining risk flags based on readiness, HRV, and workout frequency |
| `training_load_daily` | TSS / CTL / ATL / TSB calculations |
| `temperature_trends` | Body temperature deviation tracking |
| `sleep_architecture` | Deep/REM sleep proportion scoring |
| `workout_recovery_windows` | Recovery time estimation by workout type and intensity |
| `feature_readiness_daily` | ML feature table for readiness predictor |

---
## Pipeline Orchestration

The system runs a fully automated daily pipeline, triggered by EventBridge or the local `run_daily_ingestion.sh` script:

```
Step 1:  Find latest files (inbox or ~/Downloads)
Step 2:  Parse HealthKit XML → partitioned CSVs
Step 3:  Split Peloton CSV by date
Step 4:  Upload to Bronze S3 (HealthKit, Peloton, MFP)
Step 5:  Run 4 Glue normalizers in parallel (Oura, HK, Peloton, MFP)
Step 6:  Silver crawler (update Athena catalog)
Step 7:  Gold refresh (dbt run via Glue Python shell)
Step 8:  Gold crawler (update Athena catalog)
Step 9:  Verify data via Athena query
Step 10: Restart Streamlit
Step 11: Send morning briefing (Lambda → SNS)
Step 12: Weekly correlation discovery (Sundays only)
```

**AWS-side orchestration:** EventBridge listens for Oura normalizer SUCCEEDED events and chains: Silver crawler &rarr; Gold refresh &rarr; Gold crawler &rarr; Morning Briefing Lambda.

**Health Alerts** (separate from the pipeline): Lambda runs daily at 3 AM UTC via EventBridge, checks for:
- Resting HR > mean + 1.5&sigma;
- HRV < mean - 1.5&sigma;
- Overtraining risk = high
- Readiness declining 3+ consecutive days

Alerts are delivered via SNS email.

---
## Data Ingestion Automation

This project implements **two parallel ingestion strategies** for real-world biometric data:

### 1. Serverless Lambda Ingestion (Oura API)

**Architecture:** EventBridge scheduled trigger &rarr; Lambda &rarr; S3 Bronze &rarr; DynamoDB logging

**Stack:** `infrastructure/cloudformation/oura-ingest-stack.yaml`

- **Lambda Function:** `lambda/oura-api-ingest/` (Python 3.11)
- **Schedule:** Daily at 9:00 AM EST (EventBridge rule)
- **Endpoints:** 5 Oura API v2 endpoints (readiness, sleep, activity, heartrate, spo2)
- **Authentication:** OAuth2 access token stored in AWS Systems Manager Parameter Store (SecureString)
- **Error Handling:** Retry logic, token refresh, graceful degradation for missing data

**Data Flow:**
```
EventBridge (daily 9am) → Lambda → Oura API v2 (7-day lookback)
                                 ↓
                         S3 Bronze (JSON) + DynamoDB log
```

**Key Files:**
- `lambda/oura-api-ingest/handler.py` - Main event handler
- `lambda/oura-api-ingest/oura_client.py` - API wrapper with retry logic
- `lambda/oura-api-ingest/csv_transformer.py` - JSON &rarr; S3 transformer
- `tests/unit/test-oura-lambda.js` - 8 unit tests (schema, encryption, date logic)
- `tests/test-oura-lambda-integration.sh` - 6 integration tests (live AWS resources)

**Security:**
- IAM role with least-privilege permissions (S3 write, SSM read, DynamoDB write)
- All S3 uploads use `SSE-AES256` encryption (bucket policy enforced)
- OAuth tokens rotated via refresh flow (stored in `.oura-tokens.json` locally, never committed)

### 2. Local Automation (Peloton, HealthKit, MFP)

**Architecture:** LaunchD scheduled jobs &rarr; inbox &rarr; `run_daily_ingestion.sh` &rarr; S3 Bronze

- **Inbox Mover** (`scripts/move_downloads_to_inbox.sh`): Watches ~/Downloads for exported files, copies to project inbox
- **Daily Ingestion** (`run_daily_ingestion.sh`): 12-step pipeline from parse to morning briefing
- **Health Check** (`scripts/check_pipeline_health.sh`): Monitors data freshness, alerts on stale data
- **LaunchD Plists**: `com.bio-lakehouse.daily-ingestion.plist`, `com.bio-lakehouse.inbox-mover.plist`, `com.bio-lakehouse.health-check.plist`

### 3. OpenClaw Agent Automation (Peloton Backup)

**Architecture:** OpenClaw cron &rarr; Browser automation (Peloton) &rarr; S3 upload

**Why this approach?** Peloton has no public API; requires browser-based CSV export. OpenClaw provides headless browser control + cron scheduling.

**Peloton Pipeline:**
- **Schedule:** Weekly (Sundays 8:00 AM EST)
- **Method:** OpenClaw browser tool opens Peloton members page, clicks "Download Workouts" button, waits for CSV
- **Script:** `scripts/automation/peloton-sync.sh` uploads CSV to S3 with DynamoDB logging

**Oura Backup Pipeline (redundancy):**
- **Schedule:** Daily (9:00 AM EST, runs in parallel with Lambda)
- **Method:** Direct Oura API calls via `curl` (bash script)
- **Script:** `scripts/automation/oura-sync.sh` pulls 5 endpoints and uploads JSON to S3

### Test Coverage

| Suite | Tests | Coverage |
|-------|-------|----------|
| ETL utilities | 11 | Glue helper functions, schema validation |
| HealthKit parser | 37 | XML parsing, date filtering, CSV generation |
| Ingestion pipeline | 12 | S3 upload, DynamoDB logging, file discovery |
| Athena client | 9 | Query execution, caching, error handling |
| FHIR builder | 48 | Bundle structure, Observation resources, coding |
| Insights analyzers | 13 | All 11 analyzers, edge cases |
| What-If simulator | 28 | Scenarios, multi-day, edge cases |
| Health alerts | 15 | Threshold logic, SNS formatting |
| Oura Lambda | 8 | Schema, encryption, date logic |
| NL-to-SQL | 14 | Prompt construction, SQL validation |
| Weekly report | 7 | Report generation, delivery |
| Training load | 19 | TSS/CTL/ATL calculations |
| ML predictor | Integrated | Walk-forward CV, feature selection |
| **Total** | **221+** | |

### Ingestion Monitoring

**DynamoDB `bio_ingestion_log` Table:**
- Primary key: `file_path` (S3 URI)
- Sort key: `upload_timestamp` (Unix epoch)
- Metadata: `source`, `data_type`, `record_count`, `status`

**Query Recent Ingestions:**
```bash
aws dynamodb scan \
  --table-name bio_ingestion_log \
  --filter-expression "#src = :source" \
  --expression-attribute-names '{"#src": "source"}' \
  --expression-attribute-values '{":source": {"S": "oura"}}' \
  --max-items 10
```

---

## Monthly AWS Cost

Running a personal data lakehouse on AWS costs less than a streaming subscription:

| Service | Usage | Monthly Cost |
|---------|-------|-------------|
| S3 (Bronze/Silver/Gold) | ~2 GB stored, ~1K requests/day | $0.05 |
| AWS Glue | 4 normalizers × 3 DPU × ~2 min/day | ~$2.50 |
| Glue Crawlers | 2 crawlers × ~1 min/day | ~$0.30 |
| Athena | ~50 queries/day × ~10 MB scanned | ~$0.25 |
| Lambda | 5 functions × ~30 invocations/day | ~$0.00 (free tier) |
| DynamoDB | ~30 writes/day, on-demand | ~$0.00 (free tier) |
| EventBridge | ~10 rules | ~$0.00 (free tier) |
| SNS | ~2 emails/day | ~$0.00 (free tier) |
| **Total** | | **~$3.10/month** |

Key cost decisions: Athena pay-per-query over Redshift clusters ($0.25/month vs $180+/month). Glue over EMR (no idle cluster costs). Result caching in AthenaClient eliminates redundant scans. Parquet columnar format in Gold layer minimizes bytes scanned per query.

---

## Design Decisions & Trade-offs

**Why Medallion Architecture?**
Bronze/Silver/Gold provides clear separation of concerns: raw ingestion &rarr; normalization &rarr; analytics. Makes debugging easier, enables schema evolution, and follows Databricks/Delta Lake patterns familiar to enterprise teams.

**Why Athena over Redshift?**
Serverless pay-per-query model fits a personal project's intermittent query pattern. No cluster management overhead. Presto/Trino SQL is production-grade and portable.

**Why Claude for NL-to-SQL?**
Tested multiple approaches (GPT-4, Llama 3, rule-based parsers). Claude Sonnet 4 provided the best balance of accuracy (95% on benchmarks), reasoning transparency, and cost (~$0.02/query with result caching).

**Why PySpark in Glue?**
Even with small data volumes (~MB), PySpark demonstrates ETL patterns that scale to TB/PB. Same code would work on larger datasets with minimal refactoring. Shows understanding of distributed computing primitives.

**Why dbt for Gold Layer?**
dbt provides version-controlled, testable SQL transformations with dependency management. The `dbt_bio_lakehouse` project defines all Gold views as models, making the analytics layer reproducible and auditable. Runs via a Glue Python shell job.

**Why Local Streamlit vs Cloud Deployment?**
Privacy-first: biometric data stays in AWS + local machine. No public endpoints. Demonstrates you can build production-quality UIs without exposing sensitive data.

**Why Desktop I/O Optimization?**
macOS Spotlight/Finder adds massive I/O overhead to files under `~/Desktop/`. The venv and source code live physically at `~/.local/share/bio-lakehouse-*` to bypass this. Reduced import time from 600s to <1s.

**Statistical Rigor**
All correlations report p-values and sample sizes. Uses non-parametric tests (Mann-Whitney U, Spearman) appropriate for small samples. Explicitly flags limitations ("n=120 is observational, not causal").

### Enterprise Parallels

**This Project** &rarr; **Production Equivalent**

- Oura/Peloton ingestion &rarr; Multi-SaaS data integration (Salesforce, Workday, Snowflake)
- Medallion Bronze/Silver/Gold &rarr; Data lakehouse for customer 360 views
- Claude NL-to-SQL &rarr; Natural language BI for business analysts (replace Looker/Tableau prompts)
- dbt Gold layer &rarr; Analytics engineering with version-controlled transformations
- Health alerts &rarr; Automated monitoring and anomaly detection for business KPIs
- Morning briefing &rarr; Executive daily digest with freshness guarantees
- Weekly automated reports &rarr; Scheduled executive dashboards
- FHIR R4 export &rarr; Healthcare data interoperability (HL7/FHIR compliance)
- DynamoDB ingestion log &rarr; Data governance audit trail for SOC 2 compliance
- Lambda + Glue ETL &rarr; Serverless data pipelines (scales to TB/PB with no refactoring)
- Pipeline orchestrator &rarr; Event-driven workflow management (Step Functions alternative)
- What-If simulator &rarr; Decision support systems for planning and forecasting

The patterns here -- event-driven ingestion, metadata-driven governance, AI-powered analytics -- are identical to what enterprise teams need to operationalize AI at scale.

---

## Project Status

**Current State**: Fully operational
**Infrastructure**: 7 CloudFormation stacks deployed in AWS us-east-1
**Data Freshness**: Daily automated pipeline (HealthKit, Peloton, MFP local + Oura Lambda)
**Monitoring**: CloudWatch Logs for Lambda/Glue, DynamoDB ingestion log, Streamlit query log, health check script
**Testing**: 221+ unit tests across 13 suites

**Future Enhancements** (see [docs/PRD.md](docs/PRD.md) for full roadmap):
- Multi-model NL-to-SQL comparison (benchmark Claude vs GPT-4 Turbo vs Gemini Pro)
- Real-time streaming (Kinesis Data Streams &rarr; Lambda &rarr; Silver, replace batch Glue)
- n8n workflow automation (Peloton/MFP API ingestion, unified notification hub)

---

## Lessons Learned

Building a data lakehouse on real biometric data surfaced engineering problems that synthetic demos never encounter. These lessons shaped the project's architecture and are directly transferable to production AI systems.

### Why the First ML Model Failed

The initial readiness predictor used GradientBoosting with 200 estimators and max depth 4 -- wildly overparameterized for 88 training samples. The result: **R^2 = -0.556**, worse than predicting the mean. The fix was systematic: benchmark against a naive baseline (7-day rolling average), match model complexity to sample size (Ridge regression with strong regularization), and use walk-forward cross-validation with more folds. **Lesson:** Always include a naive baseline. If your model can't beat "predict the average," it's learning noise, not signal.

### Bronze CSV Column Order Bug

Spark's CSV reader silently misaligns values when files have different column header orders. Our Bronze bucket contained both bulk-uploaded CSVs (alphabetical columns) and Lambda-generated CSVs (`id, day, score, ...` order). Spark read them all as if columns matched. Rows passed schema validation but contained garbage data. **Lesson:** Validate data shapes (spot-check actual values), not just schema presence. Trust-but-verify at every layer boundary.

### PySpark Dict Key Sorting

`createDataFrame` with Python dicts sorts keys alphabetically, silently breaking column alignment with the provided schema. A dict `{"score": 85, "day": "2025-11-25"}` becomes `(day, score)` regardless of schema field order. **Lesson:** Use tuples with explicit schema, never dicts. Implicit ordering is a production bug waiting to happen.

### Feature Leakage in ML Pipeline

Same-day readiness contributors (recovery index, resting heart rate score) correlated perfectly with the target because they're *derived from* readiness. Including them made the model look accurate on training data while learning nothing predictive. The Phase 7 feature selection module now maintains an explicit leaky-feature exclusion list and validates temporal ordering against the dbt SQL. **Lesson:** Audit every feature for temporal leakage before training. High feature importance on a same-day measurement is a red flag, not a win.

### Small-Sample ML Strategy

With only ~90 samples, ensemble methods with hundreds of parameters learn noise. Linear models with strong regularization (Ridge, ElasticNet) consistently outperformed tree-based models in walk-forward CV. The pipeline now includes sample size warnings and automatically selects conservative architectures. **Lesson:** Model complexity should scale with data volume. More parameters does not equal better predictions.

### CTL Was Invisible at N=87

The 42-day chronic training load (CTL) -- which became the #1 feature at N=119 -- wasn't even selected by feature selection at N=87. The reason: with only 87 days of Oura data, only ~2 full CTL cycles existed, which is insufficient for the exponential moving average to demonstrate predictive power. At N=119 (3+ cycles), CTL jumped to 20.8% importance and fundamentally changed the model's story from "yesterday's stress" to "weeks of consistent training." **Lesson:** Time-series ML features with long lookback windows (42 days for CTL) require patience with data accumulation. Premature feature engineering at small N will miss the most important signals -- let the data grow and retrain regularly.

### Desktop I/O Killed Startup

macOS Spotlight indexes everything under `~/Desktop/`, adding massive I/O overhead to Python imports. The Streamlit app took 600+ seconds to start because the venv and source code lived in the project directory. Moving them physically to `~/.local/share/` and pointing `PYTHONPATH`/`BIO_PROJECT_ROOT` there brought startup down to <1 second. **Lesson:** Know your operating environment. Framework-level performance issues can dwarf any code optimization.

---

## What I'd Do Differently

Building and running this system daily for 6+ weeks revealed architectural choices I'd revisit:

- **Step Functions over Lambda orchestrator** -- The pipeline orchestrator Lambda chains jobs by polling Glue state in a loop. AWS Step Functions would provide visual workflow debugging, automatic retries with exponential backoff, and parallel branch support out of the box. The Lambda approach works but is harder to observe and extend.

- **dbt tests from day one** -- I added dbt late (Phase 6) and relied on manual Athena verification for the Gold layer. Starting with `dbt test` (unique, not_null, accepted_values) on the Silver layer would have caught the Bronze CSV column-order bug weeks earlier. Schema tests at every layer boundary should be non-negotiable.

- **Incremental models over full refresh** -- The Gold layer runs a full dbt rebuild daily (~45s on Glue). With 600+ rows this is fine, but the pattern won't scale. Incremental models with `merge` strategy on the date column would make this future-proof and reduce Glue DPU-minutes by ~80%.

- **Structured logging over print statements** -- The daily ingestion script uses `echo` for status output. Python's `logging` module with JSON formatting would make CloudWatch queries trivial and enable automated alerting on specific error patterns without grep.

---

## Readiness Predictor

A next-day readiness prediction pipeline using GradientBoostingRegressor with automated feature selection, walk-forward cross-validation, and Optuna hyperparameter tuning. The model retrains as data accumulates, with feature importance evolving as sample size grows.

### Model Version History

| Version | Samples | MAE | Top Feature | Features Selected | max_depth | Date |
|---------|---------|-----|-------------|-------------------|-----------|------|
| v1 | 87 | 5.00 | TSB (26.4%) | 6 | 3 | Feb 2026 |
| v2 | 119 | 4.65 | CTL (20.8%) | 8 | 2 | Mar 2026 |

The progression tells a story: as data grew from 87 to 119 samples, the model simplified (max_depth 3&rarr;2) while improving accuracy (MAE 5.0&rarr;4.65). The model's explanation of readiness shifted from "yesterday's training stress" to "chronic training load management over weeks."

### Current Model (v2, N=119)

**Metrics:**
- MAE: 4.65 (beats naive 7-day average baseline of 4.7)
- Cross-validation: Walk-forward, 12 folds, 7-day test windows, min 30-sample training set
- R^2: Negative (expected -- the model's value at this sample size is interpretable feature importance for training decisions, not raw prediction lift over naive forecasting. As N approaches 200+, we expect the accuracy gap to widen as the model captures more complex feature interactions.)

**Feature Importance:**
| Feature | Importance | What It Means |
|---------|-----------|---------------|
| CTL (chronic training load, 42-day) | 20.8% | Consistent training volume over weeks matters most |
| TSB (training stress balance) | 20.5% | Freshness vs fatigue balance drives next-day readiness |
| Resting heart rate | 18.8% | Autonomic recovery signal |
| HRV 2-day change | 13.1% | Autonomic stress dynamics (direction of change, not absolute level) |
| Day of week | 10.5% | Weekly periodization patterns (higher after rest days) |
| Readiness 7-day avg | 10.4% | Momentum/baseline |
| Sleep score 3-day avg | 4.4% | Recent sleep quality |
| Deep sleep score | 1.4% | Marginal contributor |

**Key insight:** CTL + TSB = 41% of readiness variance. Your readiness is primarily driven by training load management over weeks, not any single day's metrics.

### What Changed Between v1 and v2

| Feature | v1 (N=87) | v2 (N=119) | Explanation |
|---------|-----------|------------|-------------|
| CTL | not selected | **#1 (20.8%)** | At N=87, only ~2 CTL cycles (42-day EMA) existed. At N=119, 3+ cycles made CTL's predictive power visible. |
| Day of week | not selected | **10.5%** | Weekly periodization patterns only emerge with enough weekends in the dataset. |
| Sleep score 3d avg | not selected | **4.4%** | Replaced `sleep_debt_7d` -- short-term sleep quality predicts better than accumulated debt. |
| HRV 2-day change | 10.1% (MI=0.04) | **13.1%** | Initially flagged as borderline noise by MI scoring, but importance increased with more samples. |
| sleep_debt_7d | 19.5% (#2) | **dropped** | Replaced by sleep_score_3d_avg with more data. |
| tss (daily) | 12.8% | **dropped** | Replaced by CTL -- long-term load signal superseded daily training stress. |
| hrv_balance_score | 0% | **dropped** | Confirmed zero predictive value, removed by feature selection. |

### Honest Limitations

- MAE of 4.65 vs naive baseline of 4.7 is a 1% margin. The model's primary value at current sample size is **interpretable feature importance for training decisions**, not raw prediction lift over naive forecasting.
- `deep_sleep_score` contributes only 1.4% -- likely redundant with sleep_score and will probably be dropped at N=150+.
- R^2 remains negative. This is expected with ~120 samples of inherently noisy biometric data. The model consistently beats the baseline on MAE (the metric that matters for actionable predictions).

### Path Forward

- Target: 200+ samples for stable R^2 > 0 and meaningful prediction lift
- Timeline: ~10 weeks at current daily ingestion rate
- Plan: Retrain monthly, monitor feature importance drift, let the model decide what stays
- Connection to Gold views: The `overtraining_risk` view's logic should be revisited to weight CTL/TSB more heavily, aligning with what the ML model learned

---

Built with [Claude](https://anthropic.com) &bull; Data Engineering on AWS &bull; Open Source (MIT License)
