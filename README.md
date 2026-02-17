# Bio Lakehouse 🧬

A serverless health-metrics data lakehouse built on AWS, implementing a **medallion architecture** (Bronze → Silver → Gold) to unify biometric data from Oura Ring and Peloton into a single analytical layer.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│  Data Sources│     │    Bronze    │     │   Silver    │     │    Gold      │
│              │     │              │     │             │     │              │
│  Oura Ring   │────▶│  S3 (Raw)    │────▶│ S3 (Clean)  │────▶│ S3 (Enriched)│
│  Peloton     │     │  + DynamoDB  │     │ Glue ETL    │     │ Aggregated   │
│              │     │  Ingestion   │     │ Normalized  │     │ Readiness    │
└─────────────┘     │  Log         │     │             │     │ Scores       │
                    └──────────────┘     └─────────────┘     └──────────────┘
                          │                                         │
                    ┌─────┴─────┐                           ┌──────┴───────┐
                    │  Lambda   │                           │  Athena +    │
                    │  Trigger  │                           │  QuickSight  │
                    └───────────┘                           └──────────────┘
```

## Tech Stack

| Layer | Services |
|-------|----------|
| **Ingestion** | AWS Lambda (Python), S3 Event Notifications, DynamoDB |
| **Storage** | S3 (tiered: bronze/silver/gold) |
| **ETL** | AWS Glue (PySpark) — Oura & Peloton normalizers |
| **Query** | Amazon Athena |
| **Visualization** | Amazon QuickSight |
| **IaC** | AWS CloudFormation (per-layer stacks) |
| **Testing** | pytest + moto (AWS mocking) |

## Project Structure

```
bio-lakehouse/
├── infrastructure/
│   └── cloudformation/
│       ├── bronze-stack.yaml    # S3, Lambda, DynamoDB, IAM
│       ├── silver-stack.yaml    # Glue jobs, crawlers
│       └── gold-stack.yaml      # Aggregation layer
├── lambda/
│   └── ingestion_trigger/
│       └── handler.py           # S3 event → ingestion pipeline
├── glue/
│   ├── bio_etl_utils.py         # Shared PySpark utilities & schemas
│   ├── oura_normalizer.py       # Oura Ring data normalization
│   ├── peloton_normalizer.py    # Peloton workout normalization
│   └── readiness_aggregator.py  # Cross-source readiness scoring
├── athena/                      # Query definitions
├── quicksight/                  # Dashboard config
├── scripts/                     # Data splitting & upload utilities
├── tests/                       # Unit tests (moto-backed)
└── requirements.txt
```

## Data Pipeline

1. **Bronze (Raw):** Health data exports land in S3 via Lambda trigger. DynamoDB tracks ingestion metadata.
2. **Silver (Normalized):** Glue jobs clean, deduplicate, and normalize Oura/Peloton data into consistent schemas with PySpark.
3. **Gold (Enriched):** Aggregated readiness scores combining sleep, HRV, activity, and workout metrics. Queryable via Athena.

## Key Design Decisions

- **Medallion architecture** for clear data lineage and reprocessing capability
- **CloudFormation per layer** — independent deployment of bronze/silver/gold stacks
- **PySpark schemas** defined in shared utils for consistency across ETL jobs
- **High-recovery fabric** — stretch that doesn't bag out... wait, wrong project. Versioned S3 buckets with deletion protection for data durability.

## Local Development

```bash
pip install -r requirements.txt
pytest tests/
```

## Status

🟡 Active development — Bronze and Silver layers deployed, Gold layer + QuickSight dashboards in progress.
