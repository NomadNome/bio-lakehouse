#!/usr/bin/env python3
"""Clone every Athena view from one Gold database into another.

Used when standing up a new instance: the crawler creates the base table and
dbt builds its models, but the app-facing views (dashboard_30day,
sleep_performance_prediction, ...) live in the Gold database itself.

    python scripts/clone_views.py bio_gold bio_diego_gold \
        --results-bucket bio-lakehouse-diego-athena-results-069899605581

Fetches SHOW CREATE VIEW for each VIRTUAL_VIEW in the source DB, rewrites the
database qualifier, and executes CREATE OR REPLACE VIEW in the target DB.
"""

import argparse
import sys
import time

import boto3


def run_query(athena, sql, database, output):
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output},
    )["QueryExecutionId"]
    while True:
        state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]["State"]
        if state == "SUCCEEDED":
            return qid
        if state in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(QueryExecutionId=qid)[
                "QueryExecution"
            ]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"{state}: {reason}\n{sql[:200]}")
        time.sleep(2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source_db")
    ap.add_argument("target_db")
    ap.add_argument("--results-bucket", required=True)
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args()

    glue = boto3.client("glue", region_name=args.region)
    athena = boto3.client("athena", region_name=args.region)
    output = f"s3://{args.results_bucket}/view-clone/"

    views = [
        t["Name"]
        for page in glue.get_paginator("get_tables").paginate(
            DatabaseName=args.source_db
        )
        for t in page["TableList"]
        if t.get("TableType") == "VIRTUAL_VIEW"
    ]
    print(f"{len(views)} views in {args.source_db}: {', '.join(sorted(views))}")

    failures = []
    for name in sorted(views):
        try:
            qid = run_query(
                athena, f"SHOW CREATE VIEW {args.source_db}.{name}",
                args.source_db, output,
            )
            rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
            ddl = "\n".join(r["Data"][0].get("VarCharValue", "") for r in rows)
            ddl = ddl.replace(f"{args.source_db}.", f"{args.target_db}.").replace(
                f'"{args.source_db}".', f'"{args.target_db}".'
            )
            run_query(athena, ddl, args.target_db, output)
            print(f"  cloned: {name}")
        except Exception as e:
            failures.append((name, str(e).split(chr(10))[0]))
            print(f"  FAILED: {name}: {e}")

    if failures:
        print(f"\n{len(failures)} view(s) failed — likely reference tables the "
              "target instance doesn't have yet.")
        sys.exit(1)
    print("ALL VIEWS CLONED")


if __name__ == "__main__":
    main()
