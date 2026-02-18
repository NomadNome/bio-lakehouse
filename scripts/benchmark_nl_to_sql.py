#!/usr/bin/env python3
"""
Bio Insights Engine - NL-to-SQL Benchmark Harness

Runs all 10 PRD benchmark questions end-to-end and reports results.

Usage:
    ANTHROPIC_API_KEY=sk-ant-... python scripts/benchmark_nl_to_sql.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from insights_engine.core.athena_client import AthenaClient
from insights_engine.core.nl_to_sql import NLToSQLEngine
from insights_engine.core.query_log import QueryLog

BENCHMARK_QUESTIONS = [
    "What was my average readiness score last week?",
    "Show my sleep duration trend over the past 30 days",
    "Which workout type gives me the best next-day readiness?",
    "What's my average HRV on days after cycling vs strength training?",
    "How many workouts did I do in January?",
    "What's the correlation between my sleep score and readiness?",
    "Show me days where my readiness dropped below 70",
    "What's my average Peloton output for cycling workouts?",
    "Compare my weekday vs weekend sleep duration",
    "What was my best readiness week and what did I do differently?",
]


def main():
    athena = AthenaClient()
    engine = NLToSQLEngine(athena)
    log = QueryLog()

    total = len(BENCHMARK_QUESTIONS)
    passed = 0
    failed = 0
    errors = []

    print(f"Running {total} benchmark questions...\n")
    print(f"{'#':<4} {'Status':<8} {'Time':>7} {'Rows':>6} {'Conf':>6}  Question")
    print("-" * 90)

    for i, question in enumerate(BENCHMARK_QUESTIONS, 1):
        start = time.time()
        try:
            result = engine.ask(question)
            elapsed_ms = result.execution_time_ms

            if result.error:
                status = "FAIL"
                failed += 1
                errors.append((i, question, result.error))
                log.log(
                    question=question, sql=result.sql,
                    execution_time_ms=elapsed_ms, row_count=0,
                    confidence=result.confidence, success=False,
                    error=result.error,
                )
            else:
                status = "PASS"
                passed += 1
                log.log(
                    question=question, sql=result.sql,
                    execution_time_ms=elapsed_ms, row_count=result.row_count,
                    confidence=result.confidence, success=True,
                )

            print(
                f"Q{i:<3} {status:<8} {elapsed_ms:>6}ms {result.row_count:>5} "
                f"{result.confidence:>5.0%}  {question[:60]}"
            )

        except Exception as e:
            elapsed_ms = int((time.time() - start) * 1000)
            status = "ERROR"
            failed += 1
            errors.append((i, question, str(e)))
            log.log(
                question=question, execution_time_ms=elapsed_ms,
                success=False, error=str(e),
            )
            print(f"Q{i:<3} {status:<8} {elapsed_ms:>6}ms {'—':>5} {'—':>5}  {question[:60]}")

    # Summary
    print("-" * 90)
    print(f"\nResults: {passed}/{total} passed, {failed}/{total} failed")
    print(f"Pass rate: {passed/total:.0%} (target: 70%)")

    if errors:
        print(f"\nFailures:")
        for num, q, err in errors:
            print(f"  Q{num}: {q}")
            print(f"       {err[:200]}")

    # Return exit code
    target = 7  # PRD: 7/10 must pass
    if passed >= target:
        print(f"\nBENCHMARK PASSED ({passed}/{total} >= {target}/{total})")
        return 0
    else:
        print(f"\nBENCHMARK FAILED ({passed}/{total} < {target}/{total})")
        return 1


if __name__ == "__main__":
    sys.exit(main())
