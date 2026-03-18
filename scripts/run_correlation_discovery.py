#!/usr/bin/env python3
"""
Correlation Discovery Engine - CLI Runner

Usage:
    python scripts/run_correlation_discovery.py                   # default 180-day lookback
    python scripts/run_correlation_discovery.py --lookback 365    # 1 year
    python scripts/run_correlation_discovery.py --local-only      # skip S3, save locally
    python scripts/run_correlation_discovery.py --min-rho 0.3     # stricter filter
"""

import argparse
import sys

from insights_engine.core.athena_client import AthenaClient
from insights_engine.insights.correlation_discovery import CorrelationDiscoveryEngine
from insights_engine.insights.discovery_persistence import DiscoveryStore


def main():
    parser = argparse.ArgumentParser(description="Run Correlation Discovery Engine")
    parser.add_argument("--lookback", type=int, default=180, help="Lookback days (default: 180)")
    parser.add_argument("--min-rho", type=float, default=0.25, help="Min abs(rho) threshold (default: 0.25)")
    parser.add_argument("--local-only", action="store_true", help="Skip S3 upload, save locally only")
    args = parser.parse_args()

    print(f"Correlation Discovery Engine")
    print(f"  Lookback: {args.lookback} days")
    print(f"  Min rho:  {args.min_rho}")
    print(f"  Mode:     {'local-only' if args.local_only else 'S3 + local'}")
    print()

    athena = AthenaClient()
    engine = CorrelationDiscoveryEngine(athena)
    store = DiscoveryStore()

    # Load prior results for new-finding detection
    prior = None
    if not args.local_only:
        try:
            prior = store.load_latest()
            if prior:
                print(f"Loaded prior results from {prior.run_date}")
        except Exception as e:
            print(f"Could not load prior results: {e}")

    # Run discovery
    print("Scanning correlations...")
    result = engine.discover(
        lookback_days=args.lookback,
        min_rho=args.min_rho,
        prior_results=prior,
    )

    # Print summary
    print()
    print(f"Data range: {result.data_range_start} to {result.data_range_end} ({result.total_rows} rows)")
    print(f"Pairs tested: {result.pairs_tested}")
    print(f"Significant correlations: {len(result.correlations)}")
    print(f"Threshold effects: {len(result.thresholds)}")
    print()

    # Top correlations
    if result.correlations:
        print("=== Top 5 Correlations ===")
        for i, c in enumerate(result.correlations[:5], 1):
            new_badge = " [NEW]" if c.is_new else ""
            lag_text = f" (lag {c.lag}d)" if c.lag > 0 else ""
            print(f"  {i}. {c.narrative}{lag_text}{new_badge}")
            print(f"     rho={c.rho:+.3f}  p={c.p_corrected:.4f}  n={c.n_samples}  strength={c.strength}")
        print()

    # Top thresholds
    if result.thresholds:
        print("=== Top 3 Threshold Effects ===")
        for i, t in enumerate(result.thresholds[:3], 1):
            new_badge = " [NEW]" if t.is_new else ""
            print(f"  {i}. {t.narrative}{new_badge}")
            print(f"     delta={t.delta:+.1f}  p={t.p_value:.4f}  n_above={t.n_above}  n_below={t.n_below}")
        print()

    # Top narrative
    if result.top_finding_narrative:
        print(f"Top finding: {result.top_finding_narrative}")
        print()

    # Save locally always
    local_path = store.save_local(result)
    print(f"Local: {local_path}")

    # Save to S3 unless local-only
    if not args.local_only:
        try:
            s3_uri = store.save(result)
            print(f"S3:    {s3_uri}")
        except Exception as e:
            print(f"S3 upload failed: {e}", file=sys.stderr)

    print("Done.")


if __name__ == "__main__":
    main()
