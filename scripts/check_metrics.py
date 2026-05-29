#!/usr/bin/env python3
"""
CI metrics validation script (HW7 point 8).

Queries Prometheus API after a load test run and verifies SLI/SLO thresholds.
Exits with code 1 if any threshold is violated so the CI job fails.

Usage:
  python scripts/check_metrics.py [--prometheus-url http://localhost:9090]

Thresholds (based on SLI/SLO defined in README):
  SLI 1  API availability          >= 99.5%   (failure threshold: < 95%)
  SLI 2  HTTP p95 latency          <  500ms   (failure threshold: > 1000ms)
  SLI 3  Event processing p95      <  1000ms  (failure threshold: > 2000ms)
  SLI 4  DLQ rate                  <  1%      (failure threshold: > 5%)
"""

import argparse
import sys
import time

import requests

PROMETHEUS_URL = "http://localhost:9090"

# (name, promql, operator, threshold_value, unit)
CHECKS = [
    (
        "API availability (5m)",
        "sum(rate(http_requests_total{status=~'2..'}[5m])) / (sum(rate(http_requests_total[5m])) + 0.0001)",
        ">=",
        0.95,
        "ratio",
    ),
    (
        "HTTP p95 latency (5m)",
        "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
        "<=",
        1.0,
        "seconds",
    ),
    (
        "Event processing p95 (5m)",
        "histogram_quantile(0.95, rate(event_processing_duration_seconds_bucket[5m]))",
        "<=",
        2.0,
        "seconds",
    ),
    (
        "DLQ rate (5m)",
        "rate(dlq_events_total[5m]) / (rate(events_processed_total[5m]) + rate(dlq_events_total[5m]) + 0.0001)",
        "<=",
        0.05,
        "ratio",
    ),
]


def query(prometheus_url: str, promql: str) -> float | None:
    url = f"{prometheus_url}/api/v1/query"
    resp = requests.get(url, params={"query": promql}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("data", {}).get("result", [])
    if not results:
        return None
    return float(results[0]["value"][1])


def check(value: float, operator: str, threshold: float) -> bool:
    if operator == ">=":
        return value >= threshold
    if operator == "<=":
        return value <= threshold
    if operator == ">":
        return value > threshold
    if operator == "<":
        return value < threshold
    raise ValueError(f"Unknown operator: {operator}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prometheus-url", default=PROMETHEUS_URL)
    parser.add_argument("--wait", type=int, default=0,
                        help="Seconds to wait before querying (allow metrics to stabilise)")
    args = parser.parse_args()

    if args.wait:
        print(f"Waiting {args.wait}s for metrics to stabilise...")
        time.sleep(args.wait)

    failures = []
    print(f"\n{'='*60}")
    print(f"  Prometheus SLI/SLO Check — {args.prometheus_url}")
    print(f"{'='*60}")

    for name, promql, operator, threshold, unit in CHECKS:
        try:
            value = query(args.prometheus_url, promql)
        except Exception as exc:
            print(f"  [ERROR] {name}: query failed — {exc}")
            failures.append(name)
            continue

        if value is None:
            print(f"  [SKIP ] {name}: no data (metrics not yet populated)")
            continue

        passed = check(value, operator, threshold)
        status = "PASS" if passed else "FAIL"
        symbol = "✓" if passed else "✗"

        if unit == "ratio":
            display = f"{value*100:.2f}% {operator} {threshold*100:.1f}%"
        elif unit == "seconds":
            display = f"{value*1000:.1f}ms {operator} {threshold*1000:.0f}ms"
        else:
            display = f"{value:.4f} {operator} {threshold}"

        print(f"  [{status}] {symbol} {name}: {display}")
        if not passed:
            failures.append(name)

    print(f"{'='*60}\n")

    if failures:
        print(f"FAILED checks ({len(failures)}): {', '.join(failures)}")
        sys.exit(1)

    print(f"All {len(CHECKS)} checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
