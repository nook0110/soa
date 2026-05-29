/**
 * k6 load test for the Warehouse Consumer HTTP API.
 *
 * Scenarios:
 *   - Ramp up to 10 VUs over 10s, sustain 30s, ramp down 10s (total ~50s)
 *   - Hits /health and /metrics endpoints
 *
 * Thresholds (CI will fail if violated):
 *   - http_req_failed < 1%          (error rate SLO)
 *   - http_req_duration p(95) < 500ms  (latency SLO)
 *   - http_req_duration p(99) < 1000ms (latency failure threshold)
 *
 * Run locally:
 *   k6 run load-tests/load_test.js
 *
 * Run in CI (env var CONSUMER_URL overrides default):
 *   k6 run --env CONSUMER_URL=http://localhost:8000 load-tests/load_test.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const errorRate = new Rate("custom_error_rate");
const healthLatency = new Trend("health_endpoint_latency");

const CONSUMER_URL = __ENV.CONSUMER_URL || "http://localhost:8000";

export const options = {
  scenarios: {
    warehouse_load: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: 10 },  // ramp up
        { duration: "30s", target: 10 },  // sustain
        { duration: "10s", target: 0 },   // ramp down
      ],
      gracefulRampDown: "5s",
    },
  },

  thresholds: {
    // SLO: error rate < 1%
    http_req_failed: [{ threshold: "rate<0.01", abortOnFail: true }],
    // SLO: p95 latency < 500ms
    http_req_duration: [
      { threshold: "p(95)<500", abortOnFail: false },
      // Failure threshold: p99 < 1000ms  (system considered broken if exceeded)
      { threshold: "p(99)<1000", abortOnFail: true },
    ],
    // Custom metrics
    custom_error_rate: ["rate<0.01"],
  },
};

export default function () {
  // ── Health check ────────────────────────────────────────────────────────
  const healthRes = http.get(`${CONSUMER_URL}/health`, {
    tags: { name: "health" },
    timeout: "5s",
  });

  const healthOk = check(healthRes, {
    "health status is 200 or 503": (r) => r.status === 200 || r.status === 503,
    "health has status field":     (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.status !== undefined;
      } catch {
        return false;
      }
    },
  });

  healthLatency.add(healthRes.timings.duration);
  errorRate.add(!healthOk);

  sleep(0.5);

  // ── Metrics scrape ──────────────────────────────────────────────────────
  const metricsRes = http.get(`${CONSUMER_URL}/metrics`, {
    tags: { name: "metrics" },
    timeout: "5s",
  });

  check(metricsRes, {
    "metrics status is 200":                (r) => r.status === 200,
    "metrics contains http_requests_total": (r) => r.body.includes("http_requests_total"),
    "metrics contains consumer_lag":        (r) => r.body.includes("consumer_lag"),
  });

  sleep(0.5);
}

export function handleSummary(data) {
  // Print a compact summary to stdout for CI artifact capture
  const p95 = data.metrics.http_req_duration
    ? data.metrics.http_req_duration.values["p(95)"]
    : "N/A";
  const p99 = data.metrics.http_req_duration
    ? data.metrics.http_req_duration.values["p(99)"]
    : "N/A";
  const errRate = data.metrics.http_req_failed
    ? (data.metrics.http_req_failed.values.rate * 100).toFixed(2)
    : "N/A";

  console.log(`
=== Load Test Summary ===
  Requests:    ${data.metrics.http_reqs ? data.metrics.http_reqs.values.count : "N/A"}
  Error rate:  ${errRate}%   (threshold: <1%)
  p95 latency: ${typeof p95 === "number" ? p95.toFixed(1) : p95}ms  (threshold: <500ms)
  p99 latency: ${typeof p99 === "number" ? p99.toFixed(1) : p99}ms  (threshold: <1000ms)
=========================
`);

  return {
    stdout: JSON.stringify(data, null, 2),
  };
}
