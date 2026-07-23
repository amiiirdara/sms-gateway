# Load Test Report — Accept Path

**Tool:** [Grafana k6](https://k6.io/) v2.1.0  
**Script:** [`scripts/load-accept.js`](../scripts/load-accept.js)  
**Date:** 2026-07-23  
**Target stack:** Docker Compose local demo (`api-gateway` and dependents)

## Goal

Validate that the **message accept path** (`POST /v1/messages`) sustains a modest constant arrival rate with low latency and near-zero errors, under prepaid balance debit + Redis outbox write.

This is a **demo-scale** load test (not a 100M SMS/day capacity proof). It stresses API-gateway accept, not end-to-end operator delivery throughput.

## Environment

| Item | Value |
|---|---|
| Host OS | Windows 10 / 11 |
| Runtime | Docker Compose (full SMS Gateway stack) |
| API under test | `api-gateway` → host port **8080** |
| Tooling | k6 2.1.0 (`GrafanaLabs.k6`) |
| Base URL used | `http://[::1]:8080` (see [Host note](#host-note-port-8080)) |

Services exercised indirectly (after accept): outbox-relay → Kafka → dispatcher → operator-mock → billing / report-sink. The k6 script **only asserts HTTP 202 on accept**.

## Scenario

| Field | Value |
|---|---|
| Scenario name | `accept` |
| Executor | `constant-arrival-rate` |
| Arrival rate | **20 iterations/s** (`RATE`, default) |
| Duration | **30s** (`DURATION`, default) |
| Pre-allocated VUs | `max(10, RATE)` → 20 |
| Max VUs | `max(50, RATE * 3)` → 60 |
| Expected volume | ~600 accept iterations + setup requests |

### Thresholds (pass/fail)

| Metric | Condition |
|---|---|
| `http_req_failed` | `rate < 0.05` |
| `http_req_duration` | `p(95) < 1000` ms |
| `checks` | `rate > 0.95` |

### Check

- Response status **`202 Accepted`** for each `POST /v1/messages`.

## Path under test

```
setup()
  POST /v1/accounts          → 201  { accountId, apiKey }
  POST /v1/topups            → 200  (credit ≈ RATE × duration + headroom)

default iteration (× ~600)
  POST /v1/messages          → 202
    Authorization: Bearer <apiKey>
    Idempotency-Key: unique per iteration
    Body: { to: "09121234567", text: "k6-…", priority: "normal" }
```

**Not measured by this script:** reporting-api status polls, Express lane, campaigns, or dispatcher/operator latency SLAs.

## Execution

### Prerequisites

1. Stack up: `docker compose up -d --build`
2. k6 installed ([install docs](https://k6.io/docs/get-started/installation/))

### Command used

```powershell
$env:BASE_URL = 'http://[::1]:8080'
k6 run scripts/load-accept.js
```

### Optional knobs

```powershell
$env:BASE_URL = 'http://[::1]:8080'
$env:RATE = '50'
$env:DURATION = '1m'
k6 run scripts/load-accept.js
```

### Host note (port 8080)

On this machine, **Adobe Connect** binds `127.0.0.1:8080`, so IPv4 `localhost` / `127.0.0.1` does not reach Docker. Compose publishes on `0.0.0.0:8080` and `[::]:8080`; using **`http://[::1]:8080`** hits the gateway via IPv6. PowerShell `http://localhost:8080` also works (prefers IPv6).

## Results

All recorded runs: **exit code 0**, all thresholds **passed**.

### Run 1 — 2026-07-23 ~06:59 +03:30

| Metric | Value |
|---|---|
| Iterations | 600 |
| Checks passed | 600 / 600 (100%) |
| HTTP requests | 602 (incl. setup) |
| HTTP failure rate | **0.00%** |
| Throughput | ~20.05 req/s |
| `http_req_duration` avg / med / p(95) / max | 3.03 ms / 2.75 ms / **3.94 ms** / 39.91 ms |
| Iteration duration p(95) | 4.13 ms |
| Data sent / received | 195 kB / 118 kB |

### Run 2 — 2026-07-23 ~07:05 +03:30 (approved re-run)

| Metric | Value |
|---|---|
| Iterations | 601 |
| Checks passed | 601 / 601 (100%) |
| HTTP requests | 603 |
| HTTP failure rate | **0.00%** |
| Throughput | ~20.08 req/s |
| `http_req_duration` avg / med / p(95) / max | 2.77 ms / 2.6 ms / **3.85 ms** / 13.58 ms |
| Iteration duration p(95) | 3.88 ms |
| Data sent / received | 195 kB / 118 kB |

### Run 3 — 2026-07-23 (repeat)

| Metric | Value |
|---|---|
| Iterations | 601 |
| Checks passed | 601 / 601 (100%) |
| HTTP requests | 603 |
| HTTP failure rate | **0.00%** |
| Throughput | ~20.03 req/s |
| `http_req_duration` avg / med / p(95) / max | 7.25 ms / 6.51 ms / **9.77 ms** / 65.97 ms |
| Iteration duration p(95) | 10.71 ms |
| Data sent / received | 195 kB / 118 kB |

### Summary

| Run | Accepted checks | Failures | p(95) latency | Verdict |
|---|---|---|---|---|
| 1 | 600/600 | 0% | 3.94 ms | PASS |
| 2 | 601/601 | 0% | 3.85 ms | PASS |
| 3 | 601/601 | 0% | 9.77 ms | PASS |

Accept-path p(95) stayed **well under 1s** (threshold) across runs; variance in run 3 is still sub-10 ms at p(95).

## Interpretation

- At **20 accepts/s** for 30s, the local Compose `api-gateway` reliably returns **202** with **millisecond-class** latency.
- Setup correctly provisions a tenant and enough prepaid credit so rejects from insufficient funds do not pollute the run.
- This does **not** claim operator-side or Kafka consumer capacity; those stages run asynchronously after accept.

## Reproducing

```powershell
docker compose up -d
$env:BASE_URL = 'http://[::1]:8080'   # or http://localhost:8080 if nothing else owns 127.0.0.1:8080
k6 run scripts/load-accept.js
```
