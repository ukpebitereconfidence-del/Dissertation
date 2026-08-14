# realtime-bench

Reproducible harness for **Performance Benchmarking of Real-Time Messaging Architectures:
WebSocket vs Server-Sent Events vs HTTP Polling** (MSc Computing, Edinburgh Napier).

Four experimental arms across three architectural families, three workloads, three
concurrency tiers, ten replications. Framework-free Node.js servers, containerised,
driven by k6 with a microsecond-resolution probe, analysed with non-parametric statistics
at run-level granularity.

---

## Quick start

```bash
# 0. Verify the environment BEFORE anything else. Fails closed.
#    Run it where the measurement happens -- NOT in Git Bash / MSYS / macOS
#    shell, which is an emulation layer your containers never touch.
./scripts/validate_in_container.sh --json results/environment.json   # authoritative
# or, on native Linux / inside WSL2:
./scripts/validate_env.sh --json results/environment.json

# 1. Correctness gate: all four arms, including bidirectional chat latency.
cd servers && npm install && node selftest.js && cd ..

# 2. Build images (the k6 image compiles a custom binary with xk6-sse; takes a few minutes)
docker compose --profile ws --profile load build

# 3. Inspect the design and its time budget
python3 orchestrator/run_matrix.py --plan

# 4. Pilot (short runs, every cell shape) -- determines parameters, produces no findings
python3 orchestrator/run_matrix.py --pilot --execute
python3 orchestrator/analyse.py --results results --out analysis/pilot

# 5. Full sweep (~64 h; resumable)
python3 orchestrator/run_matrix.py --execute
python3 orchestrator/analyse.py --results results --out analysis/final
```

Observability during a run:

```bash
docker compose --profile obs up -d      # cAdvisor + Prometheus + Grafana on :3000
```

---

## Layout

```
servers/
  common/       shared by all four arms -- the code that is NOT the independent variable
    config.js       every confound-relevant knob, resolved once, exposed via /config
    workloads.js    the three workload profiles (chat / notification / dashboard)
    payload.js      exact-size envelopes with integer-microsecond publication stamps
    broadcaster.js  deadline-scheduled publisher; records missed ticks
    hub.js          subscriber registry, room fan-out, client-originated ingress
    telemetry.js    Prometheus exposition, hand-rolled to keep observer effect equal
    control.js      /health /config /metrics /stats /reset -- identical on every arm
  ws/server.js    ARM 1  node:http + ws (RFC 6455 framing only)
  sse/server.js   ARM 2  node:http alone
  poll/server.js  ARMS 3 & 4  short and long polling from one binary
  probe.js        microsecond-resolution latency instrument
  selftest.js     correctness gate -- run before any pilot

loadgen/
  Dockerfile      builds a custom k6 with xk6-sse (k6 has no native SSE support)
  scripts/
    lib/common.js all metric definitions and the latency computation, shared
    ws.js sse.js poll.js

orchestrator/
  run_matrix.py   design matrix, randomised order, resumable execution
  analyse.py      Kruskal-Wallis -> Mann-Whitney/Holm -> A12 -> ratio CI

scripts/validate_env.sh   pre-flight kernel/clock validation, fails closed
observability/            Prometheus + cAdvisor + Grafana
docs/PILOT.md             the eight pilot questions and their decision rules
results/                  per-run manifests, k6 output, probe output
```

---

## The five design decisions that matter

**1. Only the send path differs between arms.** Configuration, workload generation,
subscriber registry, fan-out, byte accounting and telemetry are one shared module set
imported unchanged by all four servers. In `ws/server.js` the send path is one statement;
in `sse/server.js` it is two. That difference *is* the independent variable.

**2. The clock problem is dissolved, not solved.** Mackovic (2025) abandoned one-way
latency measurement because no synchronisation strategy was precise enough. Containers on
one host share one kernel clock, so the inter-clock offset is zero by construction rather
than reduced to a tolerance. `validate_env.sh` verifies the clock is monotonic and
non-retrograde and measures its granularity (~45 ns on the dev host).

**3. Two instruments, because one is not enough.** k6's JS clock is quantised to 1 ms.
The WebSocket-vs-SSE median difference is ~0.2 ms. Quantisation is unbiased for the mean
but distorts percentiles, and percentiles are the headline statistic. So k6 drives the
load and owns throughput/errors/connections; the probe clients own the latency
distribution at microsecond resolution.

**4. The unit of analysis is the run, not the message.** A dashboard run at 1,000 clients
delivers ~3M messages. Testing across messages is pseudoreplication and returns p≈0 for
noise. Each run reduces to one value per metric; n = 10 replications per arm.

**5. A12 is not a magnitude.** Because per-run percentiles are precise, A12 saturates at
1.0 for differences of a few per cent. Every comparison reports four quantities, read in
order: `p_holm` (is it real?), `A12` (is it consistent?), median ratio + CI (how large?),
perceptual band (does it matter?).

---

## Fairness details that are easy to get wrong

| Detail | Why it matters |
|---|---|
| `TCP_NODELAY` on all arms | Nagle + delayed ACK injects ~40 ms — an order of magnitude above the effect being measured |
| Polling sleeps the *remainder* of its interval | Sleeping a fixed interval after a slow response polls a loaded server *less often*, flattering it |
| Compression off everywhere | Bytes-on-wire is a dependent variable |
| Keep-alive **on** for polling | The realistic deployment; disabling it inflates polling's cost as a config artefact |
| Deadline-scheduled publisher | Otherwise a saturated arm silently publishes slower, i.e. changes its own workload |
| Room size fixed at 20 across tiers | Otherwise chat fan-out grows as O(n²) and the 1,000-tier measures fan-out, not connections |
| Server pinned to 1 core | Makes saturation reachable within 1,000 clients; Appelqvist & Örnmyr peaked at 4% CPU and called it their principal limitation |
| Polling writes once to a shared log | Modelling it as per-client push would import the persistent arms' cost structure |
| Heap bounds pinned identically | Appelqvist & Örnmyr could not compare memory at all because GC differed between runs |
| Randomised run order, seeded | A 64 h sweep drifts; nested order would align drift with a factor |

---

## Interpreting the output

`analysis/runs.csv` — one row per run. `descriptives.csv` — medians, IQR, bootstrap CIs,
CV per cell. `significance_tests.csv` — every pairwise comparison with unadjusted and
Holm-adjusted p, A12 and magnitude, median ratio with CI, and practical classification.

The integrity report runs first and prints before any statistic. Treat it as a gate:
missed publication ticks, negative latencies, sequence gaps or thin sample counts mean
the affected cells measured something other than what was intended.
