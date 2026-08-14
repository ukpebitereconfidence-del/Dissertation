# Pilot Protocol

The pilot determines the design parameters that cannot responsibly be assumed, and
validates the harness. **It is not intended to produce findings**, so it runs every cell
shape briefly rather than a few cells fully.

```bash
./scripts/validate_env.sh --json results/environment.json   # must pass first
cd servers && node selftest.js && cd ..                      # must pass second
python3 orchestrator/run_matrix.py --pilot --execute
python3 orchestrator/analyse.py --results results --out analysis/pilot
```

Eight questions, each with the decision it governs and the criterion for answering it.
Record every answer — several become sentences in Chapter 3 and numbers in Chapter 4.

---

### P0 · Is the host clock stable? (do this before anything else)
**Decision:** whether latency measured on this host means anything.
**Method:** run any short cell and read `clock.maxAbsDriftUs` from the probe JSON.
**Criterion:** under ~1,000 us. Values in the hundreds of thousands mean
CLOCK_REALTIME is being stepped during runs.
**Known cause on WSL2:** `systemd-timesyncd` running inside the distro while WSL2
also syncs the VM clock from the Windows host. Two independent correctors fight
and produce repeated ~950 ms steps. Fix:
```bash
sudo systemctl disable --now systemd-timesyncd   # NTP service -> inactive
```
Measured on the development host: `maxAbsDriftUs` fell from 951,447 to -30.3.
**Note:** probe latency is measured on CLOCK_MONOTONIC and is immune to this
either way. What the fix restores is k6's realtime latency as an independent
cross-check -- which is worth having, because agreement between two differently
clocked instruments is the best available evidence that the measurement is sound.
Observed after the fix: probe p50 809.5 us against k6 median 823.0 us.

### P1 · Does the environment hold at the highest tier?
**Decision:** whether the sweep may begin at all.
**Method:** run `ws` and `poll-short` at 1,000 clients. Watch `connections_active` on
`/stats` and `rtb_connect_failures` in the k6 summary.
**Criterion:** 1,000 connections established and sustained; zero connect failures; no
descriptor, ephemeral-port or accept-queue exhaustion.
**If it fails:** stop. Re-run `validate_env.sh`. A ceiling here is a configuration default
masquerading as an architectural limit (Kegel, 2014) — the single most likely way to
publish a confidently wrong result.

### P2 · Is the load generator the constraint, not the server?
**Decision:** whether each tier is driven from one generator container or several.
**Method:** `docker stats` on the `k6` container at the 1,000-client tier of **every**
workload; the dashboard workload is the binding case.
**Criterion:** generator CPU **< 70 %**. Above that, the figures describe the generator.
**If it fails:** shard the tier across additional k6 containers with `CLIENTS` divided
between them, and record that you did.

### P3 · How long is warm-up?
**Decision:** the discard boundary in §3.7.1 — currently a default of 30 s, to be replaced
by a measured value.
**Method:** run 5 minutes with `--warmup 0`; plot probe latency and `process_rss_bytes`
against elapsed time (Grafana, or bin the k6 JSON output by timestamp).
**Criterion:** the elapsed time after which median latency and RSS both stop trending.
V8's JIT and heap growth make the first tens of seconds unrepresentative.
**Then:** set `WARMUP_S` to that value plus a margin, for all arms equally.

### P4 · How long must the measurement window be?
**Decision:** confirms or revises the 300 s / 600 s windows per workload.
**Method:** compute running p95 and p99 over the probe sample; find where they stabilise.
Check `probe_samples` per run.
**Criterion:** p99 needs ≳ 1,000 samples to be estimable; the 10-client notification cell
is the binding case (1 Hz × 10 clients = 10 msg/s → 600 s gives ~6,000).
**If thin:** lengthen that workload's window. `analyse.py` flags cells under 1,000 samples.

### P5 · How variable is a run?
**Decision:** whether 10 replications deliver the intended precision (§3.7.3 undertakes to
verify this empirically).
**Method:** 5 replications of three representative cells; read the `_cv` columns in
`descriptives.csv`.
**Criterion:** CV of run-level p95 ≲ 0.15. Above that, 10 runs will not separate close
arms and the count must rise for that metric.
**Record:** the CVs. They justify the replication count in writing.

### P6 · Is saturation reached?
**Decision:** whether the study can answer Research Question 3 at all.
**Method:** container CPU at the 1,000-client tier for each workload.
**Criterion:** at least one cell must approach saturation and show the knee of the curve.
**If no cell does:** raise the dashboard publication rate (`SERVER_RATE_HZ`) until it does,
and **report the revised rate**. Without this the study reproduces the exact limitation
Appelqvist & Örnmyr (2017) identify in their own work — a 4 % peak, no scaling curve, no
graceful-degradation comparison (Welsh, Culler & Brewer, 2001).

### P6b · Does short polling show a uniform latency distribution?
**Decision:** whether the polling arm is measuring the mechanism or an artefact.
**Method:** read the probe's latency percentiles for any matched-interval
short-polling cell.
**Criterion:** latency should be roughly UNIFORM over [0, poll interval]:
p50 near interval/2, p95 near 0.95x interval, sd near interval/sqrt(12) (= 289 ms
for a 1,000 ms interval), and samples spanning the whole range.
**If instead you see a narrow band** (e.g. p50 268 ms with sd 2 ms), every client
is polling in the same phase relative to the publication tick and the run is
measuring the arbitrary moment it started, not the architecture. Both the load
generator and the probe stagger each client's first poll uniformly across one
interval for this reason; a narrow band means that stagger is not taking effect.
**Measured after the fix:** p50 482 ms, p95 956 ms, sd 301 ms against theoretical
500 / 950 / 289. Before it, the probe and k6 reported 268 ms and 567 ms for the
same run purely because they began polling at different instants.

### P6c · Do the probe and k6 agree on latency?
**Decision:** whether the probe is an unbiased sample of the client population.
**Method:** compare the probe's p50 against k6's median for the same run.
**Criterion:** agreement within roughly a millisecond for the persistent arms.
Observed on WebSocket/SSE after the clock fix: probe 809.5 us vs k6 823.0 us.
**If the probe reads systematically HIGHER or LOWER**, client position in the
fan-out order is acting as a per-client latency offset. Delivery to n
subscribers is serial, so a fixed iteration order makes the last client wait for
the n-1 sends before it -- tens of milliseconds at the 1,000-client tier. Because
the probes all connect at about the same moment, they occupy a contiguous band of
positions and would sample one end of that distribution.
The Hub rotates its fan-out start position on every publication (and the poll
server rotates its long-poll wake order) so that expected latency is equal for
all subscribers and any subset is unbiased. Observed before the rotation: probe
p50 5.0 ms against k6 median 1.34 ms on long polling at 20 clients.
**Note:** rotation does not remove the serialisation cost, which is real and is
part of what the study measures. It distributes that cost evenly rather than
concentrating it on whichever clients happen to be instrumented.

### P7 · Is the harness fair?
**Decision:** whether any run is admissible.
**Method:** the integrity report at the top of `analyse.py` output.
**Criterion (all four):** `missed_ticks` = 0; delivered messages reconcile with published;
zero sequence gaps; zero negative latencies.
**If it fails:** a harness that under-delivers silently produces plausible latency figures
from a broken experiment. Fix before proceeding — do not exclude and continue.

### P8 · Does the harness agree with published measurement?
**Decision:** whether the byte accounting is trustworthy.
**Method:** compare `bytes_per_message` against Appelqvist & Örnmyr's (2017) directly
measured figures at the notification workload:

| Arm | Their measurement | Expect |
|---|---|---|
| short polling | 281 B request + 191 B response | ~472 B/msg + payload |
| long polling | 273 B + 191 B | ~464 B/msg + payload |
| SSE | 8 B per message | payload + ~8 B |
| WebSocket | 2 B per message | payload + 2–4 B |

**Criterion:** agreement within a small margin.
**Why this matters as method:** §2.5.1 documents a vendor header-size calculation — with an
arithmetic error and a ~900 B assumed header — propagating uncredited through several
peer-reviewed papers, where measurement later showed ~200 B. Validating against a figure
*someone else measured* rather than one this study *assumes* is a guard against joining
that lineage.

---

## Recording the answers

Create `results/pilot_findings.md` with one line per question: the value measured, the
parameter it sets, and the date. These feed directly into Chapter 3 §3.7.1 (windows),
§3.7.3 (replication count and execution time), §3.5.3 (generator headroom) and Chapter 4's
implementation narrative.

Re-run `validate_env.sh` immediately before the full sweep. Host state changes.
