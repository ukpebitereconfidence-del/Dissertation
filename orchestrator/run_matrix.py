#!/usr/bin/env python3
"""
EXPERIMENT ORCHESTRATOR

Generates the design matrix, randomises run order, executes each run in a fresh
container, and archives everything needed to reconstruct the run.

Three design decisions carry methodological weight:

1. RANDOMISED RUN ORDER, seeded.
   The sweep takes tens of hours. Over that span the host drifts -- thermally,
   through page-cache state, through background daemon activity. Executing the
   matrix in nested-loop order would align that drift with a factor: whichever
   arm ran last would carry the accumulated drift as a systematic bias
   indistinguishable from an architectural effect. Randomising converts a
   potential bias into variance, which the replication design already measures.
   The seed is recorded so the order is reproducible.

2. A FRESH CONTAINER PER RUN.
   No V8 heap, no connection table and no replay log survives between
   replications. Reusing a container would make replication r+1 depend on r.

3. RESUME.
   A sweep of this length will be interrupted. Completed runs are detected from
   the results directory and skipped, so an interrupted sweep can be continued
   without repeating work or, worse, restarting and silently mixing runs from
   two different host states.

Usage:
    python3 orchestrator/run_matrix.py --plan          # print the matrix and time budget
    python3 orchestrator/run_matrix.py --pilot         # short pilot configuration
    python3 orchestrator/run_matrix.py --execute       # run the full sweep
    python3 orchestrator/run_matrix.py --execute --only-arm ws --only-tier 10
"""

import argparse
import itertools
import json
import os
import random
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
SEED = 20260811  # recorded in the manifest; changing it changes the run order

# --- the design -------------------------------------------------------------
# Four arms across three architectural families. Section 1.1 frames HTTP polling
# as one architecture with two strategies (fixed-interval and held-open), so
# short and long polling are configurations of one family rather than separate
# architectures. Both are run because Section 2.4's central conditional finding
# -- that long polling matches WebSocket while network latency stays below half
# the message interval (Pimentel & Nickerson, 2012) -- cannot be tested otherwise.
ARMS = ["ws", "sse", "poll-short", "poll-long"]

# Order-of-magnitude tiers, per Section 1.7.
TIERS = [10, 100, 1000]

WORKLOADS = {
    #                     payload  server_hz  client_hz  matched_poll_ms  room
    "chat":         dict(payload=120,  server_hz=0,  client_hz=0.2, poll_ms=1000, room=20),
    "notification": dict(payload=256,  server_hz=1,  client_hz=0,   poll_ms=1000, room=0),
    "dashboard":    dict(payload=1024, server_hz=10, client_hz=0,   poll_ms=100,  room=0),
}

# Polling-interval mismatch is applied to ONE workload rather than all three.
# Bozdag et al. (2007) list testing a single polling interval among their own
# threats to validity, so the pull side must be a curve rather than a point --
# but a full mismatch factor across every cell would multiply the sweep beyond
# the available time budget. The notification workload is chosen because its
# publish rate is the one against which a 5x mismatch is most interpretable.
MISMATCH_FACTOR = 5
MISMATCH_WORKLOAD = "notification"

REPLICATES = 10   # see analyse.py: the unit of analysis is the run, not the message

# Warm-up and measurement windows. Both are PILOT-DETERMINED; these are the
# defaults the pilot is designed to confirm or revise, not assumptions.
WARMUP_S = 30
DURATION_S = {"chat": 600, "notification": 600, "dashboard": 300}
COOLDOWN_S = 30


@dataclass
class Cell:
    arm: str
    workload: str
    tier: int
    poll_interval_ms: int
    mismatched: bool
    payload_bytes: int
    server_rate_hz: float
    client_rate_hz: float
    room_size: int
    duration_s: int
    warmup_s: int = WARMUP_S

    @property
    def cell_id(self) -> str:
        suffix = "-mm" if self.mismatched else ""
        return f"{self.arm}-{self.workload}-c{self.tier}{suffix}"

    @property
    def is_polling(self) -> bool:
        return self.arm.startswith("poll")


def build_matrix() -> list[Cell]:
    cells: list[Cell] = []
    for arm, workload, tier in itertools.product(ARMS, WORKLOADS, TIERS):
        w = WORKLOADS[workload]
        cells.append(Cell(
            arm=arm, workload=workload, tier=tier,
            poll_interval_ms=w["poll_ms"], mismatched=False,
            payload_bytes=w["payload"], server_rate_hz=w["server_hz"],
            client_rate_hz=w["client_hz"], room_size=w["room"],
            duration_s=DURATION_S[workload],
        ))

    # Mismatched-interval cells: short polling only. Long polling has no fixed
    # interval to mismatch -- its analogue is the held-request timeout, which is
    # not the same parameter and is held constant.
    for tier in TIERS:
        w = WORKLOADS[MISMATCH_WORKLOAD]
        cells.append(Cell(
            arm="poll-short", workload=MISMATCH_WORKLOAD, tier=tier,
            poll_interval_ms=w["poll_ms"] * MISMATCH_FACTOR, mismatched=True,
            payload_bytes=w["payload"], server_rate_hz=w["server_hz"],
            client_rate_hz=w["client_hz"], room_size=w["room"],
            duration_s=DURATION_S[MISMATCH_WORKLOAD],
        ))
    return cells


def build_run_list(cells: list[Cell], replicates: int, seed: int) -> list[tuple[Cell, int]]:
    runs = [(c, r) for c in cells for r in range(replicates)]
    random.Random(seed).shuffle(runs)
    return runs


def budget(runs: list[tuple[Cell, int]]) -> dict:
    per_run = [c.warmup_s + c.duration_s + COOLDOWN_S + 20 for c, _ in runs]  # +20s container churn
    total_s = sum(per_run)
    return dict(
        runs=len(runs),
        cells=len({c.cell_id for c, _ in runs}),
        total_seconds=total_s,
        total_human=str(timedelta(seconds=total_s)),
        mean_run_s=total_s / max(1, len(runs)),
    )


# --- execution --------------------------------------------------------------
def compose_env(cell: Cell, replicate: int) -> dict:
    script = {"ws": "ws.js", "sse": "sse.js", "poll-short": "poll.js", "poll-long": "poll.js"}[cell.arm]
    total_s = cell.warmup_s + cell.duration_s
    return {
        **os.environ,
        "RTB_ARM": cell.arm,
        "RTB_SCRIPT": script,
        "RTB_WORKLOAD": cell.workload,
        "RTB_CLIENTS": str(cell.tier),
        "RTB_CELL_ID": cell.cell_id,
        "RTB_REPLICATE": str(replicate),
        "RTB_RUN_ID": f"{cell.cell_id}-r{replicate}",
        "RTB_WARMUP_S": str(cell.warmup_s),
        "RTB_DURATION_S": str(cell.duration_s),
        "RTB_TOTAL_S": str(total_s),
        "RTB_POLL_INTERVAL_MS": str(cell.poll_interval_ms),
        "RTB_PAYLOAD_BYTES": str(cell.payload_bytes),
        "RTB_SERVER_RATE_HZ": str(cell.server_rate_hz),
        "RTB_CLIENT_RATE_HZ": str(cell.client_rate_hz),
        "RTB_CLIENT_PAYLOAD_BYTES": str(cell.payload_bytes),
        "RTB_ROOM_SIZE": str(cell.room_size or 20),
        "RTB_LONG_POLL_TIMEOUT_MS": "25000",
    }


def sh(cmd: list[str], env=None, timeout=None, check=True) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:4])}... exited {r.returncode}\n{r.stderr[-2000:]}")
    return r


def wait_healthy(env: dict, service: str, timeout_s=60) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = sh(["docker", "compose", "--profile", service, "ps", "--format", "json"],
               env=env, check=False)
        if r.returncode == 0 and r.stdout.strip():
            try:
                for line in r.stdout.strip().splitlines():
                    rec = json.loads(line)
                    if rec.get("Service") == service and "healthy" in (rec.get("Health") or ""):
                        return
            except json.JSONDecodeError:
                pass
        time.sleep(1)
    raise TimeoutError(f"service {service} not healthy within {timeout_s}s")


def curl_json(env: dict, arm: str, path: str) -> dict | None:
    """Fetch a control-plane endpoint from inside the bench network."""
    r = sh(["docker", "compose", "run", "--rm", "--no-deps", "-T",
            "--entrypoint", "curl", "k6", "-s", f"http://{arm}:8080{path}"],
           env=env, check=False, timeout=60)
    if r.returncode != 0 or not r.stdout.strip():
        return None
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return None


def run_one(cell: Cell, replicate: int, dry: bool) -> dict:
    env = compose_env(cell, replicate)
    tag = f"{cell.cell_id}-r{replicate}"
    started = datetime.now(timezone.utc)

    if dry:
        print(f"    [dry-run] would execute {tag}")
        return {"tag": tag, "status": "dry-run"}

    manifest = {"tag": tag, "cell": asdict(cell), "replicate": replicate,
                "startedAt": started.isoformat()}
    try:
        # 1. Fresh container for this arm only.
        sh(["docker", "compose", "--profile", cell.arm, "up", "-d",
            "--force-recreate", "--renew-anon-volumes", cell.arm], env=env)
        wait_healthy(env, cell.arm)

        # 2. Zero the counters and restart the publication sequence so the run
        #    begins from a known state even though the container is new.
        curl_json(env, cell.arm, "/reset")
        manifest["serverConfig"] = curl_json(env, cell.arm, "/config")

        # 3. Load generator and probe run concurrently. Both must be allowed to
        #    finish.
        #
        #    --abort-on-container-exit was used here originally and is wrong for
        #    this workload: it stops every container as soon as the FIRST one
        #    exits, so whichever of k6 and the probe happened to finish first
        #    killed the other mid-write. Which one won was a race, so the loss
        #    would have been intermittent across the sweep rather than obvious.
        #    Attached `up` without that flag returns once all services have
        #    exited on their own.
        total = cell.warmup_s + cell.duration_s
        sh(["docker", "compose", "--profile", "load", "up", "k6", "probe"],
           env=env, timeout=total + 300, check=False)

        # 4. Capture the server's own view before tearing it down.
        manifest["serverStats"] = curl_json(env, cell.arm, "/stats")
        manifest["status"] = "ok"

    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = str(exc)[:4000]
    finally:
        sh(["docker", "compose", "--profile", cell.arm, "--profile", "load",
            "down", "-v", "--remove-orphans"], env=env, check=False)
        manifest["finishedAt"] = datetime.now(timezone.utc).isoformat()
        manifest["elapsedS"] = (datetime.now(timezone.utc) - started).total_seconds()
        (RESULTS / f"{tag}-manifest.json").write_text(json.dumps(manifest, indent=2))

    # 5. Cool-down. Lets the host's page cache, TIME_WAIT sockets and thermal
    #    state settle so the next run does not inherit this one's aftermath.
    time.sleep(COOLDOWN_S)
    return manifest


def already_done(tag: str) -> bool:
    m = RESULTS / f"{tag}-manifest.json"
    if not m.exists():
        return False
    try:
        return json.loads(m.read_text()).get("status") == "ok"
    except Exception:
        return False


# --- entry point ------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true", help="print matrix and time budget only")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pilot", action="store_true", help="short-duration pilot configuration")
    ap.add_argument("--replicates", type=int, default=REPLICATES)
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--only-arm", action="append")
    ap.add_argument("--only-tier", action="append", type=int)
    ap.add_argument("--only-workload", action="append")
    args = ap.parse_args()

    RESULTS.mkdir(exist_ok=True)
    cells = build_matrix()

    if args.pilot:
        # The pilot exists to determine parameters, not to produce findings, so
        # it runs every cell shape briefly rather than a few cells fully.
        for c in cells:
            c.duration_s = 120
            c.warmup_s = 60   # deliberately long: the pilot MEASURES the warm-up
        args.replicates = 2

    if args.only_arm:
        cells = [c for c in cells if c.arm in args.only_arm]
    if args.only_tier:
        cells = [c for c in cells if c.tier in args.only_tier]
    if args.only_workload:
        cells = [c for c in cells if c.workload in args.only_workload]

    runs = build_run_list(cells, args.replicates, args.seed)
    b = budget(runs)

    print(f"\n{'='*72}\nDESIGN MATRIX\n{'='*72}")
    print(f"  arms                {len(ARMS)}  ({', '.join(ARMS)})")
    print(f"  workloads           {len(WORKLOADS)}  ({', '.join(WORKLOADS)})")
    print(f"  concurrency tiers   {len(TIERS)}  ({', '.join(map(str, TIERS))})")
    print(f"  cells               {b['cells']}")
    print(f"  replicates per cell {args.replicates}")
    print(f"  total runs          {b['runs']}")
    print(f"  mean run            {b['mean_run_s']:.0f} s")
    print(f"  TOTAL EXECUTION     {b['total_human']}  ({b['total_seconds']/3600:.1f} h)")
    print(f"  run-order seed      {args.seed}")
    print("\n  Report the total execution time in Chapter 3: Arcuri & Briand (2011)")
    print("  require a study performing few repetitions to state the reason and")
    print("  report the experiment's total execution time.")

    manifest = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "seed": args.seed,
        "replicates": args.replicates,
        "budget": b,
        "cells": [asdict(c) for c in cells],
        "runOrder": [f"{c.cell_id}-r{r}" for c, r in runs],
    }
    (RESULTS / "design_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"\n  design manifest -> {RESULTS/'design_manifest.json'}")

    if args.plan or not (args.execute or args.dry_run):
        return 0

    if not shutil.which("docker"):
        print("\n  ERROR: docker not on PATH. Cannot execute.", file=sys.stderr)
        return 1

    pending = [(c, r) for c, r in runs if not already_done(f"{c.cell_id}-r{r}")]
    print(f"\n  {len(runs) - len(pending)} run(s) already complete; {len(pending)} pending\n")

    t0 = time.time()
    failures = 0
    for i, (cell, rep) in enumerate(pending, 1):
        eta = ""
        if i > 1:
            per = (time.time() - t0) / (i - 1)
            eta = f"  ETA {timedelta(seconds=int(per * (len(pending) - i + 1)))}"
        print(f"[{i}/{len(pending)}] {cell.cell_id} r{rep}{eta}")
        res = run_one(cell, rep, args.dry_run)
        if res.get("status") == "failed":
            failures += 1
            print(f"    FAILED: {res.get('error','')[:200]}")

    print(f"\nSweep complete in {timedelta(seconds=int(time.time()-t0))}; {failures} failure(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
