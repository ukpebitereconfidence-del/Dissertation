#!/usr/bin/env python3
"""
PILOT REPORT

Answers the pre-declared pilot questions of docs/PILOT.md directly from the
collected runs, so the parameter decisions for the full sweep are read off a
report rather than reconstructed from CSVs.

The pilot does not produce findings. It produces the numbers that set the
measurement windows, the replication count, and whether the workload reaches
saturation at all.

    python3 orchestrator/pilot_report.py --results results
"""

import argparse
import json
import glob
import statistics as st
from collections import defaultdict
from pathlib import Path


def load(results: Path):
    runs = []
    for f in sorted(results.glob("*-manifest.json")):
        m = json.loads(f.read_text())
        if m.get("status") != "ok":
            continue
        tag = m["tag"]
        cell = m.get("cell", {})
        r = {
            "tag": tag,
            "arm": cell.get("arm"),
            "workload": cell.get("workload"),
            "tier": cell.get("tier"),
            "mismatched": cell.get("mismatched", False),
            "elapsed": m.get("elapsedS", 0),
            "duration_s": cell.get("duration_s", 0),
        }
        srv = m.get("serverStats") or {}
        r["counters"] = srv.get("counters", {}) or {}
        r["memory"] = srv.get("memory", {}) or {}
        r["cpu"] = srv.get("cpu", {}) or {}
        b = srv.get("broadcaster") or {}
        r["published"] = b.get("published")
        r["missed"] = b.get("missedTicks", 0) or 0
        r["max_lag"] = b.get("maxTickLagMs", 0) or 0

        pf = results / f"{tag}-probe.json"
        if pf.exists():
            p = json.loads(pf.read_text())
            r["probe"] = p.get("latencyMs", {})
            r["samples"] = p.get("samples", 0)
            r["fallbacks"] = p.get("realtimeFallbacks", 0)
            r["drift"] = abs((p.get("clock") or {}).get("maxAbsDriftUs") or 0)
        sf = results / f"{tag}-summary.json"
        if sf.exists():
            try:
                mt = json.loads(sf.read_text()).get("metrics", {})
                g = lambda n, k: (mt.get(n) or {}).get(k)
                r["k6_msgs"] = g("rtb_messages_received", "count")
                r["k6_bytes"] = g("rtb_payload_bytes_received", "count")
                r["k6_polls"] = g("rtb_poll_requests", "count")
                r["k6_empty"] = g("rtb_poll_empty_responses", "count")
                r["k6_errors"] = g("rtb_delivery_errors", "rate")
                r["k6_p95"] = g("rtb_e2e_latency_ms", "p(95)")
                r["k6_med"] = g("rtb_e2e_latency_ms", "med")
            except json.JSONDecodeError:
                pass
        runs.append(r)
    return runs


def hdr(n, q):
    print(f"\n{'='*78}\n{n}  {q}\n{'='*78}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", type=Path, default=Path("results"))
    args = ap.parse_args()
    runs = load(args.results)
    if not runs:
        print("no completed runs found")
        return 1

    print(f"PILOT REPORT -- {len(runs)} completed runs")

    # ---- P0 clock -------------------------------------------------------
    hdr("P0", "Is the host clock stable?")
    drifts = [r.get("drift", 0) for r in runs if "drift" in r]
    bad = [r for r in runs if r.get("drift", 0) > 5000]
    print(f"  max re-anchor correction across runs : {max(drifts):.0f} us")
    print(f"  median                               : {st.median(drifts):.0f} us")
    print(f"  runs above the 5,000 us gate         : {len(bad)}")
    for r in bad:
        print(f"    ! {r['tag']}  drift={r['drift']:.0f} us  elapsed={r['elapsed']:.0f}s")
    print("  VERDICT:", "PASS" if not bad else "some runs must be discarded and repeated")

    # ---- P2 timebase integrity -----------------------------------------
    hdr("P2b", "Are all latency samples on the monotonic timebase?")
    fb = [r for r in runs if r.get("fallbacks", 0) > 0]
    by_wl = defaultdict(int)
    for r in fb:
        by_wl[r["workload"]] += 1
    print(f"  runs with CLOCK_REALTIME fallbacks : {len(fb)} of {len(runs)}")
    for w, n in sorted(by_wl.items()):
        print(f"    {w:14s} {n} run(s)")
    if fb:
        print("  CAUSE: messages originated by k6 carry only the realtime stamp, because")
        print("  k6's runtime cannot read CLOCK_MONOTONIC. The probe receives them and")
        print("  falls back. This affects the chat workload only, where all traffic")
        print("  originates at clients rather than at the server.")
        print("  CONSEQUENCE: chat latency is step-vulnerable, unlike every other cell.")

    # ---- P4 sample sufficiency -----------------------------------------
    hdr("P4", "Is the measurement window long enough for p99?")
    thin = [r for r in runs if r.get("samples", 0) < 1000]
    print(f"  {'cell':44s} {'samples':>9s} {'window':>8s}")
    seen = set()
    for r in sorted(runs, key=lambda x: x.get("samples", 0)):
        key = (r["arm"], r["workload"], r["tier"])
        if key in seen:
            continue
        seen.add(key)
        s = r.get("samples", 0)
        flag = "  THIN" if s < 1000 else ""
        if s < 4000:
            print(f"  {r['arm']+'-'+r['workload']+'-c'+str(r['tier']):44s} {s:9d} {r['duration_s']:7d}s{flag}")
    print(f"\n  runs under 1,000 samples: {len(thin)}")
    if thin:
        need = defaultdict(list)
        for r in thin:
            need[(r["workload"], r["tier"])].append(r.get("samples", 0))
        print("  ACTION: lengthen the window for these (workload, tier) combinations:")
        for (w, t), ss in sorted(need.items()):
            worst = min(ss)
            factor = 1000 / max(1, worst)
            print(f"    {w:14s} c{t:<5d} worst={worst:5d} samples -> multiply window by ~{factor:.1f}x")

    # ---- P5 variability ------------------------------------------------
    hdr("P5", "How variable is a run? (drives the replication count)")
    groups = defaultdict(list)
    for r in runs:
        if r.get("probe", {}).get("p95") is not None:
            groups[(r["arm"], r["workload"], r["tier"], r.get("mismatched", False))].append(r["probe"]["p95"])
    cvs = []
    print(f"  {'cell':40s} {'n':>2s} {'p95 mean':>11s} {'CV':>7s}")
    for k in sorted(groups):
        v = groups[k]
        if len(v) < 2:
            continue
        mu = st.mean(v)
        cv = st.stdev(v) / mu if mu else 0
        cvs.append(cv)
        flag = "  HIGH" if cv > 0.15 else ""
        label = k[0] + '-' + k[1] + '-c' + str(k[2]) + ('-mm' if k[3] else '')
        print(f"  {label:40s} {len(v):2d} {mu:11.2f} {cv:7.3f}{flag}")
    if cvs:
        print(f"\n  median CV across cells : {st.median(cvs):.3f}")
        print(f"  worst CV               : {max(cvs):.3f}")
        print(f"  cells above 0.15       : {sum(1 for c in cvs if c > 0.15)} of {len(cvs)}")
        print("  NOTE: with only 2 replications these CV estimates are themselves")
        print("  imprecise. Treat them as an order-of-magnitude guide to whether")
        print("  n = 10 will separate close arms.")

    # ---- P6 saturation --------------------------------------------------
    hdr("P6", "Is saturation reached? (decides whether RQ3 is answerable)")
    print(f"  {'cell':40s} {'missed':>7s} {'maxlag':>8s} {'rss MB':>8s} {'delivered':>11s}")
    sat = []
    for r in sorted(runs, key=lambda x: -(x["missed"] or 0)):
        if r["missed"] or r["max_lag"] > 50:
            sat.append(r)
            print(f"  {r['tag'][:40]:40s} {r['missed']:7d} {r['max_lag']:8.0f} "
                  f"{r['memory'].get('rss',0)/1e6:8.0f} {r['counters'].get('messages_sent',0):11d}")
    if sat:
        print(f"\n  {len(sat)} run(s) could not sustain the publication schedule.")
        print("  INTERPRETATION: this IS saturation -- the server fell behind its own")
        print("  publish deadline. Research Question 3 is therefore answerable within")
        print("  the 1,000-client ceiling, which is what Appelqvist & Ornmyr (2017)")
        print("  could not achieve at ~4% CPU.")
        print("  BUT these cells measured a DEGRADED workload, so they are not")
        print("  comparable with their unsaturated siblings and must be reported")
        print("  separately rather than pooled (Section 3.3.5).")
    else:
        print("  No run missed a publication tick.")
        print("  ACTION: saturation was NOT reached. Raise the dashboard publication")
        print("  rate until the knee of the curve falls inside the tested range, and")
        print("  report the revised rate. Without this the study reproduces the")
        print("  principal limitation of Appelqvist & Ornmyr (2017).")

    # ---- P7 fairness ----------------------------------------------------
    hdr("P7", "Is the harness fair? (delivery, loss, errors)")
    prob = []
    for r in runs:
        errs = r.get("k6_errors") or 0
        if errs > 0.01:
            prob.append((r["tag"], f"error rate {errs:.3%}"))
    print(f"  runs with delivery error rate > 1% : {len(prob)}")
    for t, why in prob[:10]:
        print(f"    ! {t}  {why}")
    print("  VERDICT:", "PASS" if not prob else "investigate before the sweep")

    # ---- P8 byte accounting --------------------------------------------
    hdr("P8", "Does byte accounting agree with published measurement?")
    print("  Appelqvist & Ornmyr (2017), notification-like workload:")
    print("    short polling ~472 B/msg overhead | SSE ~8 B | WebSocket ~2 B")
    print(f"\n  {'arm':12s} {'bytes/msg':>11s}  (payload 256 B for notification)")
    per = defaultdict(list)
    for r in runs:
        if r["workload"] != "notification" or r.get("mismatched"):
            continue
        if r.get("k6_msgs") and r.get("k6_bytes"):
            per[r["arm"]].append(r["k6_bytes"] / r["k6_msgs"])
    for a in sorted(per):
        print(f"  {a:12s} {st.mean(per[a]):11.1f}")
    print("\n  NOTE: k6 counts application payload, not TCP/HTTP framing, so these")
    print("  are payload-side figures. Compare the DIFFERENCES between arms rather")
    print("  than absolute values against the published header sizes.")

    # ---- headline ordering ---------------------------------------------
    hdr("--", "Latency ordering (descriptive only, NOT a finding)")
    order = defaultdict(list)
    for r in runs:
        p = r.get("probe", {})
        if p.get("p50") is not None and not r.get("mismatched"):
            order[(r["workload"], r["tier"], r["arm"])].append(p["p50"])
    for wl in ("chat", "notification", "dashboard"):
        print(f"\n  {wl}")
        print(f"    {'tier':>6s}  {'ws':>10s} {'sse':>10s} {'poll-short':>12s} {'poll-long':>11s}")
        for tier in (10, 100, 1000):
            cells = []
            for arm in ("ws", "sse", "poll-short", "poll-long"):
                v = order.get((wl, tier, arm))
                cells.append(f"{st.mean(v):10.2f}" if v else f"{'-':>10s}")
            print(f"    {tier:6d}  {cells[0]} {cells[1]} {cells[2]:>12s} {cells[3]:>11s}")
    print("\n  p50 latency in ms, mean of replications. Descriptive only: with 2")
    print("  replications no inference is warranted (Section 3.8.1).")

    print(f"\n{'='*78}\nRecord these answers in results/pilot_findings.md before the full sweep.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
