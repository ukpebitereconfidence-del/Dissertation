#!/usr/bin/env python3
"""
STATISTICAL ANALYSIS

Implements the procedure Section 1.8 commits to, with one decision that is not
stated there and needs to be, because it determines whether any of the p-values
mean anything.

    THE UNIT OF ANALYSIS IS THE RUN, NOT THE MESSAGE.

A single dashboard run at the 1,000-client tier delivers on the order of three
million messages. Feeding three million latency samples per arm into a
Mann-Whitney U test returns p < 1e-300 for any difference whatever, including a
difference of a few microseconds produced by nothing but scheduling noise. That
is not a real finding; it is the arithmetic of large n. Section 2.6 identifies
exactly this hazard -- "with large sample counts, trivial differences become
statistically significant" -- and it is the reason effect sizes are required.

But an effect size does not repair a mis-specified unit of analysis. Messages
within one run are not independent observations: they share a container, a heap,
a CPU, a thermal state and a scheduling regime. Treating them as independent is
pseudoreplication, and it inflates significance without bound.

So the pipeline is:

    per run  ->  one summary statistic (p50, p95, p99, mean, CPU, memory, ...)
    per cell ->  n = REPLICATES independent observations of that statistic
    tests    ->  across those run-level observations

n is therefore 10 per cell, not 3,000,000. Mann-Whitney U with n1 = n2 = 10
reaches p < 0.0001 at complete separation, which is ample power for effects of
the magnitude this literature reports, and the resulting claim is defensible:
"ten independent runs of A were faster than ten independent runs of B", not
"three million correlated messages differed".

Percentiles are computed WITHIN each run, from that run's full sample, then
compared ACROSS runs. This is the correct order: pooling all runs' messages and
taking one percentile of the pool would discard the between-run variance the
design exists to measure.

Usage:
    python3 orchestrator/analyse.py --results results/ --out analysis/
"""

import argparse
import json
import math
import sys
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

# fillna on object-dtype columns downcasts silently in current pandas and will
# change behaviour in a future release; opting in now keeps the integrity gates
# deterministic across pandas versions.
pd.set_option("future.no_silent_downcasting", True)
from scipy import stats

# ---------------------------------------------------------------------------
# Effect size
# ---------------------------------------------------------------------------
def vargha_delaney_a12(a, b) -> float:
    """
    Vargha-Delaney A12: P(X > Y) + 0.5 * P(X == Y).

    Recommended by Arcuri & Briand (2011) in preference to Cohen's d for
    strongly skewed data, because scaling a difference in means by a standard
    deviation is unreliable where the distribution's shape makes that standard
    deviation uninformative. Interpretable without reference to units:
    0.5 means no difference, 1.0 means every A observation exceeds every B.

    Computed from the Mann-Whitney U statistic, which is the standard identity
    A12 = U / (m * n) -- not by counting pairs, which is O(mn).
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    m, n = len(a), len(b)
    if m == 0 or n == 0:
        return float("nan")
    u, _ = stats.mannwhitneyu(a, b, alternative="two-sided")
    return u / (m * n)


def a12_magnitude(a12: float) -> str:
    """Vargha & Delaney's (2000) thresholds, expressed as distance from 0.5."""
    if math.isnan(a12):
        return "undefined"
    d = abs(a12 - 0.5)
    if d < 0.06:
        return "negligible"
    if d < 0.14:
        return "small"
    if d < 0.21:
        return "medium"
    return "large"


def holm_correct(pvalues: list[float]) -> list[float]:
    """
    Holm (1979) step-down adjustment.

    Section 2.6 records the study's position: Arcuri & Briand (2011) advise
    against Bonferroni-family adjustment because it suppresses genuine findings,
    and recommend reporting all p-values instead. This study reports every
    unadjusted p-value AND applies Holm to the fixed, pre-specified set of
    pairwise comparisons within each cell group, on the narrower ground that
    family-wise error across a pre-registered set is a real inflation and Holm
    is uniformly more powerful than Bonferroni. Because the unadjusted values
    are always reported, the objection that adjustment conceals results does
    not apply.
    """
    m = len(pvalues)
    order = sorted(range(m), key=lambda i: pvalues[i])
    adjusted = [0.0] * m
    running = 0.0
    for rank, idx in enumerate(order):
        val = (m - rank) * pvalues[idx]
        running = max(running, val)          # enforce monotonicity
        adjusted[idx] = min(1.0, running)
    return adjusted


def ratio_ci(a, b, n_boot=10000, alpha=0.05, seed=11):
    """Bootstrap CI for the ratio of medians -- the magnitude of the difference,
    on a scale that is interpretable without reference to units."""
    a = np.asarray(a, float); b = np.asarray(b, float)
    a = a[~np.isnan(a)]; b = b[~np.isnan(b)]
    if len(a) < 2 or len(b) < 2:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    r = []
    for _ in range(n_boot):
        mb = np.median(rng.choice(b, len(b), replace=True))
        if mb == 0:
            continue
        r.append(np.median(rng.choice(a, len(a), replace=True)) / mb)
    if not r:
        return (float("nan"), float("nan"))
    return (float(np.percentile(r, 100 * alpha / 2)), float(np.percentile(r, 100 * (1 - alpha / 2))))


# Perceptual thresholds from Nielsen (1993), adopted in Section 1.2.
INSTANTANEOUS_MS = 100.0
FLOW_BREAK_MS = 1000.0


def classify_practical(median_a, median_b, metric):
    """
    Classify a difference by whether it changes the user-perceptible outcome,
    not by whether it is detectable. Only applied to latency metrics; other
    metrics are classified by relative magnitude alone.
    """
    if any(np.isnan([median_a, median_b])):
        return "undefined"
    is_latency = "lat" in metric or metric.startswith("probe_")
    lo, hi = min(median_a, median_b), max(median_a, median_b)
    rel = (hi - lo) / lo if lo > 0 else float("inf")

    if is_latency:
        # A difference that keeps both arms inside the same perceptual band is
        # not practically decisive, however reliable it is statistically.
        def band(x):
            if x < INSTANTANEOUS_MS: return 0
            if x < FLOW_BREAK_MS: return 1
            return 2
        if band(lo) == band(hi):
            return "same perceptual band" if rel < 0.5 else f"same band, {rel:.1f}x apart"
        return "crosses perceptual threshold"

    if rel < 0.05: return "negligible (<5%)"
    if rel < 0.25: return f"modest ({rel*100:.0f}%)"
    return f"substantial ({rel:.1f}x)"


def bootstrap_ci(values, statistic=np.median, n_boot=10000, alpha=0.05, seed=7):
    """
    Percentile bootstrap CI on a run-level statistic.

    Section 1.8 commits to bootstrap confidence intervals for percentile
    estimates. With n = 10 runs the interval is wide, and that width is
    informative: it is the honest precision of ten replications, and reporting it
    prevents the over-claiming that Section 2.5 criticises in studies presenting
    single values without dispersion.
    """
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if len(v) < 2:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    boots = np.array([statistic(rng.choice(v, size=len(v), replace=True)) for _ in range(n_boot)])
    return (float(np.percentile(boots, 100 * alpha / 2)),
            float(np.percentile(boots, 100 * (1 - alpha / 2))))


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------
def load_runs(results_dir: Path) -> pd.DataFrame:
    """
    One row per RUN. Combines three sources per run:
      *-manifest.json  orchestrator record + the server's own /stats
      *-summary.json   k6 summary  (authoritative: throughput, errors, connections)
      *-probe.json     probe       (authoritative: fine-grained latency)
    """
    rows = []
    for man_path in sorted(results_dir.glob("*-manifest.json")):
        tag = man_path.name.replace("-manifest.json", "")
        try:
            man = json.loads(man_path.read_text())
        except json.JSONDecodeError:
            print(f"  skipping unreadable manifest: {man_path.name}", file=sys.stderr)
            continue
        if man.get("status") != "ok":
            continue

        cell = man.get("cell", {})
        row = {
            "tag": tag,
            "arm": cell.get("arm"),
            "family": "poll" if str(cell.get("arm", "")).startswith("poll") else cell.get("arm"),
            "workload": cell.get("workload"),
            "tier": cell.get("tier"),
            "mismatched": cell.get("mismatched", False),
            "poll_interval_ms": cell.get("poll_interval_ms"),
            "replicate": man.get("replicate"),
            "cell_id": f"{cell.get('arm')}-{cell.get('workload')}-c{cell.get('tier')}"
                       + ("-mm" if cell.get("mismatched") else ""),
            "elapsed_s": man.get("elapsedS"),
        }

        # --- server-side view -------------------------------------------
        stats_blob = man.get("serverStats") or {}
        counters = stats_blob.get("counters", {}) or {}
        bcast = stats_blob.get("broadcaster") or {}
        row["srv_messages_sent"] = counters.get("messages_sent")
        row["srv_bytes_sent"] = counters.get("bytes_sent")
        row["srv_http_requests"] = counters.get("http_requests")
        row["srv_empty_polls"] = counters.get("http_empty_responses")
        row["srv_uplink_requests"] = counters.get("uplink_requests")
        row["srv_connection_errors"] = counters.get("connection_errors")
        row["srv_rss_bytes"] = (stats_blob.get("memory") or {}).get("rss")
        row["srv_heap_used_bytes"] = (stats_blob.get("memory") or {}).get("heapUsed")
        row["published"] = bcast.get("published")
        # A non-zero value here means the server could not sustain the declared
        # publication schedule: the workload itself degraded, so the cell's
        # latency figures describe a different experiment from its siblings.
        row["missed_ticks"] = bcast.get("missedTicks")
        row["max_tick_lag_ms"] = bcast.get("maxTickLagMs")

        # --- probe: fine-grained latency --------------------------------
        probe_path = results_dir / f"{tag}-probe.json"
        if probe_path.exists():
            try:
                p = json.loads(probe_path.read_text())
                lat = p.get("latencyMs", {})
                for k in ("mean", "sd", "p50", "p90", "p95", "p99", "p999", "max"):
                    row[f"probe_{k}"] = lat.get(k)
                row["probe_samples"] = p.get("samples")
                clk = p.get("clock") or {}
                row["clock_max_drift_us"] = clk.get("maxAbsDriftUs")
                row["clock_anchor_error_ms"] = clk.get("anchorErrorMs")
                row["clock_calibrations"] = clk.get("calibrations")
                row["probe_timebase"] = p.get("timebase")
                row["probe_realtime_fallbacks"] = p.get("realtimeFallbacks")
                row["probe_negatives"] = p.get("negativeLatencies")
                row["probe_duplicates"] = p.get("duplicateSequences")
            except json.JSONDecodeError:
                pass

        # --- k6: throughput, errors, connections -------------------------
        sum_path = results_dir / f"{tag}-summary.json"
        if sum_path.exists():
            try:
                s = json.loads(sum_path.read_text())
                m = s.get("metrics", {})

                def g(metric, field):
                    return (m.get(metric) or {}).get(field)

                row["k6_msgs_received"] = g("rtb_messages_received", "count")
                row["k6_bytes_received"] = g("rtb_payload_bytes_received", "count")
                row["k6_seq_gaps"] = g("rtb_sequence_gaps", "count")
                row["k6_connect_failures"] = g("rtb_connect_failures", "count")
                row["k6_error_rate"] = g("rtb_delivery_errors", "rate")
                row["k6_poll_requests"] = g("rtb_poll_requests", "count")
                row["k6_empty_polls"] = g("rtb_poll_empty_responses", "count")
                row["k6_connect_ms_p95"] = g("rtb_connect_duration_ms", "p(95)")
                for pk, field in (("p50", "med"), ("p95", "p(95)"), ("p99", "p(99)"), ("mean", "avg")):
                    row[f"k6_lat_{pk}"] = g("rtb_e2e_latency_ms", field)
            except json.JSONDecodeError:
                pass

        # Derived: throughput and per-message network cost.
        dur = cell.get("duration_s") or 1
        if row.get("k6_msgs_received"):
            row["throughput_msg_s"] = row["k6_msgs_received"] / dur
        if row.get("k6_msgs_received") and row.get("k6_bytes_received"):
            row["bytes_per_message"] = row["k6_bytes_received"] / max(1, row["k6_msgs_received"])
        # Wasted work: the fraction of polls returning nothing new. This is
        # polling's defining inefficiency (Bozdag et al., 2007) and is zero by
        # construction for the persistent arms.
        if row.get("k6_poll_requests"):
            row["empty_poll_fraction"] = (row.get("k6_empty_polls") or 0) / row["k6_poll_requests"]

        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Integrity gate
# ---------------------------------------------------------------------------
def integrity_report(df: pd.DataFrame) -> list[str]:
    """
    Checks that must pass before any inferential result is trustworthy. A harness
    that under-delivers silently produces plausible latency figures from a broken
    experiment, so these are reported prominently rather than buried.
    """
    issues = []
    if df.empty:
        return ["no completed runs found"]

    if "missed_ticks" in df:
        bad = df[pd.to_numeric(df["missed_ticks"], errors="coerce").fillna(0) > 0]
        if len(bad):
            issues.append(
                f"{len(bad)} run(s) missed publication ticks (max {bad['missed_ticks'].max():.0f}). "
                "The server could not sustain the declared publish schedule, so these cells "
                "measured a degraded workload. Exclude or report separately."
            )
    if "clock_max_drift_us" in df:
        # A large re-anchor correction means the monotonic and realtime clocks
        # diverged faster than calibration could absorb, so latency samples from
        # that run carry an unknown constant offset. This gate exists because
        # exactly that failure produced plausible-looking 900 ms latencies during
        # harness validation.
        bad = df[pd.to_numeric(df["clock_max_drift_us"], errors="coerce").abs().fillna(0) > 5000]
        if len(bad):
            issues.append(
                f"{len(bad)} run(s) saw a CLOCK_REALTIME step above 5 ms "
                f"(max {bad['clock_max_drift_us'].abs().max():.0f} us). Probe latency is "
                "measured on CLOCK_MONOTONIC and is unaffected, but k6's latency figures "
                "for these runs are contaminated and should not be used as a cross-check."
            )
    if "probe_realtime_fallbacks" in df:
        bad = df[pd.to_numeric(df["probe_realtime_fallbacks"], errors="coerce").fillna(0) > 0]
        if len(bad):
            issues.append(
                f"{len(bad)} run(s) had probe samples fall back to CLOCK_REALTIME "
                "(envelope lacked a monotonic stamp). Those samples are step-vulnerable."
            )
    if "probe_negatives" in df:
        bad = df[pd.to_numeric(df["probe_negatives"], errors="coerce").fillna(0) > 0]
        if len(bad):
            issues.append(f"{len(bad)} run(s) recorded negative latencies -- clock assumption violated.")
    if "k6_seq_gaps" in df:
        bad = df[pd.to_numeric(df["k6_seq_gaps"], errors="coerce").fillna(0) > 0]
        if len(bad):
            issues.append(f"{len(bad)} run(s) show sequence gaps (message loss); check ring capacity and error rates.")
    if "probe_samples" in df:
        thin = df[pd.to_numeric(df["probe_samples"], errors="coerce").fillna(0) < 1000]
        if len(thin):
            issues.append(
                f"{len(thin)} run(s) collected fewer than 1,000 probe samples; p99 is not "
                "estimable from these. Lengthen the measurement window for those cells."
            )
    counts = df.groupby("cell_id").size()
    short = counts[counts < MIN_RUNS_FOR_TESTS]
    if len(short) and len(short) == len(counts):
        issues.append(
            f"ALL {len(short)} cell(s) have fewer than {MIN_RUNS_FOR_TESTS} replications "
            f"(max {int(counts.max())}). Expected for pilot data; no inference will be attempted."
        )
    elif len(short):
        issues.append(f"{len(short)} cell(s) have fewer than {MIN_RUNS_FOR_TESTS} replications: {list(short.index)[:5]}")
    return issues


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
# Minimum run-level observations per arm before a comparison is attempted.
#
# Three is the floor at which a Mann-Whitney U test can return anything other
# than p = 1, so below it inference is not merely underpowered but undefined.
# The pilot runs two replications per cell by design -- its purpose is to
# determine parameters, not to produce findings (Section 3.7.4) -- so on pilot
# data this correctly yields no tests, and the analysis must say so rather than
# fail.
MIN_RUNS_FOR_TESTS = 3


def compare_group(df: pd.DataFrame, metric: str, group_cols=("workload", "tier", "mismatched")) -> pd.DataFrame:
    """
    Within each (workload, tier) group, run the omnibus Kruskal-Wallis across
    arms, then all pairwise Mann-Whitney U tests with Holm adjustment and A12.
    """
    out = []
    for keys, grp in df.groupby(list(group_cols)):
        arms = {a: g[metric].dropna().values for a, g in grp.groupby("arm")}
        arms = {a: v for a, v in arms.items() if len(v) >= MIN_RUNS_FOR_TESTS}
        if len(arms) < 2:
            continue

        # Omnibus. Non-parametric because latency is right-skewed and heavy-
        # tailed, violating the normality and equal-variance assumptions of ANOVA
        # (Section 1.8).
        if len(arms) >= 3:
            pooled = np.concatenate(list(arms.values()))
            if np.all(pooled == pooled[0]):
                # Every observation identical (common for error rate, which is
                # legitimately 0.0 everywhere below saturation). K-W is undefined
                # on fully tied data; reporting "no difference" is correct.
                h, p_omni, eta2 = float("nan"), 1.0, 0.0
                pairs = list(combinations(sorted(arms), 2))
                for a, b in pairs:
                    out.append(dict(
                        **dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,))),
                        metric=metric, arm_a=a, arm_b=b,
                        n_a=len(arms[a]), n_b=len(arms[b]),
                        median_a=float(np.median(arms[a])), median_b=float(np.median(arms[b])),
                        ratio=1.0, ratio_ci_lo=1.0, ratio_ci_hi=1.0,
                        u_statistic=float("nan"), p_unadjusted=1.0, p_holm=1.0,
                        significant_holm_05=False, a12=0.5, a12_magnitude="negligible",
                        practical="identical", kruskal_h=float("nan"),
                        kruskal_p=1.0, kruskal_eta2=0.0,
                    ))
                continue
            h, p_omni = stats.kruskal(*arms.values())
            # eta-squared for H, the standard ordinal effect size for K-W.
            n_tot = sum(len(v) for v in arms.values())
            eta2 = (h - len(arms) + 1) / (n_tot - len(arms)) if n_tot > len(arms) else float("nan")
        else:
            h, p_omni, eta2 = float("nan"), float("nan"), float("nan")

        pairs = list(combinations(sorted(arms), 2))
        raw_p, recs = [], []
        for a, b in pairs:
            va, vb = arms[a], arms[b]
            try:
                u, p = stats.mannwhitneyu(va, vb, alternative="two-sided")
            except ValueError:
                u, p = float("nan"), 1.0
            a12 = vargha_delaney_a12(va, vb)

            # Practical significance, kept SEPARATE from statistical significance.
            #
            # The synthetic validation of this pipeline exposed why this layer is
            # necessary. Because each run's percentile is estimated from tens of
            # thousands of messages, run-level estimates are very precise, so
            # between-run variance is small and A12 reaches 1.0 for differences
            # of a few per cent. A12 therefore answers "does A reliably exceed
            # B?" -- it does NOT answer "by enough to matter?". Reporting A12
            # alone would reproduce, in a new form, exactly the over-claiming
            # from trivial-but-significant differences that Section 2.6
            # criticises.
            #
            # So the magnitude question is answered by the ratio of medians with
            # a bootstrap interval, and the "does it matter" question by the
            # perceptual thresholds Section 1.2 adopts from Nielsen (1993):
            # ~100 ms is perceived as instantaneous, ~1 s breaks interaction flow.
            ratio_lo, ratio_hi = ratio_ci(va, vb)
            practical = classify_practical(np.median(va), np.median(vb), metric)
            raw_p.append(p)
            recs.append(dict(
                **dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,))),
                metric=metric, arm_a=a, arm_b=b,
                n_a=len(va), n_b=len(vb),
                median_a=float(np.median(va)), median_b=float(np.median(vb)),
                ratio=float(np.median(va) / np.median(vb)) if np.median(vb) else float("nan"),
                ratio_ci_lo=ratio_lo, ratio_ci_hi=ratio_hi,
                u_statistic=float(u), p_unadjusted=float(p),
                a12=float(a12), a12_magnitude=a12_magnitude(a12),
                practical=practical,
                kruskal_h=float(h), kruskal_p=float(p_omni), kruskal_eta2=float(eta2),
            ))

        for rec, padj in zip(recs, holm_correct(raw_p)):
            rec["p_holm"] = float(padj)
            rec["significant_holm_05"] = bool(padj < 0.05)
            out.append(rec)

    return pd.DataFrame(out)


def descriptives(df: pd.DataFrame, metrics: list[str]) -> pd.DataFrame:
    rows = []
    # `mismatched` is part of the grouping key, not an afterthought. The pilot
    # pooled matched (1,000 ms) and mismatched (5,000 ms) short-polling runs into
    # one cell and reported a coefficient of variation of 0.79 -- which was not
    # variance at all, but two different experimental conditions averaged
    # together. A polling interval is a factor, so it must key the grouping.
    for (arm, workload, tier, mm), grp in df.groupby(["arm", "workload", "tier", "mismatched"]):
        rec = dict(arm=arm, workload=workload, tier=tier, mismatched=mm, n_runs=len(grp))
        for m in metrics:
            if m not in grp:
                continue
            v = grp[m].dropna().values
            if len(v) == 0:
                continue
            lo, hi = bootstrap_ci(v, np.median)
            rec[f"{m}_median"] = float(np.median(v))
            rec[f"{m}_iqr"] = float(np.percentile(v, 75) - np.percentile(v, 25))
            rec[f"{m}_ci_lo"] = lo
            rec[f"{m}_ci_hi"] = hi
            # Coefficient of variation drives the repetition-count decision in
            # the pilot: it is what determines whether n = 10 is sufficient.
            mu = float(np.mean(v))
            rec[f"{m}_cv"] = float(np.std(v, ddof=1) / mu) if mu and len(v) > 1 else float("nan")
        rows.append(rec)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--out", type=Path, default=Path("analysis"))
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    df = load_runs(args.results)
    if df.empty:
        print("No completed runs found. Execute the sweep first:", file=sys.stderr)
        print("  python3 orchestrator/run_matrix.py --execute", file=sys.stderr)
        return 1

    df.to_csv(args.out / "runs.csv", index=False)
    print(f"Loaded {len(df)} runs across {df['cell_id'].nunique()} cells -> {args.out/'runs.csv'}")

    print("\n=== INTEGRITY ===")
    issues = integrity_report(df)
    if issues:
        for i in issues:
            print(f"  ! {i}")
    else:
        print("  all integrity checks passed")

    # Primary latency metric is the probe's, because it is the instrument with
    # adequate resolution; k6's is reported alongside as a cross-check.
    latency_metrics = ["probe_p50", "probe_p95", "probe_p99", "probe_mean",
                       "k6_lat_p95", "throughput_msg_s", "srv_rss_bytes",
                       "bytes_per_message", "empty_poll_fraction", "k6_error_rate"]
    available = [m for m in latency_metrics if m in df.columns and df[m].notna().any()]

    desc = descriptives(df, available)
    desc.to_csv(args.out / "descriptives.csv", index=False)
    print(f"\nDescriptives -> {args.out/'descriptives.csv'}")

    frames = [compare_group(df, m) for m in available]
    frames = [f for f in frames if not f.empty]
    tests = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if tests.empty:
        max_reps = int(df.groupby(["arm", "workload", "tier", "mismatched"]).size().max()) if len(df) else 0
        print(f"\n=== NO SIGNIFICANCE TESTS RUN ===")
        print(f"  Largest number of replications in any cell: {max_reps}")
        print(f"  Minimum required for inference:             {MIN_RUNS_FOR_TESTS}")
        print("  This is the expected outcome for pilot data. The pilot exists to")
        print("  determine parameters, not to produce findings (Section 3.7.4).")
        print("  Descriptives, coefficients of variation and the integrity report")
        print("  above are the pilot's actual output -- use them to set the")
        print("  measurement windows and replication count for the full sweep.")

    if not tests.empty:
        tests.to_csv(args.out / "significance_tests.csv", index=False)
        print(f"Significance tests -> {args.out/'significance_tests.csv'}")

        key = tests[tests["metric"] == "probe_p95"] if (tests["metric"] == "probe_p95").any() else tests
        print("\n=== p95 LATENCY, PAIRWISE (run-level, n per arm shown) ===")
        for _, r in key.sort_values(["workload", "tier"]).iterrows():
            star = "*" if r["significant_holm_05"] else " "
            print(f"  {star} {r['workload']:<13} c{int(r['tier']):<5} "
                  f"{r['arm_a']:>10} vs {r['arm_b']:<10} "
                  f"{r['median_a']:9.3f} vs {r['median_b']:9.3f} ms  "
                  f"ratio {r['ratio']:7.2f} [{r.get('ratio_ci_lo', float('nan')):.2f},{r.get('ratio_ci_hi', float('nan')):.2f}]  "
                  f"p_holm={r['p_holm']:.5f}  A12={r['a12']:.3f}  {r.get('practical','')}")
        print("\n  * = significant after Holm adjustment at 0.05.")
        print("  All unadjusted p-values are reported, per Section 2.6.")
        print("  n is the number of RUNS, not messages: see the module docstring.")
        print("\n  Read the three columns together, in this order:")
        print("    p_holm     is the difference real?")
        print("    A12        is it consistent across runs?")
        print("    ratio+band is it large enough to matter?")
        print("  A12 saturates at 1.0 for small-but-reliable differences, so it must")
        print("  not be read as a magnitude. See classify_practical().")

    print(f"\nDone. Outputs in {args.out}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
