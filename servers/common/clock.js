'use strict';

/**
 * HIGH-RESOLUTION REALTIME CLOCK, PERIODICALLY RE-ANCHORED
 *
 * The problem this solves
 * ----------------------
 * The obvious way to get a sub-millisecond wall-clock reading in Node is
 *
 *     performance.timeOrigin + performance.now()
 *
 * and it is wrong for this application. `timeOrigin` is captured ONCE, when the
 * process starts; `performance.now()` counts from it on the MONOTONIC clock. So
 * the expression equals true wall-clock time only for as long as the monotonic
 * and realtime clocks agree. Whenever they diverge -- an NTP adjustment, a
 * virtual machine resume, or simply a monotonic clock that ticks at a slightly
 * different rate -- the error is frozen into that process and grows with its
 * uptime.
 *
 * Two processes started at different moments therefore acquire DIFFERENT fixed
 * offsets, and a latency computed between them is contaminated by the
 * difference. This was observed directly during harness validation: on a WSL2
 * host, the server and probe disagreed by roughly 900 ms on the same messages,
 * while the standard deviation of the samples remained near 1 ms. A large mean
 * with a tiny spread is the signature of a constant offset, not of latency.
 *
 * The irony is instructive and is recorded in Chapter 4: Section 3.4.1 argues
 * that co-locating every process on one kernel makes the inter-clock offset zero
 * by construction. That argument is sound -- there is only one CLOCK_REALTIME --
 * but it holds only if every process actually READS that clock. Anchoring to it
 * once and then extrapolating on a different clock quietly reintroduces exactly
 * the synchronisation error the design was supposed to eliminate.
 *
 * THE STRONGER SOLUTION, and why there are two timestamps
 * -------------------------------------------------------
 * Re-anchoring (below) bounds the error but cannot eliminate it: if the realtime
 * clock is STEPPED -- as WSL2 does when it re-syncs to the Windows host -- every
 * message stamped between the step and the next calibration carries the stale
 * offset. This was observed during validation: a 909 ms step left roughly ten
 * per cent of a 25-second run reporting 909 ms of latency, while the median
 * stayed correct at 0.59 ms.
 *
 * CLOCK_MONOTONIC has no such failure mode. On Linux it counts from boot, is
 * never stepped by NTP, and -- critically for this design -- is SYSTEM-WIDE
 * rather than per-process: every process on the kernel reads the same value.
 * Verified against /proc/uptime. Since Section 3.4.1's containers share one
 * kernel, two processes reading it share a timebase exactly, with no
 * synchronisation step and nothing to drift.
 *
 * Every message therefore carries two stamps:
 *
 *   m : CLOCK_MONOTONIC nanoseconds  -- AUTHORITATIVE. Used by the probe, which
 *       reads the same clock. Immune to steps, adjustments and drift.
 *   t : CLOCK_REALTIME microseconds  -- used by k6, whose JavaScript runtime
 *       exposes only Date.now(). Retained as an independent cross-check, with
 *       the understanding that it is vulnerable to steps and quantised to 1 ms.
 *
 * When the two instruments agree, that agreement is evidence. When they diverge
 * while the monotonic series stays clean, the divergence localises a realtime
 * clock step -- which is itself a reportable property of the host.
 *
 * The re-anchored realtime clock below is retained because `t` still has to be
 * produced, and re-anchoring makes it as good as a realtime stamp can be.
 *
 * The solution
 * ------------
 * Re-anchor. Calibration finds a precise realtime millisecond boundary by
 * spinning until Date.now() ticks over, captures the monotonic counter at that
 * instant, and stores the difference. Readings are then monotonic counter plus
 * offset, which gives nanosecond resolution on a value that tracks realtime.
 * Repeating the calibration on a short interval bounds the accumulated error to
 * whatever the clocks can diverge by within that interval.
 *
 * Because the server and the probe run identical code against the same kernel
 * clock, their offsets agree to within calibration precision (a few
 * microseconds), and the difference between their readings is real latency.
 *
 * Self-checking
 * -------------
 * Each calibration records how far the previous anchor had drifted. That series
 * is exported and archived with every run, so clock quality is a reported
 * measurement rather than an assumption -- and a host on which this measurement
 * is untenable announces itself instead of producing plausible wrong numbers.
 */

// 1 s, not 5 s. Re-anchoring only protects the REALTIME stamp `t`; the
// authoritative monotonic stamp `m` needs no protection. Validation on a WSL2
// host observed CLOCK_REALTIME steps of roughly 950 ms occurring within a
// 25-second run, so a 5-second window left a large fraction of `t` values
// stale. A 1-second window bounds that exposure at the cost of one sub-
// millisecond calibration spin per second, which is identical on every arm and
// therefore cannot bias the comparison.
const RECALIBRATE_MS = 1000;

let offsetNs = 0n;          // realtime_ns - monotonic_ns
let calibrated = false;
let timer = null;

const drift = {
  calibrations: 0,
  lastDriftUs: 0,
  maxAbsDriftUs: 0,
  cumulativeAbsDriftUs: 0,
};

/**
 * Establish the offset between the monotonic counter and CLOCK_REALTIME.
 *
 * Date.now() has 1 ms granularity, so reading it once would anchor with up to
 * 1 ms of error. Spinning until it CHANGES locates an exact millisecond
 * boundary, reducing the anchor error to the duration of one loop iteration --
 * tens of nanoseconds. The spin costs under 1 ms and runs once every
 * RECALIBRATE_MS, so its cost is negligible and, being identical on every arm,
 * cannot bias the comparison.
 */
function calibrate() {
  const start = Date.now();
  let boundary;
  do {
    boundary = Date.now();
  } while (boundary === start);

  const mono = process.hrtime.bigint();
  const newOffset = BigInt(boundary) * 1000000n - mono;

  if (calibrated) {
    const deltaUs = Number(newOffset - offsetNs) / 1000;
    drift.lastDriftUs = deltaUs;
    drift.cumulativeAbsDriftUs += Math.abs(deltaUs);
    if (Math.abs(deltaUs) > Math.abs(drift.maxAbsDriftUs)) drift.maxAbsDriftUs = deltaUs;
  }

  offsetNs = newOffset;
  calibrated = true;
  drift.calibrations += 1;
}

/** Start periodic re-anchoring. Safe to call more than once. */
function startClock() {
  if (!calibrated) calibrate();
  if (timer) return;
  timer = setInterval(calibrate, RECALIBRATE_MS);
  if (timer.unref) timer.unref();   // must never hold the process open
}

/**
 * CLOCK_MONOTONIC in nanoseconds. System-wide on Linux, so this value is
 * directly comparable between any two processes on the same kernel, and is
 * never adjusted by NTP. This is the authoritative timebase for latency.
 */
function nowMonoNs() {
  return process.hrtime.bigint();
}

/** Current realtime as an integer count of microseconds since the epoch. */
function nowMicros() {
  if (!calibrated) calibrate();
  return Number((process.hrtime.bigint() + offsetNs) / 1000n);
}

/** Current realtime in milliseconds, with sub-millisecond resolution. */
function nowMs() {
  return nowMicros() / 1000;
}

/**
 * How far this process's clock reading currently sits from Date.now().
 * Should stay within a few hundred microseconds. A large value means the
 * monotonic and realtime clocks are diverging faster than RECALIBRATE_MS
 * accommodates, and latency figures from this host are not trustworthy.
 */
function anchorErrorMs() {
  return nowMs() - Date.now();
}

function clockReport() {
  return {
    recalibrateIntervalMs: RECALIBRATE_MS,
    calibrations: drift.calibrations,
    lastDriftUs: drift.lastDriftUs,
    maxAbsDriftUs: drift.maxAbsDriftUs,
    meanAbsDriftUs: drift.calibrations > 1
      ? drift.cumulativeAbsDriftUs / (drift.calibrations - 1)
      : 0,
    anchorErrorMs: anchorErrorMs(),
  };
}

module.exports = { startClock, nowMicros, nowMs, nowMonoNs, calibrate, clockReport, anchorErrorMs };
