import { Trend, Counter, Rate, Gauge } from 'k6/metrics';

/**
 * Shared definitions for all four arms.
 *
 * Every metric name, stage schedule and latency computation lives here rather
 * than in the per-arm scripts, so that no arm can accidentally be measured on a
 * different basis from another. The per-arm scripts contain only the code that
 * opens a connection and receives a message.
 *
 * TIMEBASE NOTE
 * -------------
 * Every envelope carries two stamps (see servers/common/clock.js):
 *   m -- CLOCK_MONOTONIC ns, system-wide, never stepped. AUTHORITATIVE.
 *   t -- CLOCK_REALTIME us, steppable, quantised to 1 ms here.
 *
 * k6's JavaScript runtime exposes only Date.now(), so this file must use `t`.
 * That makes k6's latency figures vulnerable to two things the probe is not:
 * 1 ms quantisation (which can even yield small negative values, since
 * Date.now() truncates while the server stamps at microsecond resolution), and
 * realtime clock steps by the host. Both were observed during validation.
 *
 * k6 is therefore the cross-check, not the authority, for latency. It remains
 * authoritative for throughput, connection behaviour, error rates and bytes on
 * the wire, none of which depend on cross-process time comparison.
 *
 * RESOLUTION NOTE
 * ---------------
 * k6's Date.now() is quantised to 1 ms. That is adequate for the cross-family
 * contrasts (polling against the persistent architectures), where the effects
 * are one to three orders of magnitude larger, and for throughput, connection
 * and error metrics, for which k6 is the authority. It is NOT adequate for the
 * WebSocket-against-SSE contrast, which the harness self-test places well below
 * 1 ms. That contrast is measured by the microsecond probe clients running
 * concurrently (servers/probe.js). Both instruments are reported, and Chapter 3
 * states which is authoritative for which comparison.
 */

// --- dependent variables ---------------------------------------------------
export const e2eLatency = new Trend('rtb_e2e_latency_ms', true);
export const messagesReceived = new Counter('rtb_messages_received');
export const payloadBytes = new Counter('rtb_payload_bytes_received');
export const sequenceGaps = new Counter('rtb_sequence_gaps');
export const duplicateMessages = new Counter('rtb_duplicate_messages');
export const connectFailures = new Counter('rtb_connect_failures');
export const connectDuration = new Trend('rtb_connect_duration_ms', true);
export const deliveryErrors = new Rate('rtb_delivery_errors');
export const activeClients = new Gauge('rtb_active_clients');

// Polling-arm specific
export const pollRequests = new Counter('rtb_poll_requests');
export const emptyPolls = new Counter('rtb_poll_empty_responses');
export const pollBatchSize = new Trend('rtb_poll_batch_size');

// Chat-arm specific
export const uplinkRequests = new Counter('rtb_uplink_requests');
export const uplinkDuration = new Trend('rtb_uplink_duration_ms', true);

// --- configuration from the orchestrator ----------------------------------
export const cfg = {
  arm: __ENV.ARM || 'ws',
  host: __ENV.TARGET_HOST || 'localhost',
  port: __ENV.TARGET_PORT || '8080',
  workload: __ENV.WORKLOAD || 'notification',
  clients: parseInt(__ENV.CLIENTS || '10', 10),
  warmupS: parseInt(__ENV.WARMUP_S || '30', 10),
  durationS: parseInt(__ENV.DURATION_S || '300', 10),
  pollIntervalMs: parseInt(__ENV.POLL_INTERVAL_MS || '1000', 10),
  clientRateHz: parseFloat(__ENV.CLIENT_RATE_HZ || '0'),
  clientPayloadBytes: parseInt(__ENV.CLIENT_PAYLOAD_BYTES || '120', 10),
  cell: __ENV.CELL_ID || 'adhoc',
  replicate: parseInt(__ENV.REPLICATE || '0', 10),
};

export const baseUrl = `http://${cfg.host}:${cfg.port}`;
export const wsUrl = `ws://${cfg.host}:${cfg.port}/`;

/**
 * Scenario shape.
 *
 * A constant-VUs scenario, not a ramping arrival rate: the independent variable
 * is the number of SUSTAINED concurrent connections (10 / 100 / 1,000 per
 * Section 1.7), so the population must be held constant during the measurement
 * window rather than swept.
 *
 * `gracefulStop: 0` because a graceful stop would let VUs finish their current
 * iteration and continue emitting samples after the measurement window closes.
 *
 * The warm-up window is executed but its samples are discarded in analysis by
 * timestamp, and the discard boundary is written into the run manifest. V8's
 * JIT and heap growth make the first tens of seconds unrepresentative; the
 * pilot determines the boundary empirically rather than assuming one.
 */
export function scenarioOptions() {
  return {
    discardResponseBodies: false,
    scenarios: {
      steady: {
        executor: 'constant-vus',
        vus: cfg.clients,
        duration: `${cfg.warmupS + cfg.durationS}s`,
        gracefulStop: '0s',
      },
    },
    // Thresholds are declared so a run that breaches the pre-registered service
    // level is FLAGGED rather than silently averaged into the results. They do
    // not abort the run: a breach at the 1,000-client tier is a finding.
    thresholds: {
      'rtb_delivery_errors': ['rate<0.01'],
      'rtb_e2e_latency_ms': ['p(95)<250'],
    },
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'p(99.9)', 'max', 'count'],
    noConnectionReuse: false,   // keep-alive on: the realistic polling deployment
    userAgent: 'realtime-bench/1.0',
  };
}

/** Wall-clock ms. Quantised to 1 ms; see the resolution note above. */
export function nowMs() {
  return Date.now();
}

/**
 * Per-VU sequence tracking. Detects loss and duplication without retaining
 * every sequence number, which at 10,000 msg/s would grow unboundedly inside
 * the VU and perturb the generator.
 */
export function makeSeqTracker() {
  return { last: 0, started: false };
}

export function trackSequence(tracker, seq) {
  if (!tracker.started) { tracker.started = true; tracker.last = seq; return; }
  if (seq === tracker.last + 1) { tracker.last = seq; return; }
  if (seq > tracker.last + 1) { sequenceGaps.add(seq - tracker.last - 1); tracker.last = seq; return; }
  duplicateMessages.add(1);   // seq <= last: replay or reorder
}

/**
 * Record one delivered message. Called on every arm, so latency is defined
 * identically everywhere: receive instant minus the PUBLICATION instant carried
 * in the envelope.
 *
 * For polling this correctly includes the time the message waited in the server
 * log for the next poll. That waiting time is polling's inherent staleness
 * penalty and belongs in the latency figure -- excluding it would measure the
 * transfer only and flatter the mechanism.
 */
export function recordEnvelope(env, tracker) {
  // env.t is integer microseconds since the epoch, on CLOCK_REALTIME.
  // See the timebase note above for why this is the cross-check, not the
  // authority. The probe reads env.m instead.
  const latency = nowMs() - env.t / 1000;
  e2eLatency.add(latency);
  messagesReceived.add(1);
  deliveryErrors.add(false);
  if (tracker) trackSequence(tracker, env.s);
  return latency;
}

/** Build a client-originated chat message stamped with the SENDER's clock. */
export function buildClientMessage(seq) {
  // Client-originated chat messages must carry BOTH stamps, so that probe
  // clients (which read m) and k6 VUs (which read t) can each measure the
  // sender-to-receiver path on their own timebase.
  //
  // k6 cannot read CLOCK_MONOTONIC, so `m` is omitted here and recipients fall
  // back to `t` for k6-originated messages. The probe reports how often it fell
  // back, so the mixture is visible rather than silent. Chat latency measured by
  // the probe against PROBE-originated messages remains fully monotonic.
  const overhead = JSON.stringify({ s: seq, t: 1786400000000000, from: 0, p: '' }).length;
  const padLen = Math.max(0, cfg.clientPayloadBytes - overhead);
  return JSON.stringify({
    s: seq,
    t: Date.now() * 1000,       // microseconds, to match the server envelope
    from: __VU,
    p: 'x'.repeat(padLen),
  });
}

/** Poisson-ish inter-arrival for client-originated chat traffic, so the uplink
 *  is bursty rather than metronomic -- Section 1.7 specifies chat as bursty. */
export function nextChatDelayMs() {
  if (cfg.clientRateHz <= 0) return null;
  const meanMs = 1000 / cfg.clientRateHz;
  return -Math.log(1 - Math.random()) * meanMs;
}
