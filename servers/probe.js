'use strict';

/**
 * HIGH-RESOLUTION PROBE CLIENT
 *
 * Why this exists
 * ---------------
 * The load generator (k6) drives concurrency, but its JavaScript clock is
 * quantised to 1 ms. The self-test measures the WebSocket-to-SSE latency
 * difference at well under 1 ms on an idle host, so a 1 ms-quantised instrument
 * cannot resolve the single contrast this study exists to establish -- the
 * relative latency of Server-Sent Events, which Section 2.7 identifies as never
 * having been measured. Mackovic (2025) abandoned exactly this measurement.
 *
 * Quantisation at 1 ms is unbiased for the MEAN (the error is symmetric about
 * zero) but distorts PERCENTILES, which is fatal because percentiles are this
 * study's headline statistic. Averaging more samples does not fix a percentile.
 *
 * The solution is a small number of instrumented probe clients running
 * CONCURRENTLY with the k6 load, on the same shared kernel clock, stamping
 * arrivals at microsecond resolution. The probes contribute a negligible
 * fraction of offered load (default 10 of 10-1,000 clients) but produce the
 * fine-grained latency distribution. k6 remains the authority on throughput,
 * connection behaviour and error rates.
 *
 * This is a standard separation in network measurement: the load generator and
 * the instrument need not be the same component, and are better when they are
 * not.
 *
 * Usage:
 *   node probe.js --arm ws --host localhost --port 8080 --duration 300
 *                 --clients 10 --workload dashboard --out /results/probe.json
 */

const http = require('node:http');
const fs = require('node:fs');
const WebSocket = require('ws');

// Periodically re-anchored realtime clock. Using
// performance.timeOrigin + performance.now() here instead would freeze this
// process's clock error at start-up and contaminate every latency sample with
// the difference between it and the server's error. See common/clock.js.
const { startClock, nowMicros, nowMonoNs, clockReport, anchorErrorMs } = require('./common/clock');
startClock();

// ---------------------------------------------------------------------------
function parseArgs(argv) {
  const out = {
    arm: 'ws', host: '127.0.0.1', port: 8080, duration: 60,
    clients: 10, workload: 'notification', out: null,
    pollInterval: null, warmup: 0, cell: 'adhoc', replicate: 0,
    clientRate: 0, clientPayload: 120,
  };
  for (let i = 2; i < argv.length; i += 2) {
    const k = argv[i].replace(/^--/, '');
    const v = argv[i + 1];
    if (out[k] === undefined) throw new Error(`unknown flag --${k}`);
    out[k] = typeof out[k] === 'number' ? Number(v) : v;
  }
  return out;
}
const args = parseArgs(process.argv);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Sample collection.
//
// Latencies are accumulated as a plain Float64Array, pre-allocated. A growing
// JS array would trigger repeated reallocation and GC inside the very process
// that is timing microsecond intervals -- the instrument would perturb its own
// measurement. When the buffer fills the probe stops recording and reports the
// truncation, rather than reallocating mid-run.
// ---------------------------------------------------------------------------
const CAPACITY = 4_000_000;
const samples = new Float64Array(CAPACITY);
let n = 0;
let truncated = 0;
let warmupUntil = 0;

// Sequence tracking must be PER CLIENT. Every probe client subscribes to the
// same broadcast, so a message with sequence n legitimately arrives once at each
// of them. Tracking sequences in one shared set counted those as duplicates and
// reported 80 duplicates out of 100 samples for a perfectly healthy run --
// an integrity gate that fires on correct behaviour is worse than no gate.
let duplicates = 0;
let negatives = 0;
let uniqueSequences = 0;

function makeTracker() {
  return { seen: new Set() };
}

function trackSeq(tracker, seq) {
  if (seq === undefined) return;
  if (tracker.seen.has(seq)) duplicates++;
  else { tracker.seen.add(seq); uniqueSequences++; }
}

function record(latencyMs, seq, tracker) {
  // Warm-up samples are discarded, not merely flagged, so that no downstream
  // analysis can accidentally include them.
  if (nowMicros() < warmupUntil) return;
  if (latencyMs < 0) negatives++;
  if (n < CAPACITY) samples[n++] = latencyMs; else truncated++;
  trackSeq(tracker, seq);
}

let realtimeFallbacks = 0;

/**
 * Build a client-originated chat message carrying the MONOTONIC stamp.
 *
 * The pilot found that 18 of 78 runs fell back to CLOCK_REALTIME, all of them
 * chat cells. The cause is structural: in the chat workload every message
 * originates at a client, and the load generator cannot read CLOCK_MONOTONIC, so
 * k6-originated messages carry only the steppable realtime stamp. Chat was
 * therefore the one workload whose latency was not measured on the sound
 * timebase.
 *
 * Having the probes originate their own chat traffic fixes this at the source:
 * probe-to-probe messages carry `m` and are measured exactly like every other
 * workload. Messages originating at the load generator are still received, but
 * are now EXCLUDED from the latency distribution rather than silently degrading
 * it -- they are counted separately so the exclusion is visible.
 */
function buildChatMessage(seq) {
  const skeleton = JSON.stringify({ s: seq, m: '000000000000000', t: 0, from: 'probe', p: '' });
  const padLen = Math.max(0, args.clientPayload - Buffer.byteLength(skeleton, 'utf8'));
  return JSON.stringify({
    s: seq,
    m: nowMonoNs().toString(),
    t: nowMicros(),
    from: 'probe',
    p: 'x'.repeat(padLen),
  });
}

/** Exponential inter-arrival, matching the load generator's chat model. */
function nextChatDelayMs() {
  if (args.clientRate <= 0) return null;
  return -Math.log(1 - Math.random()) * (1000 / args.clientRate);
}

function onEnvelope(env, tracker) {
  // Prefer the monotonic stamp. It is system-wide, never stepped, and read from
  // the same kernel clock the server read -- so the subtraction is exact with no
  // synchronisation term. Fall back to the realtime stamp only if `m` is absent,
  // and count it, because a silent fallback would reintroduce step vulnerability
  // without saying so.
  if (env.m === undefined) {
    // Load-generator-originated message: realtime stamp only. Counted, but NOT
    // admitted to the latency distribution -- a step-vulnerable sample mixed in
    // with sound ones would contaminate the percentiles it is pooled with.
    realtimeFallbacks++;
    return;
  }
  const lat = Number(nowMonoNs() - BigInt(env.m)) / 1e6;
  record(lat, env.s, tracker);
}

// ---------------------------------------------------------------------------
// Arms
// ---------------------------------------------------------------------------
async function runWs() {
  const sockets = [];
  for (let i = 0; i < args.clients; i++) {
    const tracker = makeTracker();
    const ws = new WebSocket(`ws://${args.host}:${args.port}/`, { perMessageDeflate: false });
    ws.on('message', (d) => { try { onEnvelope(JSON.parse(d.toString()), tracker); } catch (_) {} });
    ws.on('error', () => {});
    if (args.clientRate > 0) {
      let seq = 0;
      const pump = () => {
        if (ws.readyState !== 1) return;
        ws.send(buildChatMessage(++seq));
        setTimeout(pump, nextChatDelayMs()).unref?.();
      };
      ws.on('open', () => setTimeout(pump, nextChatDelayMs()));
    }
    sockets.push(ws);
  }
  await sleep(args.duration * 1000);
  sockets.forEach((s) => { try { s.close(); } catch (_) {} });
}

async function runSse() {
  const streams = [];
  for (let i = 0; i < args.clients; i++) {
    const tracker = makeTracker();
    let clientId = null;   // assigned by the server's hello event
    await new Promise((resolve) => {
      const req = http.get(
        { host: args.host, port: args.port, path: '/events', agent: false, headers: { accept: 'text/event-stream' } },
        (res) => {
          res.setEncoding('utf8');
          let buf = '';
          res.on('data', (chunk) => {
            buf += chunk;
            let idx;
            while ((idx = buf.indexOf('\n\n')) !== -1) {
              const frame = buf.slice(0, idx);
              buf = buf.slice(idx + 2);

              // Parse the frame properly rather than assuming a bare data line:
              // the identity handshake arrives as a NAMED event, so a frame may
              // carry both an event field and a data field.
              let evName = null;
              let data = null;
              for (const line of frame.split('\n')) {
                if (line.startsWith('event: ')) evName = line.slice(7).trim();
                else if (line.startsWith('data: ')) data = line.slice(6);
              }
              if (data === null) continue;

              if (evName === 'hello') {
                try { clientId = JSON.parse(data).clientId; } catch (_) {}
                continue;   // never a latency sample
              }
              try { onEnvelope(JSON.parse(data), tracker); } catch (_) {}
            }
          });
          streams.push(res);
          if (args.clientRate > 0) {
            let seq = 0;
            const pump = () => {
              // The id assigned by the hello event MUST accompany the publish.
              // Without it the server cannot resolve the sender's room, and in the
              // chat workload there is no global room to fall back to, so the
              // message reaches nobody. The pilot recorded this as zero delivered
              // messages across all three sse-chat cells.
              const h = clientId === null ? {} : { 'x-client-id': String(clientId) };
              postJson('/publish', buildChatMessage(++seq), h).catch(() => {});
              const t = setTimeout(pump, nextChatDelayMs());
              if (t.unref) t.unref();
            };
            const t0 = setTimeout(pump, nextChatDelayMs());
            if (t0.unref) t0.unref();
          }
          resolve();
        }
      );
      req.on('error', resolve);
      req.setNoDelay(true);
    });
  }
  await sleep(args.duration * 1000);
  streams.forEach((s) => { try { s.destroy(); } catch (_) {} });
}

function postJson(path, body, headers) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: args.host, port: args.port, path, method: 'POST',
        headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), ...headers } },
      (res) => { let b = ''; res.on('data', (c) => (b += c)); res.on('end', () => resolve(b)); }
    );
    req.on('error', reject);
    req.end(body);
  });
}

function getJson(path, headers) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: args.host, port: args.port, path, headers, agent: pollAgent }, (res) => {
      let b = '';
      res.on('data', (c) => (b += c));
      res.on('end', () => { try { resolve(JSON.parse(b)); } catch (e) { reject(e); } });
    });
    req.on('error', reject);
  });
}

// Keep-alive agent for the polling arms. Without it every poll pays a fresh TCP
// handshake, which is a configuration choice rather than a property of polling,
// and would overstate its cost. Keep-alive on is the realistic deployment.
const pollAgent = new http.Agent({ keepAlive: true, maxSockets: 4096, maxFreeSockets: 4096 });

async function runPoll(mode) {
  const deadline = Date.now() + args.duration * 1000;
  const interval = args.pollInterval;

  const client = async () => {
    const tracker = makeTracker();
    let reg;
    try { reg = JSON.parse(await postJson('/register', '{}', {})); } catch (_) { return; }
    let cursor = reg.cursor;
    const hdr = { 'x-client-id': String(reg.clientId) };

    // Randomised initial poll phase. See the equivalent comment in
    // loadgen/scripts/poll.js: without this, every probe client shares one phase
    // relative to the publication tick and the run measures that arbitrary
    // offset rather than the mechanism. Long polling is exempt -- the server
    // holds the request, so there is no phase to stagger.
    if (mode === 'short') {
      await sleep(Math.random() * interval);
    }

    // Chat origination runs on its own TIMER, independent of the poll loop.
    //
    // Interleaving it into the loop deadlocks the long-polling arm: that client
    // blocks inside its request until data arrives, so it never reaches the point
    // where it would publish, so no data ever arrives. This is the same failure
    // shape as the SSE arm's event-driven uplink, and the pilot would have shown
    // it as zero samples in every poll-long chat cell. A client's ability to SEND
    // must not be contingent on its having RECEIVED.
    let sendSeq = 0;
    let sendTimer = null;
    if (args.clientRate > 0) {
      const pump = () => {
        if (Date.now() >= deadline) return;
        postJson('/publish', buildChatMessage(++sendSeq), hdr).catch(() => {});
        sendTimer = setTimeout(pump, nextChatDelayMs());
        if (sendTimer.unref) sendTimer.unref();
      };
      sendTimer = setTimeout(pump, nextChatDelayMs());
      if (sendTimer.unref) sendTimer.unref();
    }

    while (Date.now() < deadline) {
      const t0 = Date.now();
      try {
        const r = await getJson(`/messages?since=${cursor}`, hdr);
        cursor = r.cursor;
        for (const env of r.messages) onEnvelope(env, tracker);
      } catch (_) { /* counted by k6; the probe only measures latency */ }

      if (mode === 'short') {
        // Sleep the REMAINDER of the interval, not the whole interval. Sleeping
        // a fixed interval after a request that itself took time would make the
        // effective poll rate a function of server response time, so a slow
        // server would be polled less often -- flattering it.
        const elapsed = Date.now() - t0;
        const wait = Math.max(0, interval - elapsed);
        if (wait > 0) await sleep(wait);
      }
      // Long polling reissues immediately; the server holds the request.
    }
    if (sendTimer) clearTimeout(sendTimer);
  };

  await Promise.all(Array.from({ length: args.clients }, client));
}

// ---------------------------------------------------------------------------
// Statistics. Percentiles are computed on the sorted sample; no interpolation,
// nearest-rank, stated explicitly so the figures are reproducible.
// ---------------------------------------------------------------------------
function summarise() {
  const view = Array.prototype.slice.call(samples.subarray(0, n));
  view.sort((a, b) => a - b);

  const pct = (q) => (view.length === 0 ? null : view[Math.min(view.length - 1, Math.ceil(q * view.length) - 1)]);
  const mean = view.length ? view.reduce((a, b) => a + b, 0) / view.length : null;
  const variance = view.length > 1
    ? view.reduce((acc, x) => acc + (x - mean) ** 2, 0) / (view.length - 1)
    : null;

  return {
    arm: args.arm,
    workload: args.workload,
    cell: args.cell,
    replicate: args.replicate,
    clients: args.clients,
    durationS: args.duration,
    pollIntervalMs: args.pollInterval,
    resolution: 'nanosecond (CLOCK_MONOTONIC, system-wide; see common/clock.js)',
    clock: clockReport(),
    samples: view.length,
    truncated,
    negativeLatencies: negatives,
    excludedRealtimeOnly: realtimeFallbacks,
    timebase: 'CLOCK_MONOTONIC (system-wide)',
    timebaseNote: realtimeFallbacks > 0
      ? `${realtimeFallbacks} load-generator-originated message(s) received and EXCLUDED (realtime stamp only)`
      : 'all received messages carried a monotonic stamp',
    duplicateSequences: duplicates,
    uniqueSequences,
    latencyMs: {
      mean,
      sd: variance === null ? null : Math.sqrt(variance),
      min: pct(0),
      p50: pct(0.5),
      p90: pct(0.9),
      p95: pct(0.95),
      p99: pct(0.99),
      p999: pct(0.999),
      max: view.length ? view[view.length - 1] : null,
    },
  };
}

// ---------------------------------------------------------------------------
// Persist results on termination as well as on completion.
//
// The load generator and the probe are separate containers with independently
// scheduled end times. Whichever finishes first, the other may be signalled
// before it reaches its normal exit. A probe that only wrote its output at the
// end of a clean run would silently contribute nothing to those runs -- and the
// loss would be intermittent, which is worse than systematic.
let finished = false;

function persist(reason) {
  if (finished) return;
  finished = true;
  const result = summarise();
  result.terminationReason = reason;
  const json = JSON.stringify(result, null, 2);
  if (args.out) {
    try { fs.writeFileSync(args.out, json); } catch (err) {
      process.stderr.write(`probe: failed to write ${args.out}: ${err.message}\n`);
    }
  }
  process.stdout.write(json + '\n');
}

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => { persist(`signal:${sig}`); process.exit(0); });
}

(async () => {
  warmupUntil = nowMicros() + args.warmup * 1_000_000;

  switch (args.arm) {
    case 'ws': await runWs(); break;
    case 'sse': await runSse(); break;
    case 'poll-short': await runPoll('short'); break;
    case 'poll-long': await runPoll('long'); break;
    default: throw new Error(`unknown arm "${args.arm}"`);
  }

  persist('completed');
  process.exit(0);
})().catch((err) => {
  persist('error');
  process.stderr.write(`probe failed: ${err.stack}\n`);
  process.exit(1);
});
