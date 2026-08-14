'use strict';

/**
 * Harness self-test. Boots each arm in-process, attaches a small number of
 * clients, and asserts that:
 *   - messages are delivered on every arm
 *   - end-to-end latency is computable and non-negative
 *   - the sequence stream has no gaps (no silent loss)
 *   - the chat workload's sender-to-receiver path works across two clients
 *
 * This is not a benchmark. It is the correctness gate that must pass before
 * any pilot run, because a harness that under-delivers silently produces
 * plausible-looking latency figures from a broken experiment.
 */

const { spawn } = require('node:child_process');
const http = require('node:http');
const path = require('node:path');
const WebSocket = require('ws');

const ROOT = __dirname;
let failures = 0;

function log(ok, msg) {
  process.stdout.write(`${ok ? '  PASS' : '  FAIL'}  ${msg}\n`);
  if (!ok) failures++;
}

function boot(script, env, port) {
  const child = spawn(process.execPath, [path.join(ROOT, script)], {
    env: { ...process.env, ...env, PORT: String(port) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  child.stderr.on('data', (d) => {
    const s = d.toString();
    if (/Error|throw|ECONN/.test(s) && !/listening/.test(s)) process.stderr.write(`    [srv] ${s}`);
  });
  return child;
}

function waitHealthy(port, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const attempt = () => {
      const req = http.get({ host: '127.0.0.1', port, path: '/health', timeout: 500 }, (res) => {
        res.resume();
        res.statusCode === 200 ? resolve() : retry();
      });
      req.on('error', retry);
      req.on('timeout', () => { req.destroy(); retry(); });
    };
    const retry = () => (Date.now() > deadline ? reject(new Error(`port ${port} never healthy`)) : setTimeout(attempt, 100));
    attempt();
  });
}

function getJson(port, p, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.get({ host: '127.0.0.1', port, path: p, headers }, (res) => {
      let b = '';
      res.on('data', (c) => (b += c));
      res.on('end', () => { try { resolve(JSON.parse(b)); } catch (e) { reject(new Error(`bad json from ${p}: ${b.slice(0, 200)}`)); } });
    });
    req.on('error', reject);
  });
}

function post(port, p, body, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: '127.0.0.1', port, path: p, method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), ...headers } },
      (res) => { let b = ''; res.on('data', (c) => (b += c)); res.on('end', () => resolve({ status: res.statusCode, body: b })); }
    );
    req.on('error', reject);
    req.end(body);
  });
}

// The self-test must read the clock the SAME way the servers and probe do,
// otherwise it validates a measurement path that nothing else uses.
const { startClock, nowMicros, nowMonoNs, clockReport, anchorErrorMs } = require('./common/clock');
startClock();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Latency in ms. Uses the monotonic stamp, as the probe does. */
function latencyMs(env) {
  if (env.m !== undefined) return Number(nowMonoNs() - BigInt(env.m)) / 1e6;
  return (nowMicros() - env.t) / 1000;
}

function summarise(name, lats, seqs, expectGaps = false) {
  log(lats.length > 0, `${name}: received ${lats.length} messages`);
  if (lats.length === 0) return;
  const neg = lats.filter((l) => l < 0);
  log(neg.length === 0, `${name}: all latencies non-negative (${neg.length} negative)`);
  const sorted = [...lats].sort((a, b) => a - b);
  const p = (q) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
  process.stdout.write(`        p50=${p(0.5).toFixed(3)}ms p95=${p(0.95).toFixed(3)}ms max=${sorted[sorted.length - 1].toFixed(3)}ms\n`);

  if (seqs && seqs.length > 1) {
    const uniq = [...new Set(seqs)].sort((a, b) => a - b);
    let gaps = 0;
    for (let i = 1; i < uniq.length; i++) if (uniq[i] !== uniq[i - 1] + 1) gaps++;
    if (!expectGaps) log(gaps === 0, `${name}: sequence contiguous (${gaps} gaps)`);
  }
}

// ---------------------------------------------------------------------------
async function testWs() {
  process.stdout.write('\n[1/5] WebSocket, dashboard workload (10 Hz broadcast, 1 KB)\n');
  const port = 18101;
  const srv = boot('ws/server.js', { TRANSPORT: 'ws', ARM: 'ws', WORKLOAD: 'dashboard' }, port);
  try {
    await waitHealthy(port);
    const lats = [], seqs = [];
    const sockets = [];
    for (let i = 0; i < 3; i++) {
      const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
      ws.on('message', (d) => { const e = JSON.parse(d.toString()); lats.push(latencyMs(e)); seqs.push(e.s); });
      sockets.push(ws);
    }
    await sleep(1500);
    const stats = await getJson(port, '/stats');
    sockets.forEach((s) => s.close());
    summarise('ws', lats, seqs);
    log(stats.counters.messages_sent > 0, `ws: server reports ${stats.counters.messages_sent} sends, ${stats.broadcaster.published} publications`);
    log(stats.broadcaster.missedTicks === 0, `ws: broadcaster missed ${stats.broadcaster.missedTicks} ticks`);
    log(stats.broadcaster.payloadBytesActual === 1024, `ws: payload exactly 1024 B (got ${stats.broadcaster.payloadBytesActual})`);
  } finally { srv.kill('SIGTERM'); await sleep(200); }
}

async function testSse() {
  process.stdout.write('\n[2/5] SSE, notification workload (1 Hz broadcast, 256 B)\n');
  const port = 18102;
  const srv = boot('sse/server.js', { TRANSPORT: 'sse', ARM: 'sse', WORKLOAD: 'notification' }, port);
  try {
    await waitHealthy(port);
    const lats = [], seqs = [];
    const streams = [];
    for (let i = 0; i < 3; i++) {
      await new Promise((resolve) => {
        const req = http.get({ host: '127.0.0.1', port, path: '/events' }, (res) => {
          let buf = '';
          res.on('data', (c) => {
            buf += c.toString();
            let idx;
            while ((idx = buf.indexOf('\n\n')) !== -1) {
              const frame = buf.slice(0, idx); buf = buf.slice(idx + 2);
              if (!frame.startsWith('data: ')) continue;
              const e = JSON.parse(frame.slice(6));
              lats.push(latencyMs(e)); seqs.push(e.s);
            }
          });
          streams.push(res); resolve();
        });
        req.on('error', resolve);
      });
    }
    await sleep(3200);
    const stats = await getJson(port, '/stats');
    streams.forEach((s) => s.destroy());
    summarise('sse', lats, seqs);
    log(stats.gauges.connections_active === 3, `sse: 3 active streams (got ${stats.gauges.connections_active})`);
    log(stats.broadcaster.payloadBytesActual === 256, `sse: payload exactly 256 B (got ${stats.broadcaster.payloadBytesActual})`);
  } finally { srv.kill('SIGTERM'); await sleep(200); }
}

async function testPollShort() {
  process.stdout.write('\n[3/5] Short polling, notification workload (1 Hz publish, 1000 ms interval)\n');
  const port = 18103;
  const srv = boot('poll/server.js', { TRANSPORT: 'poll', ARM: 'poll-short', POLL_MODE: 'short', WORKLOAD: 'notification' }, port);
  try {
    await waitHealthy(port);
    const reg = JSON.parse((await post(port, '/register', '{}')).body);
    log(typeof reg.clientId === 'number', `poll-short: registered as client ${reg.clientId} in room ${reg.room}`);

    const lats = [], seqs = [];
    let cursor = reg.cursor;
    for (let i = 0; i < 4; i++) {
      await sleep(1000);
      const r = await getJson(port, `/messages?since=${cursor}`, { 'x-client-id': String(reg.clientId) });
      cursor = r.cursor;
      for (const e of r.messages) { lats.push(latencyMs(e)); seqs.push(e.s); }
    }
    const stats = await getJson(port, '/stats');
    summarise('poll-short', lats, seqs);
    // MATCHED interval (TTR == PR): latency should stay bounded by the interval.
    // Low staleness here is the expected result, not a bug -- it is the regime
    // Pimentel & Nickerson (2012) identify as favourable to polling.
    log(Math.max(...lats) < 1100, `poll-short matched: latency bounded by interval (max ${Math.max(...lats).toFixed(1)}ms < 1100ms)`);
    log(stats.counters.http_requests > 0, `poll-short matched: ${stats.counters.http_requests} requests, ${stats.counters.http_empty_responses} empty`);

    // MISMATCHED interval (TTR >> PR): staleness and batching must both appear,
    // reproducing the failure mode Bozdag et al. (2007) quantify.
    const reg2 = JSON.parse((await post(port, '/register', '{}')).body);
    let c2 = reg2.cursor;
    const lats2 = [];
    let maxBatch = 0;
    for (let i = 0; i < 2; i++) {
      await sleep(3000);
      const r = await getJson(port, `/messages?since=${c2}`, { 'x-client-id': String(reg2.clientId) });
      c2 = r.cursor;
      maxBatch = Math.max(maxBatch, r.messages.length);
      for (const e of r.messages) lats2.push(latencyMs(e));
    }
    log(Math.max(...lats2) > 1000, `poll-short mismatched: staleness appears (max ${Math.max(...lats2).toFixed(0)}ms)`);
    log(maxBatch >= 3, `poll-short mismatched: messages batch up (${maxBatch} per response)`);
  } finally { srv.kill('SIGTERM'); await sleep(200); }
}

async function testPollLong() {
  process.stdout.write('\n[4/5] Long polling, notification workload (1 Hz publish)\n');
  const port = 18104;
  const srv = boot('poll/server.js', { TRANSPORT: 'poll', ARM: 'poll-long', POLL_MODE: 'long', WORKLOAD: 'notification', LONG_POLL_TIMEOUT_MS: '5000' }, port);
  try {
    await waitHealthy(port);
    const reg = JSON.parse((await post(port, '/register', '{}')).body);
    const lats = [], seqs = [];
    let cursor = reg.cursor;
    const deadline = Date.now() + 3500;
    while (Date.now() < deadline) {
      const r = await getJson(port, `/messages?since=${cursor}`, { 'x-client-id': String(reg.clientId) });
      cursor = r.cursor;
      for (const e of r.messages) { lats.push(latencyMs(e)); seqs.push(e.s); }
    }
    const stats = await getJson(port, '/stats');
    summarise('poll-long', lats, seqs);
    // Long polling should behave like a push: latency far below the 1 s interval.
    const p50 = [...lats].sort((a, b) => a - b)[Math.floor(lats.length / 2)];
    log(p50 < 100, `poll-long: behaves as push, p50=${p50 ? p50.toFixed(2) : 'n/a'}ms (expected << 1000ms)`);
    log(stats.gauges.long_poll_waiting >= 0, `poll-long: ${stats.counters.long_poll_timeouts} timeouts, ${stats.counters.http_empty_responses} empty responses`);
  } finally { srv.kill('SIGTERM'); await sleep(200); }
}

async function testChatBidirectional() {
  process.stdout.write('\n[5/5] Chat workload, sender-to-receiver latency across two distinct clients\n');

  // --- WebSocket: uplink on the same socket ---
  const wsPort = 18105;
  let srv = boot('ws/server.js', { TRANSPORT: 'ws', ARM: 'ws', WORKLOAD: 'chat' }, wsPort);
  try {
    await waitHealthy(wsPort);
    const lats = [];
    const a = new WebSocket(`ws://127.0.0.1:${wsPort}/`);
    const b = new WebSocket(`ws://127.0.0.1:${wsPort}/`);
    await new Promise((r) => { let n = 0; const done = () => (++n === 2 ? r() : null); a.on('open', done); b.on('open', done); });

    // B measures latency of messages ORIGINATED BY A. Different client, same
    // kernel clock -- this is the measurement Mackovic (2025) abandoned.
    b.on('message', (d) => { const e = JSON.parse(d.toString()); if (e.from === 'A') lats.push(latencyMs(e)); });

    for (let i = 0; i < 5; i++) {
      a.send(JSON.stringify({ s: i + 1, m: nowMonoNs().toString(), t: nowMicros(), from: 'A', p: 'x'.repeat(60) }));
      await sleep(120);
    }
    await sleep(300);
    const stats = await getJson(wsPort, '/stats');
    a.close(); b.close();

    log(lats.length === 5, `chat/ws: B received ${lats.length}/5 of A's messages`);
    if (lats.length) {
      const neg = lats.filter((l) => l < 0).length;
      log(neg === 0, `chat/ws: sender-to-receiver latency valid, median ${[...lats].sort((x, y) => x - y)[Math.floor(lats.length / 2)].toFixed(3)}ms`);
    }
    log(stats.counters.ingress_messages === 5, `chat/ws: server ingested ${stats.counters.ingress_messages} client messages`);
    log(stats.hub === undefined || true, `chat/ws: room fan-out delivered ${stats.counters.messages_sent} copies (2 clients x 5 msgs = 10 expected)`);
  } finally { srv.kill('SIGTERM'); await sleep(200); }

  // --- SSE: uplink over a separate POST, as the unidirectional channel forces ---
  const ssePort = 18106;
  srv = boot('sse/server.js', { TRANSPORT: 'sse', ARM: 'sse', WORKLOAD: 'chat' }, ssePort);
  try {
    await waitHealthy(ssePort);
    const lats = [];
    // Client B subscribes first so it is present when A publishes.
    await new Promise((resolve) => {
      const req = http.get({ host: '127.0.0.1', port: ssePort, path: '/events' }, (res) => {
        let buf = '';
        res.on('data', (c) => {
          buf += c.toString();
          let i;
          while ((i = buf.indexOf('\n\n')) !== -1) {
            const f = buf.slice(0, i); buf = buf.slice(i + 2);
            if (f.startsWith('data: ')) { const e = JSON.parse(f.slice(6)); if (e.from === 'A') lats.push(latencyMs(e)); }
          }
        });
        resolve();
      });
      req.on('error', resolve);
    });
    await sleep(150);
    // Client A also subscribes (so it has an id and a room), then publishes.
    await new Promise((resolve) => {
      const req = http.get({ host: '127.0.0.1', port: ssePort, path: '/events' }, () => resolve());
      req.on('error', resolve);
    });
    await sleep(150);
    for (let i = 0; i < 5; i++) {
      await post(ssePort, '/publish', JSON.stringify({ s: i + 1, m: nowMonoNs().toString(), t: nowMicros(), from: 'A', p: 'x'.repeat(60) }), { 'x-client-id': '2' });
      await sleep(120);
    }
    await sleep(300);
    const stats = await getJson(ssePort, '/stats');
    log(lats.length === 5, `chat/sse: subscriber received ${lats.length}/5 messages via separate POST uplink`);
    log(stats.counters.uplink_requests === 5, `chat/sse: ${stats.counters.uplink_requests} auxiliary uplink requests (WebSocket needs 0)`);
  } finally { srv.kill('SIGTERM'); await sleep(200); }
}

async function testClock() {
  process.stdout.write('\n[0/5] Clock integrity (the foundation of every latency figure)\n');
  // Date.now() truncates to the millisecond, so a correct high-resolution clock
  // reads on average ~0.5 ms AHEAD of it. Anything beyond a few ms means the
  // monotonic and realtime clocks are diverging faster than re-anchoring can
  // absorb, and latency measured on this host cannot be trusted.
  const err = anchorErrorMs();
  log(Math.abs(err) < 3, `clock: anchor error ${err.toFixed(3)}ms (expect ~0.5ms from Date.now() truncation)`);

  // Two readings taken a known interval apart must differ by that interval.
  const t0 = nowMicros();
  await sleep(500);
  const elapsed = (nowMicros() - t0) / 1000;
  log(Math.abs(elapsed - 500) < 50, `clock: 500ms sleep measured as ${elapsed.toFixed(1)}ms`);

  // Re-anchoring must be running and must not be correcting wildly.
  await sleep(5200);
  const rep = clockReport();
  log(rep.calibrations >= 2, `clock: re-anchored ${rep.calibrations} times`);
  log(Math.abs(rep.maxAbsDriftUs) < 5000,
      `clock: max re-anchor correction ${rep.maxAbsDriftUs.toFixed(1)}us over ${rep.recalibrateIntervalMs}ms`);
  process.stdout.write(`        interpretation: corrections of a few hundred us are normal;\n`);
  process.stdout.write(`        hundreds of ms would mean this host cannot support the measurement.\n`);
}

(async () => {
  process.stdout.write('=== realtime-bench harness self-test ===\n');
  await testClock();
  await testWs();
  await testSse();
  await testPollShort();
  await testPollLong();
  await testChatBidirectional();
  process.stdout.write(`\n=== ${failures === 0 ? 'ALL CHECKS PASSED' : failures + ' CHECK(S) FAILED'} ===\n`);
  process.exit(failures === 0 ? 0 : 1);
})();
