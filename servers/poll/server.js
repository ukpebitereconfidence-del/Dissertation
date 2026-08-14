'use strict';

/**
 * ARMS 3 and 4 of 4 -- HTTP short polling and HTTP long polling.
 *
 * Dependencies: node:http only.
 *
 * Both variants are served from one binary, selected by POLL_MODE. They share
 * every line of storage, cursor and room logic and differ only in whether an
 * empty result returns immediately or is held until data arrives or a timeout
 * fires. Serving them from one image guarantees that no incidental difference
 * in the surrounding code can be mistaken for a difference between the two
 * polling strategies -- which is the comparison Section 2.4 identifies as
 * conditional on the relationship between latency and message interval
 * (Pimentel & Nickerson, 2012).
 *
 * ARCHITECTURAL ASYMMETRY, stated deliberately
 * --------------------------------------------
 * The persistent arms push once per client per message. This arm writes each
 * message ONCE into a shared replay log and lets clients collect it. That is
 * not an optimisation applied unevenly -- it is what polling is. Modelling it
 * as a per-client push would import the persistent architectures' cost
 * structure into the polling arm and destroy the comparison.
 *
 * A cursor-based log also means polling loses no messages when the client is
 * slower than the publisher: it collects a batch instead. The staleness that
 * costs polling its latency shows up in the latency metric, where it belongs,
 * rather than as spurious message loss.
 */

const http = require('node:http');

const { startClock } = require('../common/clock');
const { config, describe } = require('../common/config');
const { Telemetry } = require('../common/telemetry');
const { Broadcaster } = require('../common/broadcaster');
const { Hub } = require('../common/hub');
const { makeControlPlane } = require('../common/control');

// Anchor the clock before any message can be stamped.
startClock();

const telemetry = new Telemetry({ transport: 'poll', runId: config.runId });
// The Hub is used here ONLY for room assignment and client bookkeeping; the
// send path is a no-op because delivery happens on the client's own request.
const hub = new Hub({ config, telemetry });

// ---------------------------------------------------------------------------
// Bounded replay log
// ---------------------------------------------------------------------------
const log = [];          // [{ seq, room, wire, bytes }]
let baseSeq = 1;         // seq of log[0]
let headSeq = 0;         // seq of the most recent entry
const waiters = new Map(); // clientId -> { res, since, room, timer, startedAt }

function append({ room, wire }) {
  headSeq += 1;
  log.push({ seq: headSeq, room, wire, bytes: Buffer.byteLength(wire, 'utf8') });
  if (log.length > config.ringCapacity) {
    const dropped = log.length - config.ringCapacity;
    log.splice(0, dropped);
    baseSeq += dropped;
    telemetry.inc('replay_entries_evicted', dropped);
  }
  wakeWaiters();
}

function collect({ since, room }) {
  if (headSeq <= since) return { messages: [], cursor: since, gapDetected: false };

  // A client whose cursor has fallen off the back of the bounded log has
  // genuinely lost messages. Report it rather than silently resyncing, because
  // an unreported gap would understate polling's failure mode under load.
  const gapDetected = since > 0 && since < baseSeq - 1;

  const from = Math.max(0, since - baseSeq + 1);
  const out = [];
  for (let i = from; i < log.length && out.length < config.maxBatch; i++) {
    const e = log[i];
    if (e.room === 'global' || e.room === room) out.push(e.wire);
  }

  const cursor = out.length > 0
    ? log[Math.min(from + out.length - 1, log.length - 1)].seq
    : headSeq;

  return { messages: out, cursor, gapDetected };
}

// Rotating wake order, for the same reason the Hub rotates its fan-out: the
// long-poll waiter served last waits for every response before it, so a fixed
// order would make position a systematic per-client latency offset and any
// instrumented subset a biased sample. See common/hub.js broadcast().
let wakeRotation = 0;

function wakeWaiters() {
  if (waiters.size === 0) return;
  const entries = Array.from(waiters.entries());
  const n = entries.length;
  const start = wakeRotation % n;
  wakeRotation = (wakeRotation + 1) % Math.max(1, n);

  for (let i = 0; i < n; i++) {
    const [clientId, w] = entries[(start + i) % n];
    if (!waiters.has(clientId)) continue;
    const { messages, cursor, gapDetected } = collect({ since: w.since, room: w.room });
    if (messages.length === 0) continue;
    clearTimeout(w.timer);
    waiters.delete(clientId);
    telemetry.set('long_poll_waiting', waiters.size);
    respond(w.res, { messages, cursor, gapDetected });
  }
}

function respond(res, { messages, cursor, gapDetected }) {
  const body = `{"cursor":${cursor},"gap":${gapDetected ? 'true' : 'false'},"messages":[${messages.join(',')}]}`;
  const bytes = Buffer.byteLength(body, 'utf8');
  res.writeHead(200, { 'content-type': 'application/json', 'content-length': bytes });
  res.end(body);

  if (messages.length === 0) telemetry.inc('http_empty_responses');
  telemetry.inc('messages_sent', messages.length);
  telemetry.inc('bytes_sent', bytes);
}

// ---------------------------------------------------------------------------
// Workload driver
// ---------------------------------------------------------------------------
let broadcaster = null;
if (config.serverRateHz > 0) {
  broadcaster = new Broadcaster({
    intervalMs: config.msgIntervalMs,
    payloadBytes: config.payloadBytes,
    onMessage: (_msg, wire) => append({ room: 'global', wire }),
  });
}

const control = makeControlPlane({
  telemetry,
  broadcaster,
  onReset: () => {
    log.length = 0;
    baseSeq = 1;
    headSeq = 0;
    for (const w of waiters.values()) { clearTimeout(w.timer); try { w.res.end(); } catch (_) {} }
    waiters.clear();
  },
});

function readBody(req, limit = 1 << 20) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(new Error('too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  if (control(req, res)) return;

  const url = new URL(req.url, 'http://localhost');
  telemetry.inc('http_requests');

  // ---- client registration ---------------------------------------------
  // Polling clients hold no connection, so identity and room membership must
  // be established explicitly. This costs one request per client per RUN, not
  // per message, so it does not distort the steady-state measurement.
  if (url.pathname === '/register' && req.method === 'POST') {
    const sub = hub.add({ send: () => {}, close: () => {} });
    const body = JSON.stringify({ clientId: sub.id, room: sub.room, cursor: headSeq });
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(body);
    return;
  }

  // ---- the poll --------------------------------------------------------
  if (url.pathname === '/messages' && req.method === 'GET') {
    const since = Number.parseInt(url.searchParams.get('since') || '0', 10);
    const clientId = Number.parseInt(req.headers['x-client-id'] || '0', 10);
    const sub = hub.subscribers.get(clientId);
    const room = sub ? sub.room : 'global';

    const result = collect({ since, room });

    if (result.messages.length > 0 || config.pollMode === 'short') {
      // SHORT POLLING: answer immediately, empty or not. The empty responses
      // are the mechanism's defining inefficiency (Bozdag et al., 2007) and are
      // counted, not hidden.
      respond(res, result);
      return;
    }

    // LONG POLLING: hold the request open.
    const timer = setTimeout(() => {
      waiters.delete(clientId);
      telemetry.set('long_poll_waiting', waiters.size);
      telemetry.inc('long_poll_timeouts');
      respond(res, { messages: [], cursor: headSeq, gapDetected: false });
    }, config.longPollTimeoutMs);

    // A client that disconnects mid-wait must not leak a timer or a response.
    const existing = waiters.get(clientId);
    if (existing) { clearTimeout(existing.timer); try { existing.res.end(); } catch (_) {} }

    waiters.set(clientId, { res, since, room, timer, startedAt: Date.now() });
    telemetry.set('long_poll_waiting', waiters.size);

    req.on('close', () => {
      const w = waiters.get(clientId);
      if (w && w.res === res) { clearTimeout(w.timer); waiters.delete(clientId); telemetry.set('long_poll_waiting', waiters.size); }
    });
    return;
  }

  // ---- the uplink -------------------------------------------------------
  if (url.pathname === '/publish' && req.method === 'POST') {
    telemetry.inc('uplink_requests');
    let body;
    try { body = await readBody(req); } catch (_) { res.writeHead(413).end(); return; }
    telemetry.inc('ingress_bytes', Buffer.byteLength(body, 'utf8'));

    const senderId = Number.parseInt(req.headers['x-client-id'] || '0', 10);
    const sub = hub.subscribers.get(senderId);
    append({ room: sub ? sub.room : 'global', wire: body });
    telemetry.inc('ingress_messages');

    res.writeHead(204).end();
    return;
  }

  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('not found\n');
});

server.on('connection', (socket) => {
  if (config.noDelay) socket.setNoDelay(true);
});

// Long-poll responses are held for up to LONG_POLL_TIMEOUT_MS, which must not
// be aborted by Node's own request timeout.
server.requestTimeout = 0;
server.headersTimeout = 0;
server.keepAliveTimeout = 72000;

server.listen(config.port, () => {
  if (broadcaster) broadcaster.start();
  process.stderr.write(`[poll:${config.pollMode}] listening on ${config.port}\n${describe()}\n`);
});

function shutdown() {
  if (broadcaster) broadcaster.stop();
  for (const w of waiters.values()) { clearTimeout(w.timer); try { w.res.end(); } catch (_) {} }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
