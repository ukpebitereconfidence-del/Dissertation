'use strict';

/**
 * ARM 2 of 4 -- Server-Sent Events (WHATWG HTML Living Standard).
 *
 * Dependencies: node:http only. The wire format is simple enough that no
 * library is warranted, so this arm is entirely framework-free.
 *
 * Two details carry methodological weight:
 *
 *  1. The channel is unidirectional, so the chat workload's uplink must travel
 *     over a separate POST /publish. That extra request is not an artefact of
 *     this implementation -- it is the structural constraint Section 2.3.2
 *     identifies, and measuring it is part of the point.
 *
 *  2. No compression, and an explicit flush per event. Node does not buffer
 *     res.write() beyond the socket, but any intermediary that did would
 *     silently convert this arm into a batching one.
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

const telemetry = new Telemetry({ transport: 'sse', runId: config.runId });
const hub = new Hub({ config, telemetry });

let broadcaster = null;
if (config.serverRateHz > 0) {
  broadcaster = new Broadcaster({
    intervalMs: config.msgIntervalMs,
    payloadBytes: config.payloadBytes,
    onMessage: (_msg, wire) => hub.broadcast(wire),
  });
}

const control = makeControlPlane({ telemetry, broadcaster, onReset: () => {} });

function readBody(req, limit = 1 << 20) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(new Error('body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const server = http.createServer(async (req, res) => {
  if (control(req, res)) return;

  const url = new URL(req.url, 'http://localhost');

  // ---- the event stream ------------------------------------------------
  if (url.pathname === '/events' && req.method === 'GET') {
    if (config.noDelay) req.socket.setNoDelay(true);
    // Never let the kernel or an intermediary idle this connection out.
    req.socket.setKeepAlive(true, 30000);
    req.socket.setTimeout(0);

    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',   // defensive: disables proxy buffering
    });
    // Comment line: opens the stream so the client's connection callback fires
    // deterministically rather than on the first real event.
    res.write(': ok\n\n');

    const sub = hub.add({
      // The send path. Two lines, and this is the independent variable.
      send: (wire) => res.write(`data: ${wire}\n\n`),
      close: () => res.end(),
    });

    // IDENTITY HANDSHAKE.
    //
    // For the bidirectional chat workload the client must publish over a separate
    // POST, and the server has to resolve which ROOM the sender belongs to. On
    // the WebSocket arm the sender's identity is a property of the socket, and on
    // the polling arms it comes from /register -- but an SSE client has no way to
    // learn the id the server assigned it, so its POSTs could not be attributed
    // to a room. The pilot recorded the consequence: all three sse-chat cells
    // delivered zero messages, because every publish resolved to a non-existent
    // room.
    //
    // The id is therefore announced as a named event on the stream. Named rather
    // than anonymous so that receivers can distinguish it from a payload frame:
    // an unnamed frame carrying a clientId would be parsed as a message envelope
    // and produce a nonsense latency sample.
    res.write(`event: hello\ndata: {"clientId":${sub.id},"room":"${sub.room}"}\n\n`);

    const drop = () => hub.remove(sub.id);
    req.on('close', drop);
    res.on('error', () => { telemetry.inc('connection_errors'); drop(); });
    return;
  }

  // ---- the uplink the unidirectional channel forces ---------------------
  if (url.pathname === '/publish' && req.method === 'POST') {
    telemetry.inc('http_requests');
    telemetry.inc('uplink_requests');
    let body;
    try {
      body = await readBody(req);
    } catch (_) {
      res.writeHead(413).end();
      return;
    }
    telemetry.inc('ingress_bytes', Buffer.byteLength(body, 'utf8'));

    // The client's subscriber id travels as a header so the server can resolve
    // the sender's room without parsing the payload it is about to relay.
    const senderId = Number.parseInt(req.headers['x-client-id'], 10);
    hub.publishFromClient({ senderId, wire: body });

    res.writeHead(204).end();
    return;
  }

  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('not found\n');
});

server.on('connection', (socket) => {
  if (config.noDelay) socket.setNoDelay(true);
});

// An SSE stream is a response that never ends; the default header timeout and
// keep-alive timeout would abort long-lived streams.
server.keepAliveTimeout = 0;
server.headersTimeout = 0;
server.requestTimeout = 0;

server.listen(config.port, () => {
  if (broadcaster) broadcaster.start();
  process.stderr.write(`[sse] listening on ${config.port}\n${describe()}\n`);
});

function shutdown() {
  if (broadcaster) broadcaster.stop();
  hub.closeAll();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
