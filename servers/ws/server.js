'use strict';

/**
 * ARM 1 of 4 -- WebSocket (RFC 6455).
 *
 * Dependencies: node:http (standard library) + ws, for RFC 6455 framing only.
 * No application framework, per Section 1.7. `ws` is used because implementing
 * correct framing, masking and close handshakes by hand would introduce a
 * bespoke, unaudited implementation as a confound -- the opposite of the
 * intent. It is the minimal choice that still exposes protocol-level
 * behaviour, and is the same choice Section 2.5.2 identifies as necessary to
 * avoid the framework confounding that compromised prior comparisons.
 *
 * perMessageDeflate is disabled explicitly: compression would alter
 * bytes-on-wire, which is a dependent variable.
 */

const http = require('node:http');
const { WebSocketServer } = require('ws');

const { startClock } = require('../common/clock');
const { config, describe } = require('../common/config');
const { Telemetry } = require('../common/telemetry');
const { Broadcaster } = require('../common/broadcaster');
const { Hub } = require('../common/hub');
const { makeControlPlane } = require('../common/control');

// Anchor the clock before any message can be stamped.
startClock();

const telemetry = new Telemetry({ transport: 'ws', runId: config.runId });
const hub = new Hub({ config, telemetry });

const server = http.createServer();
const control = makeControlPlane({ telemetry, broadcaster: null, onReset: () => {} });

server.on('request', (req, res) => {
  if (control(req, res)) return;
  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('not found\n');
});

// Nagle off on the listening socket's accepted connections. A 40 ms delayed
// ACK interaction here would dominate every latency figure in the study.
server.on('connection', (socket) => {
  if (config.noDelay) socket.setNoDelay(true);
});

const wss = new WebSocketServer({
  server,
  perMessageDeflate: config.compression,
  clientTracking: false,   // the Hub owns the registry; ws's own Set is redundant work
  maxPayload: 16 * 1024 * 1024,
});

wss.on('connection', (ws, req) => {
  if (config.noDelay && req.socket) req.socket.setNoDelay(true);

  const sub = hub.add({
    // The send path. This one line is the independent variable for this arm.
    send: (wire) => ws.send(wire),
    close: () => ws.close(1001, 'run over'),
  });

  ws.on('message', (data, isBinary) => {
    // Chat ingress. Arrives over the SAME socket -- no auxiliary request.
    if (config.direction !== 'bidirectional') return;
    const wire = isBinary ? data.toString('utf8') : data.toString();
    telemetry.inc('ingress_bytes', Buffer.byteLength(wire, 'utf8'));
    hub.publishFromClient({ senderId: sub.id, wire });
  });

  ws.on('close', () => hub.remove(sub.id));
  ws.on('error', () => {
    telemetry.inc('connection_errors');
    hub.remove(sub.id);
  });
});

wss.on('error', () => telemetry.inc('connection_errors'));

// Server-originated workloads only. For chat, serverRateHz is 0 and all
// traffic originates at the client.
let broadcaster = null;
if (config.serverRateHz > 0) {
  broadcaster = new Broadcaster({
    intervalMs: config.msgIntervalMs,
    payloadBytes: config.payloadBytes,
    onMessage: (_msg, wire) => hub.broadcast(wire),
  });
  broadcaster.start();
}

// Rebuild the control plane now that the broadcaster exists, so /metrics and
// /stats can report publication statistics.
const control2 = makeControlPlane({
  telemetry,
  broadcaster,
  onReset: () => { /* connections are torn down by the load generator */ },
});
server.removeAllListeners('request');
server.on('request', (req, res) => {
  if (control2(req, res)) return;
  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('not found\n');
});

server.listen(config.port, () => {
  process.stderr.write(`[ws] listening on ${config.port}\n${describe()}\n`);
});

function shutdown() {
  if (broadcaster) broadcaster.stop();
  hub.closeAll();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
