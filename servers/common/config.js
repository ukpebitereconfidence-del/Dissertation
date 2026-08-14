'use strict';

const { getProfile } = require('./workloads');

/**
 * Single source of truth for every tunable that could confound the comparison.
 * All four arms import this, so a given run's configuration is provably
 * identical across arms except for TRANSPORT itself. The orchestrator archives
 * GET /config with each run, so every result file is self-documenting.
 */

function int(name, dflt) {
  const v = process.env[name];
  if (v === undefined || v === '') return dflt;
  const n = Number.parseInt(v, 10);
  if (Number.isNaN(n)) throw new Error(`env ${name}="${v}" is not an integer`);
  return n;
}

function num(name, dflt) {
  const v = process.env[name];
  if (v === undefined || v === '') return dflt;
  const n = Number(v);
  if (Number.isNaN(n)) throw new Error(`env ${name}="${v}" is not a number`);
  return n;
}

function bool(name, dflt) {
  const v = process.env[name];
  if (v === undefined || v === '') return dflt;
  return v === '1' || v.toLowerCase() === 'true';
}

function str(name, dflt) {
  const v = process.env[name];
  return v === undefined || v === '' ? dflt : v;
}

const workloadName = str('WORKLOAD', 'notification');
const profile = getProfile(workloadName);

const config = {
  // --- identity -----------------------------------------------------------
  transport: str('TRANSPORT', 'unknown'),   // ws | sse | poll
  arm: str('ARM', str('TRANSPORT', 'unknown')), // ws | sse | poll-short | poll-long
  port: int('PORT', 8080),
  runId: str('RUN_ID', 'adhoc'),
  cell: str('CELL_ID', 'adhoc'),
  replicate: int('REPLICATE', 0),

  // --- workload -----------------------------------------------------------
  workload: workloadName,
  direction: profile.direction,
  fanout: str('FANOUT', profile.fanout),        // broadcast | room
  roomSize: int('ROOM_SIZE', profile.roomSize || 20),
  payloadBytes: int('PAYLOAD_BYTES', profile.payloadBytes),
  serverRateHz: num('SERVER_RATE_HZ', profile.serverRateHz),

  // --- transport-neutral socket settings ---------------------------------
  // Nagle's algorithm can inject ~40 ms of artificial delay on small writes
  // and would silently invalidate every latency figure in the study. It is
  // disabled on ALL arms, unconditionally, and the setting is asserted by
  // the environment-validation script rather than assumed.
  noDelay: bool('TCP_NODELAY', true),
  // Compression alters bytes-on-wire, which is a dependent variable here.
  compression: bool('COMPRESSION', false),
  tls: bool('TLS', false),

  // --- polling-arm specific ----------------------------------------------
  pollMode: str('POLL_MODE', 'short'),          // short | long
  // Advertised to the client via /config so the load generator polls at the
  // interval the experiment declares, rather than one hard-coded in a script.
  pollIntervalMs: int('POLL_INTERVAL_MS', profile.pollIntervalMatchedMs),
  pollIntervalMatchedMs: profile.pollIntervalMatchedMs,
  longPollTimeoutMs: int('LONG_POLL_TIMEOUT_MS', 25000),
  ringCapacity: int('RING_CAPACITY', 500000),   // bounded replay buffer
  maxBatch: int('MAX_BATCH', 500),              // cap messages per poll response

  // --- housekeeping -------------------------------------------------------
  metricsEnabled: bool('METRICS_ENABLED', true),
  logLevel: str('LOG_LEVEL', 'warn'),
};

config.msgIntervalMs = config.serverRateHz > 0 ? 1000 / config.serverRateHz : 0;
config.pollIntervalMismatched = config.pollIntervalMs !== config.pollIntervalMatchedMs;

function describe() {
  return JSON.stringify(
    {
      ...config,
      nodeVersion: process.version,
      nodeFlags: process.execArgv,
      pid: process.pid,
      startedAt: new Date().toISOString(),
    },
    null,
    2
  );
}

module.exports = { config, describe };
