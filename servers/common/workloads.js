'use strict';

/**
 * The three workload scenarios specified in Section 1.7.
 *
 * Each profile fixes the two dimensions along which real-time workloads vary
 * (directionality and message frequency) and, additionally, payload size --
 * which Section 2.4 requires be treated as a controlled variable rather than
 * fixed at one value, because the relative advantage of the persistent
 * architectures is a ratio between overhead and payload and therefore shrinks
 * as payloads grow (Appelqvist & Ornmyr, 2017).
 *
 * ROOM SIZE, and why it is constant across concurrency tiers
 * ---------------------------------------------------------
 * For the chat workload every message is fanned out to the sender's room.
 * If the room were "all connected clients", delivered messages would grow as
 * O(n^2) with the tier and the 1,000-client tier would be measuring fan-out
 * degree rather than connection count. Holding room size at 20 keeps the
 * per-message fan-out constant, so the concurrency tier varies exactly one
 * thing: the number of connections the server sustains.
 */

const PROFILES = {
  // Low frequency, bursty, BIDIRECTIONAL. The archetypal WebSocket case.
  // Clients originate messages; the server fans each out to the sender's room.
  chat: {
    name: 'chat',
    direction: 'bidirectional',
    fanout: 'room',
    roomSize: 20,
    payloadBytes: 120,          // a typical chat line
    // Server-side publish rate is zero: all traffic originates at the client.
    serverRateHz: 0,
    clientRateHz: 0.2,          // one message per client per 5 s
    burstiness: 3,              // bursts of up to 3 messages, then idle
    pollIntervalMatchedMs: 1000,
  },

  // Moderate frequency, server-to-client only. The archetypal SSE case.
  notification: {
    name: 'notification',
    direction: 'server-to-client',
    fanout: 'broadcast',
    roomSize: null,
    payloadBytes: 256,
    serverRateHz: 1,
    clientRateHz: 0,
    burstiness: 1,
    pollIntervalMatchedMs: 1000,
  },

  // High frequency, server-to-client. Stresses sustained throughput and
  // sharpens the streaming-vs-repeated-polling comparison.
  dashboard: {
    name: 'dashboard',
    direction: 'server-to-client',
    fanout: 'broadcast',
    roomSize: null,
    payloadBytes: 1024,
    serverRateHz: 10,
    clientRateHz: 0,
    burstiness: 1,
    pollIntervalMatchedMs: 100,
  },
};

function getProfile(name) {
  const p = PROFILES[name];
  if (!p) {
    throw new Error(`unknown workload "${name}"; expected one of ${Object.keys(PROFILES).join(', ')}`);
  }
  return p;
}

module.exports = { PROFILES, getProfile };
