import http from 'k6/http';
import { sleep } from 'k6';
import {
  cfg, baseUrl, scenarioOptions, nowMs, makeSeqTracker, recordEnvelope,
  payloadBytes, connectFailures, deliveryErrors, activeClients,
  pollRequests, emptyPolls, pollBatchSize,
  buildClientMessage, uplinkRequests, uplinkDuration,
} from './lib/common.js';

export const options = scenarioOptions();

/**
 * ARMS 3 and 4 -- HTTP short and long polling. Selected by ARM, so both are
 * driven by identical client code and differ only in whether the client waits
 * out an interval between requests.
 */
const isLong = cfg.arm === 'poll-long';

export function setup() {
  return { deadline: nowMs() + (cfg.warmupS + cfg.durationS) * 1000 };
}

export default function (data) {
  // Register once per VU per run: one request per client for the whole run, so
  // it does not distort the steady-state per-message cost.
  const reg = http.post(`${baseUrl}/register`, '{}', {
    headers: { 'Content-Type': 'application/json' },
  });
  if (reg.status !== 200) { connectFailures.add(1); return; }

  const { clientId, room } = reg.json();
  let cursor = reg.json('cursor');
  const tracker = makeSeqTracker();
  const headers = { 'X-Client-Id': String(clientId) };
  activeClients.add(1);

  let seq = 0;

  // RANDOMISED INITIAL POLL PHASE -- required, not cosmetic.
  //
  // A short-polling client's latency is determined by where its poll falls
  // relative to the publication tick. If every client begins polling at the same
  // instant they all share one phase, and the run reports a near-constant
  // latency fixed by the arbitrary moment the run started rather than by the
  // mechanism. Validation showed exactly this: the probe reported a median of
  // 268 ms and k6 567 ms against the same server in the same run, each with a
  // spread of only a few milliseconds, purely because the two instruments began
  // polling at different moments.
  //
  // Staggering each client's first poll uniformly across one interval makes the
  // phase distribution uniform, which is both the realistic case -- clients
  // arrive independently -- and the one the mechanism-level analysis assumes.
  // The resulting latency is then uniform over [0, interval] plus delivery time,
  // with a median near interval/2, which is the honest characterisation of
  // short polling and the quantity Bozdag et al. (2007) reason about.
  //
  // Long polling needs no stagger: the server holds the request, so there is no
  // phase to align. It is applied only to the fixed-interval arm.
  if (!isLong) {
    sleep((Math.random() * cfg.pollIntervalMs) / 1000);
  }

  while (nowMs() < data.deadline) {
    const t0 = nowMs();
    const res = http.get(`${baseUrl}/messages?since=${cursor}`, { headers });
    pollRequests.add(1);

    if (res.status !== 200) {
      deliveryErrors.add(true);
      sleep(0.05);
      continue;
    }

    payloadBytes.add(res.body.length);
    const body = res.json();
    cursor = body.cursor;

    const msgs = body.messages || [];
    pollBatchSize.add(msgs.length);
    if (msgs.length === 0) emptyPolls.add(1);
    for (const env of msgs) recordEnvelope(env, tracker);

    // Chat uplink: a separate POST, as for SSE.
    if (cfg.clientRateHz > 0 && Math.random() < cfg.clientRateHz * (cfg.pollIntervalMs / 1000)) {
      seq += 1;
      const u0 = nowMs();
      const r = http.post(`${baseUrl}/publish`, buildClientMessage(seq), {
        headers: { 'Content-Type': 'application/json', ...headers },
      });
      uplinkDuration.add(nowMs() - u0);
      uplinkRequests.add(1);
      if (r.status !== 204) deliveryErrors.add(true);
    }

    if (!isLong) {
      // Sleep the REMAINDER of the interval. Sleeping a fixed interval after a
      // request that itself took time would make the effective poll rate depend
      // on server response time -- so a slower server would be polled less
      // often, flattering it. This is the single most important fairness detail
      // in the polling arm.
      const remaining = cfg.pollIntervalMs - (nowMs() - t0);
      if (remaining > 0) sleep(remaining / 1000);
    }
    // Long polling reissues immediately; the server holds the request open.
  }

  activeClients.add(-1);
}
