import sse from 'k6/x/sse';
import http from 'k6/http';
import {
  cfg, baseUrl, scenarioOptions, nowMs, makeSeqTracker, recordEnvelope,
  nextChatDelayMs,
  payloadBytes, connectFailures, connectDuration, deliveryErrors, activeClients,
  buildClientMessage, uplinkRequests, uplinkDuration,
} from './lib/common.js';

export const options = scenarioOptions();

/**
 * ARM 2 -- Server-Sent Events, via the xk6-sse extension (see loadgen/Dockerfile
 * for why a custom k6 binary is required).
 *
 * The channel is unidirectional, so the chat uplink must travel over a SEPARATE
 * POST /publish. Every such request is counted, because the auxiliary request
 * count is one of the study's findings: it is the cost the mechanism's structure
 * imposes and WebSocket does not pay.
 */
export default function () {
  const t0 = nowMs();
  const tracker = makeSeqTracker();
  let seq = 0;

  const url = `${baseUrl}/events`;
  const params = { headers: { Accept: 'text/event-stream' } };

  const res = sse.open(url, params, (client) => {
    client.on('open', () => {
      connectDuration.add(nowMs() - t0);
      activeClients.add(1);

      // Chat uplink, driven by a TIMER.
      //
      // An earlier revision issued the uplink from inside the 'event' handler,
      // on the reasoning that every chat client is also a recipient of its own
      // room's traffic. That reasoning is circular: in the chat workload there is
      // no server-side publisher, so no event ever arrives, so no client ever
      // publishes, so no event ever arrives. The pilot recorded exactly this --
      // zero messages across all three sse-chat cells, while ws-chat and
      // poll-chat (whose uplinks are timer- and loop-driven) worked normally.
      //
      // setTimeout is a standard global in k6 v1.x and its callbacks run on the
      // VU event loop, which continues to turn while sse.open holds the VU inside
      // the stream callback.
      if (cfg.clientRateHz > 0) {
        const pump = () => {
          seq += 1;
          const u0 = nowMs();
          const r = http.post(`${baseUrl}/publish`, buildClientMessage(seq), {
            headers: { 'Content-Type': 'application/json', 'X-Client-Id': String(__VU) },
          });
          uplinkDuration.add(nowMs() - u0);
          uplinkRequests.add(1);
          if (r.status !== 204) deliveryErrors.add(true);
          setTimeout(pump, nextChatDelayMs());
        };
        setTimeout(pump, nextChatDelayMs());
      }
    });

    client.on('event', (event) => {
      if (!event.data) return;
      payloadBytes.add(event.data.length);
      try {
        recordEnvelope(JSON.parse(event.data), tracker);
      } catch (_) {
        deliveryErrors.add(true);
      }

    });

    client.on('error', () => { connectFailures.add(1); deliveryErrors.add(true); });
  });

  if (!res || res.status >= 400) connectFailures.add(1);
  activeClients.add(-1);
}
