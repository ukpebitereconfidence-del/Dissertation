import { WebSocket } from 'k6/experimental/websockets';
// NOTE: no timers import. In k6 v1.x, k6/experimental/timers has graduated and
// setTimeout is a standard global; importing it now raises a script exception.
// k6/experimental/websockets, by contrast, is still a real module in v1.2.1
// (verified against internal/js/jsmodules.go), so that import remains correct.
import {
  cfg, wsUrl, scenarioOptions, nowMs, makeSeqTracker, recordEnvelope,
  payloadBytes, connectFailures, connectDuration, deliveryErrors, activeClients,
  buildClientMessage, nextChatDelayMs, uplinkRequests,
} from './lib/common.js';

export const options = scenarioOptions();

/**
 * ARM 1 -- WebSocket.
 *
 * One persistent connection per VU, held for the whole run. The chat uplink
 * travels over the SAME socket: no auxiliary request, which is the structural
 * advantage this arm is expected to show.
 */
export default function () {
  const t0 = nowMs();
  const ws = new WebSocket(wsUrl);
  const tracker = makeSeqTracker();
  let seq = 0;

  ws.onopen = () => {
    connectDuration.add(nowMs() - t0);
    activeClients.add(1);

    if (cfg.clientRateHz > 0) {
      const pump = () => {
        seq += 1;
        ws.send(buildClientMessage(seq));
        uplinkRequests.add(1);
        setTimeout(pump, nextChatDelayMs());
      };
      setTimeout(pump, nextChatDelayMs());
    }

    // Close at the end of the measurement window. gracefulStop is 0, so the VU
    // must terminate its own connection rather than be cut mid-frame.
    setTimeout(() => ws.close(), (cfg.warmupS + cfg.durationS) * 1000 - (nowMs() - t0) - 500);
  };

  ws.onmessage = (e) => {
    payloadBytes.add(e.data.length);
    try {
      recordEnvelope(JSON.parse(e.data), tracker);
    } catch (_) {
      deliveryErrors.add(true);
    }
  };

  ws.onerror = () => { connectFailures.add(1); deliveryErrors.add(true); };
  ws.onclose = () => activeClients.add(-1);
}
