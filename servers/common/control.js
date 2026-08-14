'use strict';

const { config, describe } = require('./config');

/**
 * Identical control plane on all three arms.
 *
 * /health  -> readiness probe used by compose and by the orchestrator's
 *             barrier before a run starts
 * /config  -> echoes the resolved configuration; the orchestrator archives
 *             this with each run so the cell is self-documenting
 * /metrics -> Prometheus scrape target
 * /stats   -> JSON snapshot, captured at end-of-run into the results file
 * /reset   -> zeroes counters and restarts the broadcaster sequence so each
 *             replication begins from a known state
 */
function makeControlPlane({ telemetry, broadcaster, onReset }) {
  return function handle(req, res) {
    const url = new URL(req.url, 'http://localhost');

    switch (url.pathname) {
      case '/health':
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true, transport: config.transport }));
        return true;

      case '/config':
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(describe());
        return true;

      case '/metrics': {
        const extra = broadcaster
          ? {
              broadcast_published_total: broadcaster.stats.published,
              broadcast_missed_ticks_total: broadcaster.stats.missedTicks,
              broadcast_max_tick_lag_ms: broadcaster.stats.maxTickLagMs,
            }
          : {};
        res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' });
        res.end(telemetry.render(extra));
        return true;
      }

      case '/stats':
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(
          JSON.stringify(
            telemetry.json({ broadcaster: broadcaster ? broadcaster.snapshot() : null }),
            null,
            2
          )
        );
        return true;

      case '/reset':
        telemetry.reset();
        if (broadcaster) broadcaster.reset();
        if (onReset) onReset();
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true, reset: true }));
        return true;

      default:
        return false; // not a control route; let the transport handle it
    }
  };
}

module.exports = { makeControlPlane };
