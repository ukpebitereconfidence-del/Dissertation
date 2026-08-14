'use strict';

const { PayloadFactory } = require('./payload');

/**
 * The workload generator that sits BEHIND all three transports.
 *
 * Why this exists as shared code: if each server produced its own messages,
 * a busy event loop would slow message production, and the observed
 * publication rate would become a function of the transport. That is a
 * confound -- the transport would be changing the workload it is being
 * measured against. Here the schedule is drift-corrected against absolute
 * wall-clock deadlines, so a saturated arm reports *missed ticks* (an
 * honest, recorded observation) instead of silently publishing slower.
 */
class Broadcaster {
  constructor({ intervalMs, payloadBytes, onMessage, logger = console }) {
    this.intervalMs = intervalMs;
    this.factory = new PayloadFactory(payloadBytes);
    this.onMessage = onMessage;
    this.logger = logger;

    this.timer = null;
    this.startedAt = null;
    this.tick = 0;

    this.stats = {
      published: 0,
      missedTicks: 0,      // deadlines skipped because the loop was blocked
      maxTickLagMs: 0,     // worst observed scheduling lag
      cumulativeLagMs: 0,
      lastBytes: 0,
    };
  }

  start() {
    if (this.timer) return;
    this.startedAt = Date.now();
    this.tick = 0;
    this._schedule();
  }

  _schedule() {
    // Absolute deadline for the next tick, derived from the start instant.
    // This is what prevents cumulative drift.
    const nextDeadline = this.startedAt + (this.tick + 1) * this.intervalMs;
    const delay = nextDeadline - Date.now();

    if (delay <= 0) {
      // We are behind. Skip the tick(s) we missed rather than firing a burst,
      // and record it: a burst would corrupt the arrival-rate assumption.
      const behind = Math.floor(-delay / this.intervalMs);
      if (behind > 0) {
        this.tick += behind;
        this.stats.missedTicks += behind;
      }
      this.timer = setImmediate(() => this._fire(nextDeadline));
      return;
    }

    this.timer = setTimeout(() => this._fire(nextDeadline), delay);
    if (this.timer.unref) this.timer.unref();
  }

  _fire(deadline) {
    this.tick += 1;

    const lag = Date.now() - deadline;
    if (lag > this.stats.maxTickLagMs) this.stats.maxTickLagMs = lag;
    this.stats.cumulativeLagMs += Math.max(0, lag);

    // Serialise exactly once per tick, sized to target exactly. Every
    // subscriber receives the same bytes, so per-client serialisation cost
    // cannot differ between arms.
    const { msg, wire, bytes } = this.factory.nextWire();

    this.stats.published += 1;
    this.stats.lastBytes = bytes;

    try {
      this.onMessage(msg, wire);
    } catch (err) {
      this.logger.error('broadcast handler threw', err);
    }

    this._schedule();
  }

  stop() {
    if (!this.timer) return;
    clearTimeout(this.timer);
    clearImmediate(this.timer);
    this.timer = null;
  }

  reset() {
    this.stop();
    this.factory.seq = 0;
    this.stats = { published: 0, missedTicks: 0, maxTickLagMs: 0, cumulativeLagMs: 0, lastBytes: 0 };
    this.start();
  }

  snapshot() {
    return {
      ...this.stats,
      seq: this.factory.seq,
      payloadBytesActual: this.factory.actualSize(),
      uptimeMs: this.startedAt ? Date.now() - this.startedAt : 0,
    };
  }
}

module.exports = { Broadcaster };
