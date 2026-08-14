'use strict';

/**
 * Minimal Prometheus text-format exposition, hand-rolled on purpose.
 *
 * Rationale: prom-client is excellent, but it registers default collectors
 * that walk the event loop and GC on a timer. That is measurable overhead
 * inside the very process whose CPU cost is a dependent variable. Emitting
 * the text format directly keeps the observer effect small and, more
 * importantly, *identical* across the three arms.
 */

const os = require('node:os');
const { clockReport } = require('./clock');

class Telemetry {
  constructor({ transport, runId }) {
    this.transport = transport;
    this.runId = runId;

    this.counters = {
      connections_opened: 0,
      connections_closed: 0,
      connection_errors: 0,
      messages_sent: 0,        // application messages delivered to clients
      bytes_sent: 0,           // application payload bytes (excl. framing)
      http_requests: 0,        // polling arm: request count
      http_empty_responses: 0, // polling arm: polls that returned nothing
      long_poll_timeouts: 0,
    };

    this.gauges = {
      connections_active: 0,
      long_poll_waiting: 0,
    };

    this._lastCpu = process.cpuUsage();
    this._lastCpuAt = Date.now();
  }

  inc(name, by = 1) {
    if (this.counters[name] === undefined) this.counters[name] = 0;
    this.counters[name] += by;
  }

  set(name, value) {
    this.gauges[name] = value;
  }

  add(name, by) {
    if (this.gauges[name] === undefined) this.gauges[name] = 0;
    this.gauges[name] += by;
  }

  /** In-process CPU accounting. cAdvisor gives the authoritative
   *  container-level figure; this is a cross-check and catches the case
   *  where cAdvisor sampling is too coarse for short runs. */
  cpuSnapshot() {
    const now = Date.now();
    const delta = process.cpuUsage(this._lastCpu);
    const wallMs = now - this._lastCpuAt;
    this._lastCpu = process.cpuUsage();
    this._lastCpuAt = now;
    const cpuMs = (delta.user + delta.system) / 1000;
    return {
      cpuPercent: wallMs > 0 ? (cpuMs / wallMs) * 100 : 0,
      userMs: delta.user / 1000,
      systemMs: delta.system / 1000,
    };
  }

  memSnapshot() {
    const m = process.memoryUsage();
    return { rss: m.rss, heapUsed: m.heapUsed, heapTotal: m.heapTotal, external: m.external };
  }

  /** Prometheus text exposition. */
  render(extra = {}) {
    const labels = `transport="${this.transport}",run_id="${this.runId}"`;
    const lines = [];

    const emit = (name, type, value, help) => {
      lines.push(`# HELP rtb_${name} ${help}`);
      lines.push(`# TYPE rtb_${name} ${type}`);
      lines.push(`rtb_${name}{${labels}} ${value}`);
    };

    for (const [k, v] of Object.entries(this.counters)) {
      emit(k, 'counter', v, `rtb ${k}`);
    }
    for (const [k, v] of Object.entries(this.gauges)) {
      emit(k, 'gauge', v, `rtb ${k}`);
    }

    const mem = this.memSnapshot();
    emit('process_rss_bytes', 'gauge', mem.rss, 'resident set size');
    emit('process_heap_used_bytes', 'gauge', mem.heapUsed, 'v8 heap in use');

    const cpu = process.cpuUsage();
    emit('process_cpu_seconds_total', 'counter', (cpu.user + cpu.system) / 1e6, 'total cpu seconds');

    emit('load1', 'gauge', os.loadavg()[0], 'host 1-minute load average');

    for (const [k, v] of Object.entries(extra)) {
      if (typeof v === 'number') emit(k, 'gauge', v, `rtb ${k}`);
    }

    return lines.join('\n') + '\n';
  }

  json(extra = {}) {
    return {
      transport: this.transport,
      runId: this.runId,
      counters: { ...this.counters },
      gauges: { ...this.gauges },
      memory: this.memSnapshot(),
      cpu: this.cpuSnapshot(),
      clock: clockReport(),
      ...extra,
    };
  }

  reset() {
    for (const k of Object.keys(this.counters)) this.counters[k] = 0;
    this.counters.http_requests = 0;
  }
}

module.exports = { Telemetry };
