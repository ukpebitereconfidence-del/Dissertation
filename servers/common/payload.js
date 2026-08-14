'use strict';

/**
 * Message envelope, identical on every arm:
 *
 *   { s: <seq>, t: <publishedAtMs>, p: "<padding>" }
 *
 *   s : monotonic sequence number -> lets the client detect gaps (loss)
 *   t : publication instant as an INTEGER count of microseconds since the
 *       Unix epoch. Integer, and therefore fixed-width at 16 digits, so the
 *       serialised envelope length does not vary with the clock -- necessary
 *       because payload size is a controlled variable and a variable-width
 *       float timestamp would make it drift by several bytes per message.
 *       Load generator and SUT run as containers on one kernel and therefore
 *       read one CLOCK_REALTIME, so the inter-clock offset is zero by
 *       construction and no NTP term is required. The measurement floor is
 *       set by the coarser of the two stamps -- the load generator's, at
 *       1 ms -- not by this one. See Chapter 3.
 *   p : incompressible-ish padding to hit a target serialised size, so
 *       payload size can be varied as an independent factor.
 */

const PAD_ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';

// Timestamps come from the periodically re-anchored clock in ./clock.js, NOT
// from performance.timeOrigin + performance.now(). See that module for why the
// obvious approach silently corrupts cross-process latency measurement.
const { nowMicros, nowMs, nowMonoNs } = require('./clock');

/** Pre-compute padding once; regenerating per message would pollute the
 *  server CPU measurement with string-building work. */
function buildPadCache(targetBytes) {
  // Payload size is a declared controlled variable (Section 2.4), so it must
  // land on target exactly rather than approximately. The serialised length of
  // the float timestamp varies by a few characters between messages, so the
  // pad is sized by measurement against a representative envelope and then
  // corrected, instead of by arithmetic on an assumed overhead.
  const probe = (pad) =>
    Buffer.byteLength(JSON.stringify({ s: 999999999, t: nowMs(), p: pad }), 'utf8');

  const grow = (n) => {
    let s = '';
    for (let i = 0; i < n; i++) s += PAD_ALPHABET[i % PAD_ALPHABET.length];
    return s;
  };

  let padLen = Math.max(0, targetBytes - probe(''));
  // Converge in a handful of passes; each pass corrects by the exact residual.
  for (let pass = 0; pass < 8; pass++) {
    const delta = targetBytes - probe(grow(padLen));
    if (delta === 0) break;
    padLen = Math.max(0, padLen + delta);
  }
  return grow(padLen);
}

class PayloadFactory {
  constructor(targetBytes) {
    this.targetBytes = targetBytes;
    this.pad = buildPadCache(targetBytes);
    this.seq = 0;
  }

  /** Produce the next message object. Timestamp is stamped here, at the
   *  logical moment of publication, before any transport touches it. */
  next() {
    this.seq += 1;
    // Two stamps, taken as close together as possible. See common/clock.js:
    //   m -- CLOCK_MONOTONIC ns, authoritative, read by the probe
    //   t -- CLOCK_REALTIME us, cross-check, read by k6 (Date.now() only)
    return { s: this.seq, m: nowMonoNs().toString(), t: nowMicros(), p: this.pad };
  }

  /**
   * Produce the next message together with its serialised form, sized to the
   * target byte count EXACTLY.
   *
   * The residual correction is necessary because the sequence number's digit
   * count grows during a run, which would otherwise make the envelope creep by
   * a few bytes over a long sweep. Payload size is a controlled variable, so it
   * cannot be allowed to co-vary with elapsed run time. The cost is one extra
   * length measurement per publication tick -- not per delivered message -- so
   * it does not enter the per-client send path that the CPU measurement
   * attributes to the transport.
   */
  nextWire() {
    const msg = this.next();
    let wire = JSON.stringify(msg);
    let len = Buffer.byteLength(wire, 'utf8');
    const deficit = this.targetBytes - len;

    if (deficit !== 0) {
      const padLen = Math.max(0, msg.p.length + deficit);
      msg.p = padLen <= this.pad.length ? this.pad.slice(0, padLen) : this.pad.padEnd(padLen, PAD_ALPHABET);
      wire = JSON.stringify(msg);
      len = Buffer.byteLength(wire, 'utf8');
    }

    if (len !== this.targetBytes) this.sizeMisses = (this.sizeMisses || 0) + 1;
    this.lastBytes = len;
    return { msg, wire, bytes: len };
  }

  /** Serialise. Kept separate so each transport can decide whether to
   *  serialise once and share the string (broadcast) or per client. */
  serialise(msg) {
    return JSON.stringify(msg);
  }

  /** Reported via /stats and archived with every run, so it must reflect the
   *  exact-size build path rather than re-deriving an approximation. */
  actualSize() {
    return this.lastBytes || this.targetBytes;
  }
}

module.exports = { PayloadFactory, nowMs, nowMicros };
