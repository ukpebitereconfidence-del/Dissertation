'use strict';

/**
 * Subscriber registry and fan-out, shared by all four arms.
 *
 * Each transport registers subscribers with a `send(wireString)` closure and
 * the hub owns everything else: room assignment, fan-out, and the ingress path
 * for client-originated messages. Keeping this common means the only code that
 * differs between arms is the code that puts bytes on a socket -- which is the
 * independent variable.
 *
 * THE CHAT INGRESS PATH, and why it is measurable here
 * ----------------------------------------------------
 * For the chat workload the client originates the message and stamps it with
 * its own send instant. Every recipient computes latency as
 * (receive instant - sender's stamp), i.e. a true sender-to-receiver one-way
 * latency across two different clients.
 *
 * That measurement is normally intractable: Mackovic (2025) abandoned exactly
 * this experiment because no clock-synchronisation strategy available was
 * precise enough. It is tractable here because sender, receiver and server are
 * containers on one kernel and therefore read one CLOCK_REALTIME, so the
 * inter-clock offset is zero by construction rather than by synchronisation.
 * The cost is ecological validity, which Section 1.7 accepts explicitly.
 *
 * The uplink differs by architecture and that difference is the point:
 *   ws          -> same socket, no extra request
 *   sse         -> separate POST /publish (the channel is unidirectional)
 *   poll-short  -> separate POST /publish
 *   poll-long   -> separate POST /publish
 */

class Hub {
  constructor({ config, telemetry, logger = console }) {
    this.config = config;
    this.telemetry = telemetry;
    this.logger = logger;

    this.subscribers = new Map();   // id -> { id, send, room, close }
    this.rooms = new Map();         // roomId -> Set<id>
    this.nextId = 1;
    this.nextRoom = 0;
    this.rotation = 0;   // rotating fan-out start; see broadcast()
  }

  /** @param send  (wire: string) => number|void   returns bytes written */
  add({ send, close }) {
    const id = this.nextId++;
    const room = this._assignRoom(id);
    const sub = { id, send, close, room, delivered: 0 };
    this.subscribers.set(id, sub);

    if (!this.rooms.has(room)) this.rooms.set(room, new Set());
    this.rooms.get(room).add(id);

    this.telemetry.inc('connections_opened');
    this.telemetry.set('connections_active', this.subscribers.size);
    return sub;
  }

  remove(id) {
    const sub = this.subscribers.get(id);
    if (!sub) return;
    this.subscribers.delete(id);
    const room = this.rooms.get(sub.room);
    if (room) {
      room.delete(id);
      if (room.size === 0) this.rooms.delete(sub.room);
    }
    this.telemetry.inc('connections_closed');
    this.telemetry.set('connections_active', this.subscribers.size);
  }

  _assignRoom(id) {
    if (this.config.fanout !== 'room') return 'global';
    // Deterministic round-robin into fixed-size rooms. Deterministic because a
    // random assignment would vary fan-out degree between replications of the
    // same cell, adding variance the design is trying to control.
    return `r${Math.floor((id - 1) / this.config.roomSize)}`;
  }

  /**
   * Server-originated broadcast (notification and dashboard workloads).
   *
   * ROTATING FAN-OUT ORDER, and why it is necessary
   * -----------------------------------------------
   * Delivery to n subscribers is serial: the client served last waits for the
   * n-1 sends before it. If the iteration order were fixed, position in that
   * order would be a systematic, per-client latency offset -- the first
   * subscriber would always be fastest and the last always slowest, by an amount
   * that grows with the tier. At 1,000 subscribers on one core that offset is
   * tens of milliseconds.
   *
   * That would corrupt this study specifically, because the measurement probes
   * are a small subset of clients that all connect at about the same moment.
   * They would therefore occupy a contiguous band of positions and sample one
   * end of the distribution rather than the middle of it. Validation showed the
   * effect directly: the probe reported a median of 5.0 ms against the load
   * generator's 1.34 ms in the same run, purely from where each had registered.
   *
   * Rotating the starting position by one on every publication makes each
   * client's position uniform over the run, so expected latency is equal for all
   * subscribers and any subset is an unbiased sample. It does not reduce the
   * serialisation cost -- that cost is real and is part of what the study
   * measures -- it distributes it evenly instead of concentrating it on
   * whichever clients happen to be instrumented.
   */
  broadcast(wire) {
    const bytes = Buffer.byteLength(wire, 'utf8');
    let delivered = 0;

    const subs = Array.from(this.subscribers.values());
    const n = subs.length;
    if (n === 0) return 0;

    const start = this.rotation % n;
    this.rotation = (this.rotation + 1) % Math.max(1, n);

    for (let i = 0; i < n; i++) {
      const sub = subs[(start + i) % n];
      try {
        sub.send(wire);
        sub.delivered += 1;
        delivered += 1;
      } catch (err) {
        this.telemetry.inc('send_errors');
      }
    }

    this.telemetry.inc('messages_sent', delivered);
    this.telemetry.inc('bytes_sent', delivered * bytes);
    return delivered;
  }

  /**
   * Client-originated message (chat workload). The envelope already carries
   * the SENDER's timestamp; the server must not restamp it, or the measurement
   * would silently become server-to-client instead of sender-to-receiver.
   */
  publishFromClient({ senderId, wire }) {
    const sub = this.subscribers.get(senderId);
    const roomId = sub ? sub.room : 'global';
    const members = this.rooms.get(roomId);
    if (!members) return 0;

    const bytes = Buffer.byteLength(wire, 'utf8');
    let delivered = 0;

    for (const memberId of members) {
      const member = this.subscribers.get(memberId);
      if (!member) continue;
      try {
        member.send(wire);
        member.delivered += 1;
        delivered += 1;
      } catch (err) {
        this.telemetry.inc('send_errors');
      }
    }

    this.telemetry.inc('messages_sent', delivered);
    this.telemetry.inc('bytes_sent', delivered * bytes);
    this.telemetry.inc('ingress_messages');
    return delivered;
  }

  size() {
    return this.subscribers.size;
  }

  closeAll() {
    for (const sub of this.subscribers.values()) {
      try {
        if (sub.close) sub.close();
      } catch (_) { /* teardown, ignore */ }
    }
    this.subscribers.clear();
    this.rooms.clear();
    this.telemetry.set('connections_active', 0);
  }

  snapshot() {
    return {
      subscribers: this.subscribers.size,
      rooms: this.rooms.size,
      roomSizeConfigured: this.config.roomSize,
    };
  }
}

module.exports = { Hub };
