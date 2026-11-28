'use strict';

const DEFAULTS = {
  upstreamWindow: 65536,
  downstreamWindow: 16384,
  resumeThreshold: null,
  ackTimeoutMs: 1000,
};

class ProxySession {
  constructor(id, downstreamWindow) {
    this.id = id;
    this.downstreamWindow = downstreamWindow;
    this.nextSeq = 0;
    this.writtenSeq = 0;
    this.ackedSeq = 0;
    this.stale = false;
  }

  get bufferedBytes() {
    return this.nextSeq - this.ackedSeq;
  }

  get inFlightBytes() {
    return this.writtenSeq - this.ackedSeq;
  }

  get unwrittenBytes() {
    return this.nextSeq - this.writtenSeq;
  }
}

function resolveConfig(config) {
  const merged = { ...DEFAULTS, ...config };
  if (merged.resumeThreshold === null || merged.resumeThreshold === undefined) {
    merged.resumeThreshold = Math.floor(merged.downstreamWindow / 2);
  }
  return merged;
}

function runSimulation(config, events) {
  const cfg = resolveConfig(config);
  const state = {
    time: 0,
    paused: false,
    halfClosed: false,
    downstreamOpen: true,
    cancelled: false,
    bufferPeak: 0,
    duplicateBytes: 0,
    retransmittedBytes: 0,
    deferredBytes: 0,
    actions: [],
    proxies: [new ProxySession(0, cfg.downstreamWindow)],
    activeProxy: 0,
    lastAckTime: 0,
  };

  const active = () => state.proxies[state.activeProxy];
  const log = (action, detail) => {
    state.actions.push({ time: state.time, action, ...detail });
  };

  const updatePeak = () => {
    const buffered = active().bufferedBytes;
    if (buffered > state.bufferPeak) state.bufferPeak = buffered;
  };

  const applyBackpressure = () => {
    const proxy = active();
    if (!state.paused && proxy.bufferedBytes >= cfg.downstreamWindow) {
      state.paused = true;
      log('pause-upstream', { bufferedBytes: proxy.bufferedBytes });
    } else if (state.paused && proxy.bufferedBytes <= cfg.resumeThreshold) {
      state.paused = false;
      log('resume-upstream', { bufferedBytes: proxy.bufferedBytes });
    }
  };

  const flushDownstream = () => {
    const proxy = active();
    if (!state.downstreamOpen) return;
    const writable = Math.min(
      proxy.downstreamWindow - proxy.inFlightBytes,
      proxy.unwrittenBytes,
    );
    if (writable > 0) {
      proxy.writtenSeq += writable;
      log('write-downstream', { bytes: writable, inFlightBytes: proxy.inFlightBytes });
    }
  };

  const maybeCloseDownstream = () => {
    if (
      state.halfClosed &&
      state.downstreamOpen &&
      active().bufferedBytes === 0
    ) {
      state.downstreamOpen = false;
      log('close-downstream', { reason: 'upstream-fin-drained' });
    }
  };

  const acceptUpstream = (bytes) => {
    if (state.halfClosed || state.cancelled || !state.downstreamOpen) {
      log('reject-upstream', { bytes, reason: 'closed' });
      return;
    }
    if (state.paused) {
      state.deferredBytes += bytes;
      log('defer-upstream', { bytes, reason: 'paused' });
      return;
    }
    const proxy = active();
    const accepted = Math.min(bytes, cfg.upstreamWindow);
    proxy.nextSeq += accepted;
    if (accepted < bytes) {
      state.deferredBytes += bytes - accepted;
      log('defer-upstream', { bytes: bytes - accepted, reason: 'upstream-window-full' });
    }
    updatePeak();
    flushDownstream();
    applyBackpressure();
  };

  const applyAck = (bytes, proxy) => {
    const target = proxy || active();
    const ackTo = Math.min(target.ackedSeq + bytes, target.writtenSeq);
    const applied = ackTo - target.ackedSeq;
    target.ackedSeq = ackTo;
    if (applied > 0) {
      state.lastAckTime = state.time;
      log('ack-downstream', { bytes: applied, proxyId: target.id });
    }
    if (!proxy) {
      flushDownstream();
      applyBackpressure();
      maybeCloseDownstream();
    }
  };

  const retransmit = (reason) => {
    const proxy = active();
    const unacked = proxy.inFlightBytes;
    if (unacked <= 0) return 0;
    state.retransmittedBytes += unacked;
    log('retransmit', { bytes: unacked, reason });
    state.lastAckTime = state.time;
    return unacked;
  };

  const migrate = () => {
    const old = active();
    old.stale = true;
    const next = new ProxySession(state.proxies.length, cfg.downstreamWindow);
    next.nextSeq = old.nextSeq;
    next.ackedSeq = old.ackedSeq;
    next.writtenSeq = old.ackedSeq;
    state.proxies.push(next);
    state.activeProxy = next.id;
    const unacked = next.nextSeq - next.ackedSeq;
    log('migrate', { fromProxy: old.id, toProxy: next.id, unackedBytes: unacked });
    if (unacked > 0) {
      next.writtenSeq = next.nextSeq;
      state.retransmittedBytes += unacked;
      log('retransmit', { bytes: unacked, reason: 'migration' });
    }
    state.lastAckTime = state.time;
    applyBackpressure();
  };

  const lateFromOldProxy = (kind, proxyId, bytes) => {
    const proxy = state.proxies[proxyId];
    if (!proxy || !proxy.stale) {
      log('ignore-late', { kind, proxyId, reason: 'not-stale' });
      return;
    }
    state.duplicateBytes += bytes;
    log('discard-duplicate', { kind, proxyId, bytes });
  };

  const tick = (ms) => {
    state.time += ms;
    if (state.cancelled || !state.downstreamOpen) return;
    const proxy = active();
    if (
      proxy.inFlightBytes > 0 &&
      state.time - state.lastAckTime >= cfg.ackTimeoutMs
    ) {
      retransmit('ack-timeout');
    }
  };

  const cancel = () => {
    if (state.cancelled) return;
    state.cancelled = true;
    const discarded = active().bufferedBytes;
    state.downstreamOpen = false;
    log('cancel', { discardedBytes: discarded });
  };

  for (const event of events) {
    if (state.cancelled && event.type !== 'tick') continue;
    switch (event.type) {
      case 'upstream-data':
        acceptUpstream(event.bytes);
        break;
      case 'downstream-ack':
        applyAck(event.bytes);
        break;
      case 'upstream-fin':
        if (!state.halfClosed) {
          state.halfClosed = true;
          log('half-close', { bufferedBytes: active().bufferedBytes });
          maybeCloseDownstream();
        }
        break;
      case 'migrate':
        migrate();
        break;
      case 'late-ack':
        lateFromOldProxy('ack', event.proxy, event.bytes);
        break;
      case 'late-data':
        lateFromOldProxy('data', event.proxy, event.bytes);
        break;
      case 'tick':
        tick(event.ms);
        break;
      case 'cancel':
        cancel();
        break;
      default:
        throw new Error(`unknown event type: ${event.type}`);
    }
  }

  return {
    bufferPeak: state.bufferPeak,
    duplicateBytes: state.duplicateBytes,
    retransmittedBytes: state.retransmittedBytes,
    deferredBytes: state.deferredBytes,
    actions: state.actions,
    finalState: {
      paused: state.paused,
      halfClosed: state.halfClosed,
      downstreamOpen: state.downstreamOpen,
      cancelled: state.cancelled,
      bufferedBytes: active().bufferedBytes,
      proxyCount: state.proxies.length,
    },
  };
}

module.exports = { runSimulation, ProxySession, DEFAULTS };
