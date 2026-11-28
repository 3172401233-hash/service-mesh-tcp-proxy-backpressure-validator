'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { runSimulation } = require('../src/proxy');

const actionsOf = (result, name) =>
  result.actions.filter((a) => a.action === name);

test('slow downstream pauses and resumes upstream', () => {
  const result = runSimulation(
    { downstreamWindow: 1000, resumeThreshold: 500 },
    [
      { type: 'upstream-data', bytes: 1200 },
      { type: 'upstream-data', bytes: 400 },
      { type: 'downstream-ack', bytes: 700 },
      { type: 'downstream-ack', bytes: 500 },
    ],
  );

  assert.equal(result.bufferPeak, 1200);
  assert.equal(actionsOf(result, 'pause-upstream').length, 1);
  assert.equal(actionsOf(result, 'resume-upstream').length, 1);
  assert.equal(result.deferredBytes, 400);
  assert.equal(result.finalState.bufferedBytes, 0);
  assert.equal(result.finalState.paused, false);

  const pause = actionsOf(result, 'pause-upstream')[0];
  const resume = actionsOf(result, 'resume-upstream')[0];
  assert.equal(pause.bufferedBytes, 1200);
  assert.equal(resume.bufferedBytes, 500);
});

test('connection migration retransmits unacked and discards late duplicates', () => {
  const result = runSimulation(
    { downstreamWindow: 1000 },
    [
      { type: 'upstream-data', bytes: 800 },
      { type: 'downstream-ack', bytes: 300 },
      { type: 'migrate' },
      { type: 'late-data', proxy: 0, bytes: 500 },
      { type: 'late-ack', proxy: 0, bytes: 200 },
      { type: 'downstream-ack', bytes: 500 },
    ],
  );

  const migrate = actionsOf(result, 'migrate')[0];
  assert.equal(migrate.unackedBytes, 500);
  assert.equal(actionsOf(result, 'retransmit')[0].reason, 'migration');
  assert.equal(result.retransmittedBytes, 500);
  assert.equal(result.duplicateBytes, 700);
  assert.equal(actionsOf(result, 'discard-duplicate').length, 2);
  assert.equal(result.finalState.proxyCount, 2);
  assert.equal(result.finalState.bufferedBytes, 0);
});

test('half-close drains buffer before closing downstream', () => {
  const result = runSimulation(
    { downstreamWindow: 1000 },
    [
      { type: 'upstream-data', bytes: 600 },
      { type: 'upstream-fin' },
      { type: 'upstream-data', bytes: 100 },
      { type: 'downstream-ack', bytes: 600 },
    ],
  );

  const halfClose = actionsOf(result, 'half-close')[0];
  assert.equal(halfClose.bufferedBytes, 600);
  assert.equal(actionsOf(result, 'reject-upstream').length, 1);

  const closes = actionsOf(result, 'close-downstream');
  assert.equal(closes.length, 1);
  assert.equal(closes[0].reason, 'upstream-fin-drained');
  assert.ok(
    result.actions.indexOf(closes[0]) >
      result.actions.indexOf(actionsOf(result, 'ack-downstream')[0]),
  );
  assert.equal(result.finalState.downstreamOpen, false);
});

test('ack timeout triggers retransmission of in-flight bytes', () => {
  const result = runSimulation(
    { downstreamWindow: 1000, ackTimeoutMs: 1000 },
    [
      { type: 'upstream-data', bytes: 500 },
      { type: 'tick', ms: 500 },
      { type: 'tick', ms: 500 },
    ],
  );

  const retransmits = actionsOf(result, 'retransmit');
  assert.equal(retransmits.length, 1);
  assert.equal(retransmits[0].reason, 'ack-timeout');
  assert.equal(retransmits[0].bytes, 500);
  assert.equal(result.retransmittedBytes, 500);
});

test('cancel discards buffered bytes and stops the session', () => {
  const result = runSimulation(
    { downstreamWindow: 1000 },
    [
      { type: 'upstream-data', bytes: 500 },
      { type: 'cancel' },
      { type: 'upstream-data', bytes: 100 },
      { type: 'downstream-ack', bytes: 500 },
    ],
  );

  const cancel = actionsOf(result, 'cancel')[0];
  assert.equal(cancel.discardedBytes, 500);
  assert.equal(result.finalState.cancelled, true);
  assert.equal(result.finalState.downstreamOpen, false);
  assert.equal(result.finalState.bufferedBytes, 500);
});
