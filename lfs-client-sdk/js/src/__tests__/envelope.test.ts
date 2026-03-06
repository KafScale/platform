import test from 'node:test';
import assert from 'node:assert/strict';
import { isLfsEnvelope } from '../envelope.js';

test('isLfsEnvelope detects marker', () => {
  assert.equal(isLfsEnvelope(new TextEncoder().encode('{"kfs_lfs":1}')), true);
  assert.equal(isLfsEnvelope(new TextEncoder().encode('plain')), false);
});
