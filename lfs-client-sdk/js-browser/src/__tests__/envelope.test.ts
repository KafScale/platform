import { describe, expect, it } from 'vitest';
import { decodeLfsEnvelope, isLfsEnvelope } from '../envelope.js';

const sampleEnvelope = {
  kfs_lfs: 1,
  bucket: 'demo-bucket',
  key: 'default/topic/0/segment.bin',
  size: 42,
  sha256: 'abc123',
};

describe('isLfsEnvelope', () => {
  it('accepts a valid envelope object', () => {
    expect(isLfsEnvelope(sampleEnvelope)).toBe(true);
  });

  it('rejects non-objects and incomplete payloads', () => {
    expect(isLfsEnvelope(null)).toBe(false);
    expect(isLfsEnvelope('{"kfs_lfs":1}')).toBe(false);
    expect(isLfsEnvelope({ kfs_lfs: 1, bucket: 'b', key: 'k' })).toBe(false);
    expect(isLfsEnvelope({ ...sampleEnvelope, sha256: 123 })).toBe(false);
  });
});

describe('decodeLfsEnvelope', () => {
  it('decodes JSON strings and byte payloads', () => {
    const json = JSON.stringify(sampleEnvelope);

    expect(decodeLfsEnvelope(json)).toEqual(sampleEnvelope);
    expect(decodeLfsEnvelope(new TextEncoder().encode(json))).toEqual(sampleEnvelope);
  });

  it('throws when required fields are missing', () => {
    expect(() => decodeLfsEnvelope('{"kfs_lfs":1}')).toThrow(
      'Invalid LFS envelope: missing required fields'
    );
  });
});