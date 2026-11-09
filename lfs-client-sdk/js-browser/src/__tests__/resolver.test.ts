import { afterEach, describe, expect, it, vi } from 'vitest';
import { LfsResolver } from '../resolver.js';

const helloBytes = new TextEncoder().encode('hello');
const helloSha256 = '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824';

const sampleEnvelope = {
  kfs_lfs: 1,
  bucket: 'demo-bucket',
  key: 'default/topic/0/segment.bin',
  size: helloBytes.length,
  sha256: helloSha256,
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe('LfsResolver', () => {
  it('returns plain payloads unchanged when input is not an envelope', async () => {
    const resolver = new LfsResolver({
      getBlobUrl: () => 'https://example.invalid/blob',
    });

    const result = await resolver.resolve('plain-text');

    expect(result.isEnvelope).toBe(false);
    expect(new TextDecoder().decode(result.payload)).toBe('plain-text');
    expect(result.envelope).toBeUndefined();
  });

  it('fetches and validates envelope blobs', async () => {
    const getBlobUrl = vi.fn().mockResolvedValue('https://example.invalid/blob');
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      arrayBuffer: async () => helloBytes.buffer,
    });
    vi.stubGlobal('fetch', fetchMock);

    const resolver = new LfsResolver({ getBlobUrl });
    const result = await resolver.resolve(sampleEnvelope);

    expect(getBlobUrl).toHaveBeenCalledWith(sampleEnvelope.key, sampleEnvelope.bucket);
    expect(fetchMock).toHaveBeenCalledWith('https://example.invalid/blob');
    expect(result.isEnvelope).toBe(true);
    expect(result.envelope).toEqual(sampleEnvelope);
    expect(result.payload).toEqual(helloBytes);
  });

  it('rejects blobs that exceed maxSize', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        arrayBuffer: async () => helloBytes.buffer,
      })
    );

    const resolver = new LfsResolver({
      getBlobUrl: async () => 'https://example.invalid/blob',
      maxSize: 2,
      validateChecksum: false,
    });

    await expect(resolver.resolve(sampleEnvelope)).rejects.toThrow(
      'Payload exceeds max size: 5 > 2'
    );
  });

  it('rejects checksum mismatches', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        arrayBuffer: async () => helloBytes.buffer,
      })
    );

    const resolver = new LfsResolver({
      getBlobUrl: async () => 'https://example.invalid/blob',
    });

    await expect(
      resolver.resolve({ ...sampleEnvelope, sha256: 'deadbeef' })
    ).rejects.toThrow('Checksum mismatch');
  });

  describe.each([
    ['sha256', helloSha256],
    ['md5', '5d41402abc4b2a76b9719d911017c592'],
    ['crc32', '3610a686'],
    ['none', ''],
  ])('checksum_alg %s', (alg, checksum) => {
    const envelope = checksum ? { ...sampleEnvelope, checksum, checksum_alg: alg } : { ...sampleEnvelope, checksum_alg: alg };
    const resolverFor = (payload: Uint8Array) => {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, arrayBuffer: async () => payload.slice().buffer }));
      return new LfsResolver({ getBlobUrl: async () => 'https://example.invalid/blob' });
    };

    it('resolves valid blobs', async () => {
      const result = await resolverFor(helloBytes).resolve(envelope);
      expect(result.payload).toEqual(helloBytes);
    });

    it('rejects corrupted blobs', async () => {
      const corrupt = helloBytes.slice();
      corrupt[0] ^= 1;
      await expect(resolverFor(corrupt).resolve(envelope)).rejects.toThrow('Checksum mismatch');
    });
  });

  it.each([
    ['object', { ...sampleEnvelope, sha256: '' }],
    ['string', JSON.stringify({ ...sampleEnvelope, sha256: '' })],
    ['bytes', new TextEncoder().encode(JSON.stringify({ ...sampleEnvelope, sha256: '' }))],
  ])('rejects %s envelopes with an empty sha256 before fetching', async (_kind, value) => {
    const getBlobUrl = vi.fn();
    const resolver = new LfsResolver({ getBlobUrl });
    await expect(resolver.resolve(value)).rejects.toThrow('Invalid LFS envelope: missing required fields');
    expect(getBlobUrl).not.toHaveBeenCalled();
  });

  it('rejects unsupported checksum algorithms', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, arrayBuffer: async () => helloBytes.slice().buffer }));
    const resolver = new LfsResolver({ getBlobUrl: async () => 'https://example.invalid/blob' });
    await expect(
      resolver.resolve({ ...sampleEnvelope, checksum: 'x', checksum_alg: 'sha1' })
    ).rejects.toThrow('Unsupported checksum algorithm: sha1');
  });
});
