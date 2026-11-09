import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { crc32 } from 'node:zlib';
import { S3Client } from '@aws-sdk/client-s3';
import { LfsResolver } from '../resolver.js';

const original = new Uint8Array(262144).fill(42);
const hash = (data: Uint8Array) => createHash('sha256').update(data).digest('hex');
const envelope = (extra = {}) => new TextEncoder().encode(JSON.stringify({
  kfs_lfs: 1, bucket: 'test', key: 'blob', size: original.length, sha256: hash(original), ...extra,
}));
function client(payload: Uint8Array): S3Client {
  return { send: async () => ({ Body: { transformToByteArray: async () => payload } }) } as unknown as S3Client;
}

for (const options of [undefined, { validateChecksum: true }]) {
  test(`rejects same-length S3 corruption (${JSON.stringify(options)})`, async () => {
    const corrupt = original.slice();
    corrupt[0] ^= 1;
    await assert.rejects(new LfsResolver(client(corrupt), 'test', options).resolve(envelope()), /checksum mismatch/i);
  });
}
test('valid object resolves exact bytes', async () => {
  const result = await new LfsResolver(client(original), 'test').resolve(envelope());
  assert.equal(result.isEnvelope, true);
  assert.deepEqual(result.payload, original);
});
test('validation disabled preserves corrupted bytes', async () => {
  const corrupt = original.slice();
  corrupt[0] ^= 1;
  const result = await new LfsResolver(client(corrupt), 'test', { validateChecksum: false }).resolve(envelope());
  assert.deepEqual(result.payload, corrupt);
});
test('checksum overrides legacy sha256', async () => {
  await assert.rejects(new LfsResolver(client(original), 'test').resolve(envelope({ checksum: '0'.repeat(64) })), /checksum mismatch/i);
  const result = await new LfsResolver(client(original), 'test').resolve(envelope({ checksum: hash(original), sha256: 'legacy' }));
  assert.deepEqual(result.payload, original);
});
test('plain records bypass S3 and size guard still applies to envelopes', async () => {
  const resolver = new LfsResolver({ send: () => { throw new Error('unexpected S3 access'); } } as unknown as S3Client, 'test');
  assert.deepEqual(await resolver.resolve(original), { payload: original, isEnvelope: false });
  await assert.rejects(new LfsResolver(client(original), 'test', { maxSize: 1 }).resolve(envelope()), /max size/);
});
const algChecksums: Record<string, string> = {
  sha256: hash(original),
  md5: createHash('md5').update(original).digest('hex'),
  crc32: crc32(original).toString(16).padStart(8, '0'),
  none: '',
};
for (const [alg, checksum] of Object.entries(algChecksums)) {
  const env = () => envelope(checksum ? { checksum, checksum_alg: alg } : { checksum_alg: alg });
  test(`${alg} envelope resolves exact bytes`, async () => {
    const result = await new LfsResolver(client(original), 'test').resolve(env());
    assert.deepEqual(result.payload, original);
  });
  test(`${alg} envelope rejects same-length S3 corruption`, async () => {
    const corrupt = original.slice();
    corrupt[0] ^= 1;
    await assert.rejects(new LfsResolver(client(corrupt), 'test').resolve(env()), /checksum mismatch/i);
  });
}
test('unsupported checksum algorithm is rejected', async () => {
  await assert.rejects(new LfsResolver(client(original), 'test').resolve(envelope({ checksum: 'x', checksum_alg: 'sha1' })), /unsupported checksum algorithm/i);
});
