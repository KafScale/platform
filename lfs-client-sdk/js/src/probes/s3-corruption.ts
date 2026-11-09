// Opt-in destructive probe against a dedicated, newly created test bucket.
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { S3Client, CreateBucketCommand, PutObjectCommand, DeleteObjectCommand, DeleteBucketCommand } from '@aws-sdk/client-s3';
import { LfsResolver } from '../resolver.js';

const endpoint = process.env.LFS_S3_ENDPOINT;
if (!endpoint) throw new Error('Set LFS_S3_ENDPOINT to a disposable S3 test service');
const s3 = new S3Client({ endpoint, forcePathStyle: true, region: process.env.AWS_REGION || 'us-east-1' });
const Bucket = `kafscale-integrity-${randomUUID()}`;
const Key = 'payload';
// Override only for reproducing this probe against a compiled baseline resolver.
const Resolver: typeof LfsResolver = process.env.LFS_RESOLVER_MODULE
  ? (await import(process.env.LFS_RESOLVER_MODULE)).LfsResolver : LfsResolver;
await s3.send(new CreateBucketCommand({ Bucket }));
try {
  const original = new Uint8Array(262144).fill(42);
  const sha256 = createHash('sha256').update(original).digest('hex');
  const value = new TextEncoder().encode(JSON.stringify({ kfs_lfs: 1, bucket: Bucket, key: Key, size: original.length, sha256 }));
  await s3.send(new PutObjectCommand({ Bucket, Key, Body: original }));
  for (const options of [undefined, { validateChecksum: true }, { validateChecksum: false }]) {
    assert.deepEqual((await new Resolver(s3, Bucket, options).resolve(value)).payload, original);
  }
  const corrupt = original.slice();
  corrupt[0] ^= 1;
  await s3.send(new PutObjectCommand({ Bucket, Key, Body: corrupt }));
  assert.deepEqual((await new Resolver(s3, Bucket, { validateChecksum: false }).resolve(value)).payload, corrupt);
  for (const options of [undefined, { validateChecksum: true, maxSize: 1048576 }]) {
    await assert.rejects(new Resolver(s3, Bucket, options).resolve(value), /checksum mismatch/i);
  }
  console.log(JSON.stringify({ realS3: true, bytes: original.length, validExactBytes: true, corruptionRejectedDefault: true, corruptionRejectedExplicit: true, validationDisabledExactBytes: true }));
} finally {
  try { await s3.send(new DeleteObjectCommand({ Bucket, Key })); }
  finally { await s3.send(new DeleteBucketCommand({ Bucket })); s3.destroy(); }
}
