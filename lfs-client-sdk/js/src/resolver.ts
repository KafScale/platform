import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { decodeEnvelope, isLfsEnvelope, LfsEnvelope } from './envelope.js';

export interface ResolvedRecord {
  envelope?: LfsEnvelope;
  payload: Uint8Array;
  isEnvelope: boolean;
}

export interface ResolverOptions {
  validateChecksum?: boolean;
  maxSize?: number;
}

export class LfsResolver {
  private readonly s3: S3Client;
  private readonly bucket: string;
  private readonly validateChecksum: boolean;
  private readonly maxSize: number;

  constructor(s3: S3Client, bucket: string, options?: ResolverOptions) {
    this.s3 = s3;
    this.bucket = bucket;
    this.validateChecksum = options?.validateChecksum ?? true;
    this.maxSize = options?.maxSize ?? 0;
  }

  async resolve(value: Uint8Array): Promise<ResolvedRecord> {
    if (!isLfsEnvelope(value)) {
      return { payload: value, isEnvelope: false };
    }
    const env = decodeEnvelope(value);
    const obj = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: env.key }));
    const body = await obj.Body?.transformToByteArray();
    const payload = body ?? new Uint8Array();
    if (this.maxSize > 0 && payload.length > this.maxSize) {
      throw new Error('payload exceeds max size');
    }
    if (this.validateChecksum) {
      // checksum validation placeholder (sha256)
    }
    return { envelope: env, payload, isEnvelope: true };
  }
}
