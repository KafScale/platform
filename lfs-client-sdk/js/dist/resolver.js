import { GetObjectCommand } from '@aws-sdk/client-s3';
import { decodeEnvelope, isLfsEnvelope } from './envelope.js';
export class LfsResolver {
    s3;
    bucket;
    validateChecksum;
    maxSize;
    constructor(s3, bucket, options) {
        this.s3 = s3;
        this.bucket = bucket;
        this.validateChecksum = options?.validateChecksum ?? true;
        this.maxSize = options?.maxSize ?? 0;
    }
    async resolve(value) {
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
