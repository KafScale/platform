import { S3Client } from '@aws-sdk/client-s3';
import { LfsEnvelope } from './envelope.js';
export interface ResolvedRecord {
    envelope?: LfsEnvelope;
    payload: Uint8Array;
    isEnvelope: boolean;
}
export interface ResolverOptions {
    validateChecksum?: boolean;
    maxSize?: number;
}
export declare class LfsResolver {
    private readonly s3;
    private readonly bucket;
    private readonly validateChecksum;
    private readonly maxSize;
    constructor(s3: S3Client, bucket: string, options?: ResolverOptions);
    resolve(value: Uint8Array): Promise<ResolvedRecord>;
}
