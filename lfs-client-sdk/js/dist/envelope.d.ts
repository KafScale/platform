export interface LfsEnvelope {
    kfs_lfs: number;
    bucket: string;
    key: string;
    size: number;
    sha256: string;
    checksum?: string;
    checksum_alg?: string;
    content_type?: string;
    original_headers?: Record<string, string>;
    created_at?: string;
    proxy_id?: string;
}
export declare function isLfsEnvelope(value: Uint8Array | null | undefined): boolean;
export declare function decodeEnvelope(value: Uint8Array): LfsEnvelope;
