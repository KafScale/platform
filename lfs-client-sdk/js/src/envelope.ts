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

export function isLfsEnvelope(value: Uint8Array | null | undefined): boolean {
  if (!value || value.length < 15) return false;
  if (value[0] !== 123) return false;
  const prefix = new TextDecoder().decode(value.slice(0, Math.min(50, value.length)));
  return prefix.includes('"kfs_lfs"');
}

export function decodeEnvelope(value: Uint8Array): LfsEnvelope {
  const env = JSON.parse(new TextDecoder().decode(value)) as LfsEnvelope;
  if (!env.kfs_lfs || !env.bucket || !env.key || !env.sha256) {
    throw new Error('invalid envelope: missing required fields');
  }
  return env;
}
