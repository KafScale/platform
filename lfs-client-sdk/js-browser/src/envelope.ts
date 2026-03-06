// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

/**
 * Check if data looks like an LFS envelope.
 */
export function isLfsEnvelope(data: unknown): data is LfsEnvelope {
  if (typeof data !== 'object' || data === null) return false;
  const obj = data as Record<string, unknown>;
  return (
    typeof obj.kfs_lfs === 'number' &&
    typeof obj.bucket === 'string' &&
    typeof obj.key === 'string' &&
    typeof obj.sha256 === 'string'
  );
}

/**
 * Decode LFS envelope from JSON string or Uint8Array.
 */
export function decodeLfsEnvelope(data: string | Uint8Array): LfsEnvelope {
  const text = typeof data === 'string' ? data : new TextDecoder().decode(data);
  const parsed = JSON.parse(text);
  if (!isLfsEnvelope(parsed)) {
    throw new Error('Invalid LFS envelope: missing required fields');
  }
  return parsed;
}
