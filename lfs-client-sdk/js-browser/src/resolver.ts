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

import { LfsEnvelope, isLfsEnvelope, decodeLfsEnvelope } from './envelope.js';

export interface ResolvedRecord {
  envelope?: LfsEnvelope;
  payload: Uint8Array;
  isEnvelope: boolean;
}

export interface ResolverConfig {
  /**
   * Function to get a URL for fetching the blob.
   * For pre-signed URLs: return the signed S3 URL.
   * For direct access: return the S3 endpoint URL.
   */
  getBlobUrl: (key: string, bucket: string) => string | Promise<string>;

  /**
   * Validate SHA-256 checksum after download.
   */
  validateChecksum?: boolean;

  /**
   * Maximum allowed payload size (0 = unlimited).
   */
  maxSize?: number;
}

/**
 * Browser-native LFS resolver using fetch API.
 */
export class LfsResolver {
  private readonly getBlobUrl: ResolverConfig['getBlobUrl'];
  private readonly validateChecksum: boolean;
  private readonly maxSize: number;

  constructor(config: ResolverConfig) {
    this.getBlobUrl = config.getBlobUrl;
    this.validateChecksum = config.validateChecksum ?? true;
    this.maxSize = config.maxSize ?? 0;
  }

  /**
   * Resolve an LFS envelope to its blob content.
   * If the value is not an envelope, returns it unchanged.
   */
  async resolve(value: string | Uint8Array | LfsEnvelope): Promise<ResolvedRecord> {
    // Try to parse as envelope
    let envelope: LfsEnvelope;

    if (isLfsEnvelope(value)) {
      envelope = value;
    } else {
      try {
        envelope = decodeLfsEnvelope(value as string | Uint8Array);
      } catch {
        // Not an envelope, return as-is
        const payload =
          typeof value === 'string' ? new TextEncoder().encode(value) : value as Uint8Array;
        return { payload, isEnvelope: false };
      }
    }

    // Fetch blob from URL
    const url = await this.getBlobUrl(envelope.key, envelope.bucket);
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`Failed to fetch blob: ${response.status} ${response.statusText}`);
    }

    const buffer = await response.arrayBuffer();
    const payload = new Uint8Array(buffer);

    if (this.maxSize > 0 && payload.length > this.maxSize) {
      throw new Error(`Payload exceeds max size: ${payload.length} > ${this.maxSize}`);
    }

    if (this.validateChecksum) {
      const expected = envelope.checksum || envelope.sha256;
      const actual = await sha256Hex(payload);
      if (actual !== expected) {
        throw new Error(`Checksum mismatch: expected ${expected}, got ${actual}`);
      }
    }

    return { envelope, payload, isEnvelope: true };
  }
}

/**
 * Compute SHA-256 hash using Web Crypto API.
 */
async function sha256Hex(data: Uint8Array): Promise<string> {
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = new Uint8Array(hashBuffer);
  return Array.from(hashArray)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}
