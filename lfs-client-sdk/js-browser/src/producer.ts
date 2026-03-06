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

import { LfsEnvelope } from './envelope.js';

export interface UploadProgress {
  loaded: number;
  total: number;
  percent: number;
}

export interface ProduceOptions {
  key?: string;
  headers?: Record<string, string>;
  onProgress?: (progress: UploadProgress) => void;
  signal?: AbortSignal;
}

export interface LfsProducerConfig {
  endpoint: string;
  timeout?: number;
  retries?: number;
  retryDelay?: number;
}

export class LfsHttpError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
    public readonly requestId: string,
    public readonly body: string
  ) {
    super(message);
    this.name = 'LfsHttpError';
  }
}

const DEFAULT_TIMEOUT = 300000; // 5 minutes
const DEFAULT_RETRIES = 3;
const DEFAULT_RETRY_DELAY = 200;

/**
 * Browser-native LFS producer using fetch API.
 */
export class LfsProducer {
  private readonly endpoint: string;
  private readonly timeout: number;
  private readonly retries: number;
  private readonly retryDelay: number;

  constructor(config: LfsProducerConfig) {
    this.endpoint = config.endpoint;
    this.timeout = config.timeout ?? DEFAULT_TIMEOUT;
    this.retries = config.retries ?? DEFAULT_RETRIES;
    this.retryDelay = config.retryDelay ?? DEFAULT_RETRY_DELAY;
  }

  /**
   * Upload a blob to the LFS proxy.
   */
  async produce(
    topic: string,
    payload: Blob | ArrayBuffer | File,
    options?: ProduceOptions
  ): Promise<LfsEnvelope> {
    const headers: Record<string, string> = {
      'X-Kafka-Topic': topic,
      'X-Request-ID': crypto.randomUUID(),
    };

    if (options?.key) {
      headers['X-Kafka-Key'] = options.key;
    }

    if (options?.headers) {
      Object.assign(headers, options.headers);
    }

    // Get payload as Blob for size info
    const blob = payload instanceof Blob ? payload : new Blob([payload]);
    headers['X-LFS-Size'] = String(blob.size);
    headers['X-LFS-Mode'] = blob.size < 5 * 1024 * 1024 ? 'single' : 'multipart';

    // If Content-Type not set, use blob type
    if (!headers['Content-Type'] && blob.type) {
      headers['Content-Type'] = blob.type;
    }

    return this.sendWithRetry(blob, headers, options);
  }

  private async sendWithRetry(
    blob: Blob,
    headers: Record<string, string>,
    options?: ProduceOptions
  ): Promise<LfsEnvelope> {
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= this.retries; attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        // Combine with external signal if provided
        if (options?.signal) {
          options.signal.addEventListener('abort', () => controller.abort());
        }

        try {
          // Use XMLHttpRequest for progress tracking (fetch doesn't support upload progress)
          const envelope = await this.uploadWithProgress(
            blob,
            headers,
            controller.signal,
            options?.onProgress
          );
          return envelope;
        } finally {
          clearTimeout(timeoutId);
        }
      } catch (error) {
        lastError = error as Error;

        // Don't retry on abort
        if (error instanceof DOMException && error.name === 'AbortError') {
          throw error;
        }

        // Don't retry on 4xx errors
        if (error instanceof LfsHttpError && error.statusCode < 500) {
          throw error;
        }

        // Retry on 5xx or network errors
        if (attempt < this.retries) {
          await this.sleep(this.retryDelay * Math.pow(2, attempt - 1));
        }
      }
    }

    throw lastError ?? new Error('Upload failed: no response');
  }

  private uploadWithProgress(
    blob: Blob,
    headers: Record<string, string>,
    signal: AbortSignal,
    onProgress?: (progress: UploadProgress) => void
  ): Promise<LfsEnvelope> {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();

      xhr.open('POST', this.endpoint, true);

      // Set headers
      for (const [key, value] of Object.entries(headers)) {
        xhr.setRequestHeader(key, value);
      }

      // Progress handler
      if (onProgress) {
        xhr.upload.onprogress = (event) => {
          if (event.lengthComputable) {
            onProgress({
              loaded: event.loaded,
              total: event.total,
              percent: Math.round((event.loaded / event.total) * 100),
            });
          }
        };
      }

      // Abort handler
      signal.addEventListener('abort', () => xhr.abort());

      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          try {
            const envelope = JSON.parse(xhr.responseText) as LfsEnvelope;
            resolve(envelope);
          } catch {
            reject(new Error('Invalid JSON response'));
          }
        } else {
          let code = '';
          let message = xhr.responseText;
          let requestId = headers['X-Request-ID'];

          try {
            const err = JSON.parse(xhr.responseText);
            code = err.code ?? '';
            message = err.message ?? xhr.responseText;
            requestId = err.request_id ?? requestId;
          } catch {
            // Use raw response
          }

          reject(new LfsHttpError(xhr.status, code, message, requestId, xhr.responseText));
        }
      };

      xhr.onerror = () => reject(new Error('Network error'));
      xhr.onabort = () => reject(new DOMException('Upload aborted', 'AbortError'));

      xhr.send(blob);
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

/**
 * Convenience function for one-shot uploads.
 */
export async function produceLfs(
  endpoint: string,
  topic: string,
  payload: Blob | ArrayBuffer | File,
  options?: ProduceOptions
): Promise<LfsEnvelope> {
  const producer = new LfsProducer({ endpoint });
  return producer.produce(topic, payload, options);
}
