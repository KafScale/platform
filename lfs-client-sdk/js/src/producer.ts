import { request } from 'undici';
import { LfsEnvelope } from './envelope.js';

export interface ProduceOptions {
  topic: string;
  key?: Uint8Array;
  headers?: Record<string, string>;
}

export async function produceLfs(endpoint: string, payload: Uint8Array, options: ProduceOptions): Promise<LfsEnvelope> {
  const headers: Record<string, string> = {
    'X-Kafka-Topic': options.topic,
  };
  if (options.key) {
    headers['X-Kafka-Key'] = Buffer.from(options.key).toString('utf8');
  }
  if (options.headers) {
    Object.assign(headers, options.headers);
  }

  const res = await request(endpoint, {
    method: 'POST',
    headers,
    body: payload,
  });
  const body = await res.body.text();
  if (res.statusCode < 200 || res.statusCode >= 300) {
    throw new Error(`produce failed: ${res.statusCode} ${body}`);
  }
  return JSON.parse(body) as LfsEnvelope;
}
