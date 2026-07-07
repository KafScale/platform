import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { LfsHttpError, LfsProducer, produceLfs } from '../producer.js';

const sampleEnvelope = {
  kfs_lfs: 1,
  bucket: 'demo-bucket',
  key: 'default/topic/0/segment.bin',
  size: 5,
  sha256: 'abc123',
};

class MockXMLHttpRequest {
  static instances: MockXMLHttpRequest[] = [];

  method = '';
  url = '';
  async = false;
  status = 0;
  responseText = '';
  requestHeaders: Record<string, string> = {};
  upload = { onprogress: null as ((event: ProgressEvent) => void) | null };
  onload: (() => void) | null = null;
  onerror: (() => void) | null = null;
  onabort: (() => void) | null = null;
  sentBody: Blob | null = null;

  constructor() {
    MockXMLHttpRequest.instances.push(this);
  }

  open(method: string, url: string, async: boolean) {
    this.method = method;
    this.url = url;
    this.async = async;
  }

  setRequestHeader(key: string, value: string) {
    this.requestHeaders[key] = value;
  }

  send(body?: Blob) {
    this.sentBody = body ?? null;
    queueMicrotask(() => responder(this, MockXMLHttpRequest.instances.length));
  }

  abort() {
    this.onabort?.();
  }

  succeed(body: unknown, status = 200) {
    this.status = status;
    this.responseText = JSON.stringify(body);
    this.onload?.();
  }

  fail(status: number, body: string | Record<string, unknown>) {
    this.status = status;
    this.responseText = typeof body === 'string' ? body : JSON.stringify(body);
    this.onload?.();
  }

  networkError() {
    this.onerror?.();
  }

  emitProgress(loaded: number, total: number) {
    this.upload.onprogress?.({
      lengthComputable: true,
      loaded,
      total,
    } as ProgressEvent);
  }
}

let responder: (xhr: MockXMLHttpRequest, attempt: number) => void = () => {
  throw new Error('XMLHttpRequest responder not configured');
};

beforeEach(() => {
  MockXMLHttpRequest.instances = [];
  responder = () => {
    throw new Error('XMLHttpRequest responder not configured');
  };

  vi.stubGlobal('XMLHttpRequest', MockXMLHttpRequest as unknown as typeof XMLHttpRequest);
  vi.stubGlobal('crypto', {
    randomUUID: () => 'test-request-id',
  });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('LfsProducer', () => {
  it('uploads payloads and returns the proxy envelope', async () => {
    responder = (xhr) => xhr.succeed(sampleEnvelope);

    const producer = new LfsProducer({ endpoint: 'https://proxy.example/lfs' });
    const payload = new Blob(['hello'], { type: 'text/plain' });
    const envelope = await producer.produce('orders', payload, { key: 'order-1' });

    expect(envelope).toEqual(sampleEnvelope);
    expect(MockXMLHttpRequest.instances).toHaveLength(1);

    const xhr = MockXMLHttpRequest.instances[0];
    expect(xhr.method).toBe('POST');
    expect(xhr.url).toBe('https://proxy.example/lfs');
    expect(xhr.requestHeaders).toMatchObject({
      'X-Kafka-Topic': 'orders',
      'X-Kafka-Key': 'order-1',
      'X-Request-ID': 'test-request-id',
      'X-LFS-Size': '5',
      'X-LFS-Mode': 'single',
      'Content-Type': 'text/plain',
    });
    expect(xhr.sentBody).toBe(payload);
  });

  it('uses multipart mode for large payloads', async () => {
    responder = (xhr) => xhr.succeed(sampleEnvelope);

    const producer = new LfsProducer({ endpoint: 'https://proxy.example/lfs' });
    await producer.produce('orders', new Blob([new Uint8Array(5 * 1024 * 1024)]));

    expect(MockXMLHttpRequest.instances[0].requestHeaders['X-LFS-Mode']).toBe('multipart');
  });

  it('reports upload progress', async () => {
    responder = (xhr) => {
      xhr.emitProgress(25, 100);
      xhr.succeed(sampleEnvelope);
    };

    const onProgress = vi.fn();
    const producer = new LfsProducer({ endpoint: 'https://proxy.example/lfs' });
    await producer.produce('orders', new Blob(['hello']), { onProgress });

    expect(onProgress).toHaveBeenCalledWith({
      loaded: 25,
      total: 100,
      percent: 25,
    });
  });

  it('maps 4xx responses to LfsHttpError without retrying', async () => {
    responder = (xhr) =>
      xhr.fail(404, {
        code: 'TOPIC_NOT_FOUND',
        message: 'topic missing',
        request_id: 'server-request-id',
      });

    const producer = new LfsProducer({
      endpoint: 'https://proxy.example/lfs',
      retries: 3,
      retryDelay: 10,
    });

    await expect(producer.produce('orders', new Blob(['hello']))).rejects.toSatisfy(
      (error: unknown) =>
        error instanceof LfsHttpError &&
        error.statusCode === 404 &&
        error.code === 'TOPIC_NOT_FOUND' &&
        error.message === 'topic missing' &&
        error.requestId === 'server-request-id'
    );
    expect(MockXMLHttpRequest.instances).toHaveLength(1);
  });

  it('retries 5xx failures before succeeding', async () => {
    vi.useFakeTimers();

    let attempts = 0;
    responder = (xhr) => {
      attempts += 1;
      if (attempts < 3) {
        xhr.fail(503, 'temporary outage');
        return;
      }
      xhr.succeed(sampleEnvelope);
    };

    const producer = new LfsProducer({
      endpoint: 'https://proxy.example/lfs',
      retries: 3,
      retryDelay: 10,
    });

    const promise = producer.produce('orders', new Blob(['hello']));
    await vi.runAllTimersAsync();
    const envelope = await promise;

    expect(envelope).toEqual(sampleEnvelope);
    expect(MockXMLHttpRequest.instances).toHaveLength(3);
  });

  it('rejects invalid JSON responses', async () => {
    responder = (xhr) => {
      xhr.status = 200;
      xhr.responseText = 'not-json';
      xhr.onload?.();
    };

    const producer = new LfsProducer({ endpoint: 'https://proxy.example/lfs', retries: 1 });
    await expect(producer.produce('orders', new Blob(['hello']))).rejects.toThrow(
      'Invalid JSON response'
    );
  });

  it('propagates abort signals', async () => {
    responder = () => {
      // Keep the request pending until the caller aborts it.
    };

    const controller = new AbortController();
    const producer = new LfsProducer({ endpoint: 'https://proxy.example/lfs', retries: 3 });
    const promise = producer.produce('orders', new Blob(['hello']), {
      signal: controller.signal,
    });

    await Promise.resolve();
    controller.abort();

    await expect(promise).rejects.toSatisfy(
      (error: unknown) => error instanceof DOMException && error.name === 'AbortError'
    );
    expect(MockXMLHttpRequest.instances).toHaveLength(1);
  });
});

describe('produceLfs', () => {
  it('uploads via the convenience helper', async () => {
    responder = (xhr) => xhr.succeed(sampleEnvelope);

    const envelope = await produceLfs(
      'https://proxy.example/lfs',
      'orders',
      new Blob(['hello'])
    );

    expect(envelope).toEqual(sampleEnvelope);
    expect(MockXMLHttpRequest.instances[0].requestHeaders['X-Kafka-Topic']).toBe('orders');
  });
});