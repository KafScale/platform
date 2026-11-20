// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { test } = require('node:test');
const vm = require('node:vm');
const appPath = path.join(__dirname, 'public/app.js');
const script = new vm.Script(readFileSync(appPath, 'utf8'), { filename: appPath });

function element() {
  const classes = new Set();
  return {
    hidden: true, children: [], listeners: {}, textContent: '', innerHTML: '', value: '',
    classList: {
      add: (...names) => names.forEach(name => classes.add(name)),
      remove: (...names) => names.forEach(name => classes.delete(name)),
      contains: name => classes.has(name),
      toggle: (name, active) => active ? classes.add(name) : classes.delete(name),
    },
    addEventListener(name, fn) { this.listeners[name] = fn; },
    appendChild(child) { this.children.push(child); },
    querySelector() { return element(); },
    querySelectorAll() { return []; },
  };
}

function app(statusResponse, clusterResponse = () => ({ ok: true, json: async () => ({}) })) {
  const elements = new Map();
  const get = id => {
    if (!elements.has(id)) elements.set(id, Object.assign(element(), { id }));
    return elements.get(id);
  };
  const content = Array.from({ length: 8 }, element);
  const requests = [];
  const streams = [];
  const context = vm.createContext({
    Intl, console, setInterval: () => 1, clearInterval: () => {},
    document: {
      getElementById: get,
      querySelector: get,
      querySelectorAll: selector => selector === '.lfs-enabled-content' ? content : [],
      createElement: element,
    },
    fetch: async url => {
      // Leave authentication bootstrap pending; these tests drive LFS lifecycle
      // explicitly and do not replace the application functions under test.
      if (url === '/ui/api/auth/config') return new Promise(() => {});
      requests.push(url);
      if (url === '/ui/api/lfs/status') return statusResponse();
      if (url === '/ui/api/status') return clusterResponse();
      return { ok: true, json: async () => ({}) };
    },
    EventSource: class {
      constructor(url) { this.url = url; this.closed = false; streams.push(this); }
      close() { this.closed = true; }
    },
  });
  script.runInContext(context);
  return { context, requests, streams, get, content };
}

function status(enabled) {
  return { ok: true, status: 200, json: async () => ({ enabled }) };
}

function deferred() {
  let resolve, reject;
  const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}

async function assertNoDisabledRequests(page) {
  await page.context.fetchLfsTopics();
  await page.context.fetchLfsObjects();
  await page.context.fetchLfsOrphans();
  await page.context.fetchLfsS3Browse();
  page.context.startLfsEventsStream();
  assert.deepEqual(page.requests, ['/ui/api/lfs/status']);
  assert.equal(page.streams.length, 0);
  assert.ok(page.content.every(item => item.hidden));
}

test('disabled LFS requests only status, including refresh actions', async () => {
  const page = app(() => status(false));
  await page.context.startLfsConsole();
  assert.equal(page.get('lfs-status-text').textContent, 'LFS Disabled');
  await assertNoDisabledRequests(page);
  page.get('lfs-objects-refresh').listeners.click();
  page.get('lfs-events-filter').listeners.change();
  page.get('lfs-s3-refresh').listeners.click();
  await assertNoDisabledRequests(page);
});

test('enabled LFS starts data requests only after status resolves', async () => {
  const pending = deferred();
  const page = app(() => pending.promise);
  const started = page.context.startLfsConsole();
  await assertNoDisabledRequests(page);
  pending.resolve(status(true));
  await started;
  assert.deepEqual(page.requests, [
    '/ui/api/lfs/status', '/ui/api/lfs/topics', '/ui/api/lfs/objects?limit=50',
    '/ui/api/lfs/orphans', '/ui/api/lfs/s3/browse?prefix=&delimiter=/&max_keys=100',
  ]);
  assert.equal(page.streams.length, 1);
  assert.equal(page.streams[0].url, '/ui/api/lfs/events');
  assert.ok(page.content.every(item => !item.hidden));
  page.context.stopLfsConsole();
  assert.ok(page.streams[0].closed);
  assert.ok(page.content.every(item => item.hidden));
});

for (const stage of ['response', 'json']) {
  for (const stop of ['stopLfsConsole', 'showLogin']) {
    test(`late ${stage} cannot start LFS after ${stop}`, async () => {
      const pending = deferred();
      const jsonStarted = deferred();
      const page = app(() => stage === 'response' ? pending.promise : { ok: true, status: 200, json: () => { jsonStarted.resolve(); return pending.promise; } });
      const started = page.context.startLfsConsole();
      if (stage === 'json') await jsonStarted.promise;
      page.context[stop]();
      pending.resolve(stage === 'response' ? status(true) : { enabled: true });
      await started;
      await assertNoDisabledRequests(page);
    });
  }
}

for (const code of [401, 503, 500]) {
  test(`status ${code} never starts LFS data requests`, async () => {
    const page = app(() => ({ ok: false, status: code }));
    await page.context.startLfsConsole();
    await assertNoDisabledRequests(page);
  });
}

for (const failure of ['network', 'json']) {
  test(`${failure} failure never starts LFS data requests`, async () => {
    const page = app(() => {
      if (failure === 'network') throw new Error('offline');
      return { ok: true, status: 200, json: async () => { throw new Error('invalid JSON'); } };
    });
    await page.context.startLfsConsole();
    await assertNoDisabledRequests(page);
  });
}

test('a late error from an old stream cannot close its replacement', async () => {
  const page = app(() => status(true));
  await page.context.startLfsConsole();
  const old = page.streams[0];
  await page.context.startLfsConsole();
  const current = page.streams[1];
  assert.ok(old.closed);
  old.onerror();
  assert.equal(current.closed, false);
  current.onerror();
  assert.equal(current.closed, true);
});

test('logging out closes the active LFS stream', async () => {
  const page = app(() => status(true));
  await page.context.startLfsConsole();
  page.context.showLogin();
  assert.ok(page.streams[0].closed);
  assert.ok(page.content.every(item => item.hidden));
});

test('signing back in with LFS tab selected checks status again', async () => {
  const page = app(() => status(false));
  page.get('tab-lfs').classList.add('active');
  page.context.fetchStatus = () => {};
  page.context.startMetricsStream = () => {};
  await page.context.startConsole();
  await assertNoDisabledRequests(page);
});


test('late status failure cannot overwrite a newer disabled result', async () => {
  const pending = deferred();
  let calls = 0;
  const page = app(() => ++calls === 1 ? pending.promise : status(false));
  const old = page.context.startLfsConsole();
  await page.context.startLfsConsole();
  pending.reject(new Error('old request failed'));
  await old;
  assert.equal(page.get('lfs-status-text').textContent, 'LFS Disabled');
  assert.equal(page.streams.length, 0);
});

test('stream filters replace the stream and ignore stale callbacks', async () => {
  const page = app(() => status(true));
  const received = [];
  page.context.addLfsEvent = event => received.push(event);
  await page.context.startLfsConsole();
  const old = page.streams[0];
  page.context.startLfsEventsStream('upload_completed');
  const current = page.streams[1];
  assert.ok(old.closed);
  assert.equal(current.url, '/ui/api/lfs/events?types=upload_completed');
  old.onopen();
  old.onmessage({ data: '{"event_type":"old"}' });
  assert.equal(received.length, 0);
  current.onopen();
  assert.equal(page.get('lfs-events-status').textContent, 'Connected to event stream');
  current.onmessage({ data: 'keepalive' });
  current.onmessage({ data: '{"event_type":"upload_completed"}' });
  assert.equal(received.length, 1);
  assert.equal(received[0].event_type, 'upload_completed');
});


for (const source of ['lfs', 'cluster']) {
  for (const code of [401, 503]) {
    test(`${source} status ${code} distinguishes service failure from authentication`, async () => {
      const response = () => ({ ok: false, status: code });
      const page = app(source === 'lfs' ? response : () => status(true), response);
      page.context.showApp();
      page.context.startMetricsStream();
      if (source === 'cluster') {
        await page.context.startLfsConsole();
        await page.context.fetchStatus();
      } else {
        await page.context.startLfsConsole();
      }
      const expired = code === 401;
      assert.equal(page.get('app').classList.contains('screen-active'), !expired);
      assert.equal(page.get('login-screen').classList.contains('screen-active'), expired);
      assert.ok(page.streams.every(stream => stream.closed === expired));
      if (!expired) {
        if (source === 'lfs') {
          assert.equal(page.get('lfs-status-text').textContent, 'LFS unavailable');
          assert.deepEqual(page.requests, ['/ui/api/lfs/status']);
          assert.ok(page.content.every(item => item.hidden));
        } else {
          assert.equal(page.get('cluster-summary').textContent, 'Failed to load status: status 503');
        }
      }
    });
  }
}

test('etcd snapshot access shows n/a when the stream has no access value', () => {
  const page = app(() => status(false));
  page.context.startMetricsStream();
  const stream = page.streams[page.streams.length - 1];
  const access = page.get('etcd-snapshot-access');
  stream.onmessage({ data: JSON.stringify({ metrics: { etcd_snapshot_age_s: 10, etcd_snapshot_stale: 0, etcd_snapshot_access_ok: 0 } }) });
  assert.equal(access.textContent, 'FAIL');
  stream.onmessage({ data: JSON.stringify({ metrics: { etcd_snapshot_age_s: 10, etcd_snapshot_stale: 0 } }) });
  assert.equal(access.textContent, 'n/a');
  assert.equal(page.get('etcd-snapshot-age').textContent, '10');
  stream.onmessage({ data: JSON.stringify({ metrics: { etcd_snapshot_age_s: 10, etcd_snapshot_stale: 0, etcd_snapshot_access_ok: 1 } }) });
  assert.equal(access.textContent, 'OK');
  stream.onmessage({ data: JSON.stringify({ metrics: { etcd_snapshot_age_s: 0, etcd_snapshot_stale: 0, etcd_snapshot_last_success_ts: 1700000000 } }) });
  assert.equal(page.get('etcd-snapshot-age').textContent, '0');
});

test('missing metrics render n/a and a snapshot that never succeeded renders never', () => {
  const page = app(() => status(false));
  page.context.startMetricsStream();
  const stream = page.streams[page.streams.length - 1];
  stream.onmessage({ data: JSON.stringify({ timestamp: Date.parse('2026-11-20T12:00:00Z'), metrics_available: false, metrics: {} }) });
  for (const id of ['produce-rps', 'fetch-rps', 'live-s3-latency', 's3-error-rate', 'admin-requests', 'admin-errors', 'admin-latency', 'etcd-snapshot-age']) {
    assert.equal(page.get(id).textContent, 'n/a', id);
  }
  assert.match(page.get('metrics-status').textContent, /^Metrics unavailable /);

  stream.onmessage({ data: JSON.stringify({ metrics_available: false }) });
  assert.equal(page.get('metrics-status').textContent, 'Metrics unavailable');

  stream.onmessage({ data: JSON.stringify({ metrics: { produce_rps: 0, s3_error_rate: 0 } }) });
  assert.equal(page.get('produce-rps').textContent, '0');
  assert.equal(page.get('s3-error-rate').textContent, '0%');
  assert.equal(page.get('metrics-status').textContent, 'Live metrics');

  stream.onmessage({ data: JSON.stringify({
    timestamp: Date.parse('2026-11-20T12:00:00Z'),
    metrics_available: true,
    metrics: {
      etcd_snapshot_age_s: 0,
      etcd_snapshot_stale: 1,
      etcd_snapshot_access_ok: 1,
      etcd_snapshot_last_success_ts: 0,
      etcd_snapshot_last_schedule_ts: 1700000000,
    },
  }) });
  assert.equal(page.get('etcd-snapshot-age').textContent, 'never');
  assert.equal(page.get('etcd-snapshot-stale').textContent, 'YES');
  assert.equal(page.get('etcd-snapshot-access').textContent, 'OK');
  assert.equal(page.get('etcd-snapshot-last-success').textContent, 'n/a');
  assert.notEqual(page.get('etcd-snapshot-last-schedule').textContent, 'n/a');
  assert.match(page.get('metrics-status').textContent, /^Last update /);

  page.context.setMetricValue(page.get('produce-rps'), Number.NaN);
  assert.equal(page.get('produce-rps').textContent, 'n/a');
  page.context.setMetricValue(null, 1);
});
