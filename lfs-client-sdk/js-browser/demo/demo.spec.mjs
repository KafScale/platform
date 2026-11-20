// Copyright 2026 KafScale team. Licensed under the Apache License, Version 2.0.
import { test, expect } from '@playwright/test';
import { createHash, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';

async function selectFile(page, bytes = Buffer.from('browser demo fixture')) {
  await page.locator('#file').setInputFiles({ name: 'browser-fixture.bin', mimeType: 'application/octet-stream', buffer: bytes });
}
const fixture = Buffer.from('browser demo fixture');
const envelope = { kfs_lfs: 1, bucket: 'kafscale', key: 'default/@lfs/test', size: fixture.length,
  sha256: createHash('sha256').update(fixture).digest('hex') };
async function mockUpload(page) {
  await page.route('**/lfs/produce', route => route.fulfill({ json: envelope }));
  await selectFile(page);
  await page.getByRole('button', { name: 'Upload to KafScale' }).click();
  await expect(page.locator('#download')).toBeEnabled();
}

test('real proxy round trips text and a file above the SDK multipart threshold', async ({ page }, testInfo) => {
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('/');
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('Large files. Small Kafka records.');
  for (const bytes of [Buffer.from('Hello from the browser!'), randomBytes(6 * 1024 * 1024)]) {
    await selectFile(page, bytes);
    await page.getByRole('button', { name: 'Upload to KafScale' }).click();
    await expect(page.locator('#status')).toContainText('Upload complete');
    const record = JSON.parse(await page.locator('#envelope').textContent());
    expect(record.size).toBe(bytes.length);
    expect(record.sha256).toBe(createHash('sha256').update(bytes).digest('hex'));
    const saved = page.waitForEvent('download');
    await page.getByRole('button', { name: 'Verify and download file' }).click();
    const download = await saved;
    expect(download.suggestedFilename()).toBe('browser-fixture.bin');
    expect(await readFile(await download.path())).toEqual(bytes);
    await expect(page.locator('#status')).toHaveText('Download verified: size and SHA-256 match.');
  }
  expect(errors).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath('demo.png'), fullPage: true });
});

test('shows upload errors and clears stale envelopes', async ({ page }) => {
  await page.goto('/');
  await mockUpload(page);
  await page.unroute('**/lfs/produce');
  await page.route('**/lfs/produce', route => route.fulfill({ status: 503, json: { message: 'Storage unavailable' } }));
  await page.locator('#upload').click();
  await expect(page.locator('#status')).toHaveText('Upload failed: Storage unavailable');
  await expect(page.locator('#download')).toBeDisabled();
  await expect(page.locator('#envelope')).toHaveText('No envelope yet.');
  await expect(page.locator('#upload')).toBeEnabled();
});

for (const mode of ['corrupt', 'truncated', 'proxy error']) {
  test(`refuses ${mode} downloads`, async ({ page }) => {
    await page.goto('/');
    await mockUpload(page);
    let request;
    await page.route('**/lfs/download', route => {
      request = route.request().postDataJSON();
      return route.fulfill(mode === 'proxy error'
        ? { status: 502, body: 'Storage verification failed' }
        : { body: mode === 'corrupt' ? Buffer.alloc(fixture.length, 33) : fixture.subarray(1) });
    });
    const downloads = [];
    page.on('download', file => downloads.push(file));
    await page.locator('#download').click();
    await expect(page.locator('#status')).toContainText('Download failed:');
    expect(request).toEqual({ bucket: envelope.bucket, key: envelope.key, mode: 'stream',
      integrity: { sha256: envelope.sha256, checksum_alg: 'sha256', size: envelope.size } });
    expect(downloads).toHaveLength(0);
    await expect(page.locator('#download')).toBeEnabled();
  });
}

test('rejects empty files without issuing a request', async ({ page }) => {
  let uploads = 0;
  await page.route('**/lfs/produce', route => { uploads++; return route.abort(); });
  await page.goto('/');
  await selectFile(page, Buffer.alloc(0));
  await page.locator('#upload').click();
  await expect(page.locator('#status')).toHaveText('Choose a non-empty file.');
  expect(uploads).toBe(0);
});

test('rejects malformed upload envelopes', async ({ page }) => {
  await page.goto('/');
  await page.route('**/lfs/produce', route => route.fulfill({ json: { ...envelope, sha256: 'not-a-digest' } }));
  await selectFile(page);
  await page.locator('#upload').click();
  await expect(page.locator('#status')).toContainText('Upload failed: The upload returned an invalid size or SHA-256 checksum.');
  await expect(page.locator('#download')).toBeDisabled();
});
