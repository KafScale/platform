// Copyright 2026 KafScale team. Licensed under the Apache License, Version 2.0.
import { LfsProducer, decodeLfsEnvelope } from '../src/index.ts';

const form = document.querySelector('#upload-form');
const status = document.querySelector('#status');
const upload = document.querySelector('#upload');
const download = document.querySelector('#download');
const progress = document.querySelector('#progress');
let current;
const producer = new LfsProducer({ endpoint: '/lfs/produce', retries: 1 });

form.addEventListener('submit', async event => {
  event.preventDefault();
  current = undefined;
  download.disabled = true;
  document.querySelector('#envelope').textContent = 'No envelope yet.';
  const file = document.querySelector('#file').files[0];
  if (!file || file.size === 0) {
    status.textContent = 'Choose a non-empty file.';
    return;
  }
  upload.disabled = true;
  progress.value = 0;
  status.textContent = 'Uploading…';
  try {
    const envelope = decodeLfsEnvelope(JSON.stringify(await producer.produce(
      document.querySelector('#topic').value.trim(), file,
      { onProgress: event => { progress.value = event.percent; } },
    )));
    if (!Number.isSafeInteger(envelope.size) || envelope.size !== file.size || !/^[a-f0-9]{64}$/i.test(envelope.sha256)) {
      throw new Error('The upload returned an invalid size or SHA-256 checksum.');
    }
    current = { envelope, name: file.name };
    document.querySelector('#envelope').textContent = JSON.stringify(envelope, null, 2);
    progress.value = 100;
    download.disabled = false;
    status.textContent = 'Upload complete. The Kafka envelope is ready.';
  } catch (error) {
    status.textContent = `Upload failed: ${error.message}`;
  } finally {
    upload.disabled = false;
  }
});

download.addEventListener('click', async () => {
  if (!current) return;
  const { envelope, name } = current;
  download.disabled = true;
  upload.disabled = true;
  status.textContent = 'Downloading and verifying…';
  let url;
  try {
    const response = await fetch('/lfs/download', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ bucket: envelope.bucket, key: envelope.key, mode: 'stream',
        integrity: { sha256: envelope.sha256, checksum_alg: 'sha256', size: envelope.size } }),
    });
    if (!response.ok) throw new Error(`Proxy returned HTTP ${response.status}: ${await response.text()}`);
    const bytes = await response.arrayBuffer();
    const hash = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)), b => b.toString(16).padStart(2, '0')).join('');
    if (bytes.byteLength !== envelope.size || hash !== envelope.sha256.toLowerCase()) {
      throw new Error('Downloaded bytes do not match the envelope.');
    }
    url = URL.createObjectURL(new Blob([bytes], { type: 'application/octet-stream' }));
    const link = document.createElement('a');
    link.href = url;
    link.download = name;
    link.click();
    status.textContent = 'Download verified: size and SHA-256 match.';
  } catch (error) {
    status.textContent = `Download failed: ${error.message}`;
  } finally {
    // Keep the object URL alive until the browser has begun the download.
    if (url) setTimeout(() => URL.revokeObjectURL(url), 1000);
    download.disabled = false;
    upload.disabled = false;
  }
});
