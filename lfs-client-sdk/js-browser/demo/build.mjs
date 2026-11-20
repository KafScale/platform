// Copyright 2026 KafScale team. Licensed under the Apache License, Version 2.0.
import { build } from 'esbuild';
import { mkdir, copyFile } from 'node:fs/promises';
await mkdir('demo/dist', { recursive: true });
await build({ entryPoints: ['demo/app.js'], bundle: true, format: 'esm', outfile: 'demo/dist/app.js' });
for (const name of ['index.html', 'style.css']) await copyFile(`demo/${name}`, `demo/dist/${name}`);
