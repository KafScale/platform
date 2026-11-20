// Copyright 2026 KafScale team. Licensed under the Apache License, Version 2.0.
import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: '.', testMatch: 'demo.spec.mjs', workers: 1,
  timeout: 90_000, expect: { timeout: 15_000 },
  outputDir: 'test-results',
  use: {
    baseURL: process.env.DEMO_BASE_URL || 'http://127.0.0.1:3072',
    browserName: 'chromium',
    launchOptions: process.env.CHROMIUM_EXECUTABLE_PATH ? { executablePath: process.env.CHROMIUM_EXECUTABLE_PATH } : {},
    trace: 'retain-on-failure',
  },
});
