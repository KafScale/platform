# Plan: E30 Flink KafScale Demo

## Goals
- Add E30 Flink word-count demo with header/key/value stats.
- Support local broker (make demo) and in-cluster run.
- Use profiles to switch bootstrap servers.
- Document onboarding in 101_kafscale-dev-guide.

## Steps
1) Build Maven project for Flink job with profile-based config.
2) Implement Kafka source + word count + missing-key/header/value stats.
3) Add Dockerfile and Kubernetes manifest for cluster run.
4) Update dev guide + SUMMARY with new section and run instructions.

## Status
- [x] Inspected existing demos and deploy patterns.
- [x] Created E30 demo project and job implementation.
- [x] Added container packaging + k8s manifest.
- [x] Updated dev guide with new section.
- [x] Added helper scripts for standalone Flink + local demo + K8S stack.
- [x] Updated E30 README and dev guide with scripts + verification steps.
