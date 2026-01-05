---
layout: doc
title: Benchmarks
description: Repeatable benchmark scenarios and baseline results for KafScale.
permalink: /benchmarks/
nav_title: Benchmarks
nav_order: 9
nav_group: References
---

<!--
Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Benchmarking

This document captures repeatable benchmark scenarios for Kafscale. It focuses on end-to-end throughput and latency across the broker and S3-backed storage.

The local runner lives in `docs/benchmarks/scripts/run_local.sh` and can be used to reproduce the baseline numbers on a demo platform setup. A tiny Makefile wrapper is available at `docs/benchmarks/Makefile`.

The runner uses dedicated topics (`bench-hot`, `bench-s3`, `bench-multi`) and will create them if `kafka-topics.sh` is available. Otherwise, it falls back to auto-creation behavior in the demo stack.

## Goals

- Quantify produce/consume throughput on the broker path.
- Quantify consume performance when reading from S3-backed segments.
- Capture tail latency and error rates under steady load.

## General Guidance

- **Stop the demo workload** before benchmarking to avoid noise.
- Keep topic/partition counts fixed for each run.
- Record system resources (CPU, memory, disk, network) alongside metrics.
- Use consistent message size and producer acks across runs.

<div class="diagram">
<svg viewBox="0 0 600 260" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale throughput by scenario">
  <defs>
    <marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="var(--diagram-stroke)"/></marker>
  </defs>

  <!-- Title -->
  <text x="24" y="28" font-size="13" font-weight="600" fill="var(--diagram-text)">Throughput by Scenario</text>
  <text x="24" y="46" font-size="10" fill="var(--diagram-label)">Local demo on kind · 256B messages · kcat client</text>

  <!-- Row 1: Produce (cross-partition) - 7,500 msg/s -->
  <text x="24" y="82" font-size="11" fill="var(--diagram-text)">Produce (cross-partition)</text>
  <rect x="200" y="68" width="300" height="20" rx="4" fill="rgba(52, 211, 153, 0.25)" stroke="#34d399" stroke-width="1"/>
  <text x="510" y="82" font-size="11" font-weight="600" fill="var(--diagram-text)">7,500</text>
  <text x="548" y="82" font-size="10" fill="var(--diagram-label)">msg/s</text>

  <!-- Row 2: Produce (single topic) - 4,115 msg/s -->
  <text x="24" y="114" font-size="11" fill="var(--diagram-text)">Produce (single topic)</text>
  <rect x="200" y="100" width="165" height="20" rx="4" fill="rgba(52, 211, 153, 0.25)" stroke="#34d399" stroke-width="1"/>
  <text x="510" y="114" font-size="11" font-weight="600" fill="var(--diagram-text)">4,115</text>
  <text x="548" y="114" font-size="10" fill="var(--diagram-label)">msg/s</text>

  <!-- Row 3: Consume (cross-partition) - 1,629 msg/s -->
  <text x="24" y="146" font-size="11" fill="var(--diagram-text)">Consume (cross-partition)</text>
  <rect x="200" y="132" width="65" height="20" rx="4" fill="rgba(106, 167, 255, 0.25)" stroke="#6aa7ff" stroke-width="1"/>
  <text x="510" y="146" font-size="11" font-weight="600" fill="var(--diagram-text)">1,629</text>
  <text x="548" y="146" font-size="10" fill="var(--diagram-label)">msg/s</text>

  <!-- Row 4: Consume (cold read) - 1,388 msg/s -->
  <text x="24" y="178" font-size="11" fill="var(--diagram-text)">Consume (cold read)</text>
  <rect x="200" y="164" width="55" height="20" rx="4" fill="rgba(106, 167, 255, 0.25)" stroke="#6aa7ff" stroke-width="1"/>
  <text x="510" y="178" font-size="11" font-weight="600" fill="var(--diagram-text)">1,388</text>
  <text x="548" y="178" font-size="10" fill="var(--diagram-label)">msg/s</text>

  <!-- Row 5: Consume (S3 backlog) - 1,117 msg/s -->
  <text x="24" y="210" font-size="11" fill="var(--diagram-text)">Consume (S3 backlog)</text>
  <rect x="200" y="196" width="45" height="20" rx="4" fill="rgba(106, 167, 255, 0.25)" stroke="#6aa7ff" stroke-width="1"/>
  <text x="510" y="210" font-size="11" font-weight="600" fill="var(--diagram-text)">1,117</text>
  <text x="548" y="210" font-size="10" fill="var(--diagram-label)">msg/s</text>

  <!-- Legend -->
  <rect x="200" y="235" width="14" height="14" rx="3" fill="rgba(52, 211, 153, 0.25)" stroke="#34d399" stroke-width="1"/>
  <text x="220" y="246" font-size="10" fill="var(--diagram-label)">Produce</text>
  <rect x="290" y="235" width="14" height="14" rx="3" fill="rgba(106, 167, 255, 0.25)" stroke="#6aa7ff" stroke-width="1"/>
  <text x="310" y="246" font-size="10" fill="var(--diagram-label)">Consume</text>

  <!-- Footnote -->
  <text x="400" y="246" font-size="9" fill="var(--diagram-label)">Cold read = after broker restart</text>
</svg>
</div>

## Scenarios

### 1) Produce and Consume via Broker

This is the hot path: produce to the broker and consume immediately.

**What to measure**
- Produce throughput (msg/s and MB/s).
- Consume throughput (msg/s and MB/s).
- p50/p95/p99 produce and fetch latency.
- Fetch/produce error rates.

**Notes**
- Use a single consumer group and fixed partition count.
- Keep message size constant (for example 1 KB, 10 KB, 100 KB).

### 2) Produce via Broker, Consume from S3

This validates catch-up and deep reads from S3-backed segments.

**What to measure**
- Catch-up rate when consuming from earliest offsets.
- Steady-state fetch latency when reading older segments.
- S3 request latency and error rate.

**Notes**
- Preload data (produce a backlog), then consume from `-o beginning`.
- Record the time to drain N records or M bytes.

## Optional Scenarios

- **Backlog catch-up**: produce a large backlog, then measure how quickly a new consumer group catches up to head.
- **Partition scale**: repeat scenarios with 1, 3, 6, 12 partitions.
- **Message size sweep**: 1 KB, 10 KB, 100 KB to characterize bandwidth.
- **Multi-consumer fan-out**: multiple consumer groups reading the same topic.
- **Offset recovery**: restart consumer and measure resume time.

## Metrics to Capture

- Broker metrics (`/metrics`): `kafscale_produce_rps`, `kafscale_fetch_rps`, `kafscale_s3_latency_ms_avg`, `kafscale_s3_error_rate`.
- Client-side latency distribution (p50/p95/p99).
- End-to-end throughput (msg/s and MB/s).

## Reporting Template

Record each run with:

- **Scenario**:
- **Topic/Partitions**:
- **Message Size**:
- **Producer Acks**:
- **Produce Throughput**:
- **Consume Throughput**:
- **Latency p95/p99**:
- **S3 Health**:
- **Notes**:

## Local Run (2026-01-03)

Local demo setup on kind (demo workload stopped). Commands and results below document the baseline numbers we observed on a laptop.

### Broker Produce + Consume (Hot Path)

**Command**

```sh
time env TOPIC=bench-hot N=2000 SIZE=256 sh -c 'kcat -C -b 127.0.0.1:39092 -t "$TOPIC" -o end -c "$N" -e -q >/tmp/bench-consume-broker.log & cons=$!; sleep 1; python3 - <<\"PY\" | kcat -P -b 127.0.0.1:39092 -t "$TOPIC" -q
import os,sys
n=int(os.environ["N"])
size=int(os.environ["SIZE"])
line=("x"*(size-1))+"\\n"
for _ in range(n):
    sys.stdout.write(line)
PY
wait $cons'
```

**Output**

- 2000 messages, 256B payload, total wall time ~3.617s
- End-to-end throughput: ~553 msg/s

### Produce Backlog, Consume from S3

**Produce backlog**

```sh
time env TOPIC=bench-s3 N=2000 SIZE=256 sh -c 'python3 - <<\"PY\" | kcat -P -b 127.0.0.1:39092 -t "$TOPIC" -q
import os,sys
n=int(os.environ["N"])
size=int(os.environ["SIZE"])
line=("x"*(size-1))+"\\n"
for _ in range(n):
    sys.stdout.write(line)
PY'
```

**Consume from beginning**

```sh
time kcat -C -b 127.0.0.1:39092 -t bench-s3 -o beginning -c 2000 -e -q
```

**Output**

- Produce backlog: ~0.486s for 2000 messages (~4115 msg/s)
- Consume from beginning: ~1.79s for 2000 messages (~1117 msg/s)

### Metrics Snapshot

Record a metrics snapshot during each run:

```sh
curl -s http://127.0.0.1:9093/metrics | rg 'kafscale_(produce|fetch)_rps|kafscale_s3_latency_ms_avg|kafscale_s3_error_rate'
```

### Cross-Partition Fan-Out (All Partitions)

**Produce backlog**

```sh
time env TOPIC=bench-hot N=4000 SIZE=256 sh -c 'python3 - <<\"PY\" | kcat -P -b 127.0.0.1:39092 -t "$TOPIC" -q
import os,sys
n=int(os.environ["N"])
size=int(os.environ["SIZE"])
line=("x"*(size-1))+"\\n"
for _ in range(n):
    sys.stdout.write(line)
PY'
```

**Consume from beginning**

```sh
time kcat -C -b 127.0.0.1:39092 -t bench-hot -o beginning -c 4000 -e -q
```

**Output**

- Produce backlog: ~0.533s for 4000 messages (~7500 msg/s)
- Consume from beginning: ~2.455s for 4000 messages (~1629 msg/s)

### S3 Cold Read (After Broker Restart)

**Produce backlog**

```sh
time env TOPIC=bench-s3 N=2000 SIZE=256 sh -c 'python3 - <<\"PY\" | kcat -P -b 127.0.0.1:39092 -t "$TOPIC" -q
import os,sys
n=int(os.environ["N"])
size=int(os.environ["SIZE"])
line=("x"*(size-1))+"\\n"
for _ in range(n):
    sys.stdout.write(line)
PY'
```

**Restart brokers**

```sh
kubectl -n kafscale-demo rollout restart statefulset/kafscale-broker
kubectl -n kafscale-demo rollout status statefulset/kafscale-broker --timeout=120s
```

**Consume from beginning**

```sh
time kcat -C -b 127.0.0.1:39092 -t bench-s3 -o beginning -c 2000 -e -q
```

**Output**

- Produce backlog: ~0.929s for 2000 messages (~2152 msg/s)
- Cold consume: ~1.441s for 2000 messages (~1388 msg/s)

### Large Records (Attempted)

Open

### Multi-Topic Mixed Load (Attempted)

Open
