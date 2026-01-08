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

This document captures repeatable benchmark scenarios for Kafscale. It focuses
on end-to-end throughput and latency across the broker and S3-backed storage.

The local runner lives in `docs/benchmarks/scripts/run_local.sh` and can be
used to reproduce the baseline numbers on a demo platform setup. A tiny
Makefile wrapper is available at `docs/benchmarks/Makefile`.

The runner uses dedicated topics (`bench-hot`, `bench-s3`, `bench-multi`) and
will create them if `kafka-topics.sh` is available. Otherwise, it falls back to
auto-creation behavior in the demo stack.

## Goals

- Quantify produce/consume throughput on the broker path.
- Quantify consume performance when reading from S3-backed segments.
- Capture tail latency and error rates under steady load.

## General Guidance

- **Stop the demo workload** before benchmarking to avoid noise.
- Keep topic/partition counts fixed for each run.
- Record system resources (CPU, memory, disk, network) alongside metrics.
- Use consistent message size and producer acks across runs.

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

- **Backlog catch-up**: produce a large backlog, then measure how quickly a new
  consumer group catches up to head.
- **Partition scale**: repeat scenarios with 1, 3, 6, 12 partitions.
- **Message size sweep**: 1 KB, 10 KB, 100 KB to characterize bandwidth.
- **Multi-consumer fan-out**: multiple consumer groups reading the same topic.
- **Offset recovery**: restart consumer and measure resume time.

## Metrics to Capture

- Broker metrics (`/metrics`): `kafscale_produce_rps`, `kafscale_fetch_rps`,
  `kafscale_s3_latency_ms_avg`, `kafscale_s3_error_rate`.
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

Local demo setup on kind (demo workload stopped). Commands and results below
document the baseline numbers we observed on a laptop.

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
