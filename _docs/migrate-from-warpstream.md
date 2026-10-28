---
layout: doc
title: "Migrate from WarpStream to KafScale"
description: Move topics from WarpStream to KafScale with a kaf-mirror dual run and a planned cutover. Includes what KafScale does not support.
permalink: /migrate-from-warpstream/
nav_title: Migrate from WarpStream
nav_order: 11
---

# Migrate from WarpStream

WarpStream and KafScale both keep brokers stateless and put data in object storage. The difference is who runs the metadata. WarpStream agents run in your cloud, but the control plane runs at WarpStream, which is owned by Confluent and, since March 2026, by IBM. KafScale keeps metadata in etcd inside your own Kubernetes cluster, and the whole stack is Apache 2.0.

In short:

- **License.** KafScale is open source under Apache 2.0. WarpStream is proprietary.
- **Metadata.** KafScale keeps metadata in etcd inside your own Kubernetes cluster. WarpStream uses a SaaS control plane.
- **Segment format.** KafScale uses the open, documented [.kfs segment format](/storage-format/). WarpStream's format is not public.

This guide moves topics over with [kaf-mirror](https://github.com/KafScale/kaf-mirror), runs both systems side by side, and then cuts clients over.

## Check fit first

KafScale does not support everything WarpStream does. Check these before you start:

| Feature | WarpStream | KafScale |
|---------|------------|----------|
| Transactions and exactly-once | Yes | No |
| Idempotent producers | Yes | No. Set `enable.idempotence=false` |
| Compacted topics | Yes | No |
| ACL admin API | Yes | No |
| SASL authentication | Yes | No |
| mTLS client authentication | Yes | Yes, at the KafScale proxy. See [mTLS authentication](/kafka-authentication/) |
| Produce latency | ~400-600ms p99 (WarpStream docs) | Roughly S3 PUT latency plus one etcd write per acked produce. AWS documents roughly 100–200 ms for small S3 objects. See [Latency](/latency/#derived-latency-by-operation) |
| Metadata | WarpStream control plane | etcd in your cluster |
| License | Proprietary | Apache 2.0 |

These are deliberate trade-offs of the S3 design, explained in [Trade-offs of the S3 design](/latency/#trade-offs-of-the-s3-design). If a workload needs transactions or compacted topics (for example CDC with log compaction), keep it on WarpStream or move it to Apache Kafka. Move the rest: logs, telemetry, events, ETL feeds and AI agent context are a good fit.

See the [user guide](/user-guide/) and [FAQ](/faq/) for the full list of limits.

## 1. Run KafScale

Install KafScale with the [Helm chart](/helm/) against your own S3 bucket, or try it locally with `make demo` from the [platform repo](https://github.com/KafScale/platform). The local demo broker listens on `127.0.0.1:39092`.

Create the target topics with the same names and partition counts as on WarpStream.

## 2. Start kaf-mirror

kaf-mirror consumes from one Kafka-protocol cluster and produces to another. It mirrors topics under the same name by default, and also supports exact or regex mappings. It commits source offsets only after the target acknowledges the write, so a stopped job resumes where it left off.

KafScale does not implement `InitProducerId`, so turn idempotent writes off when KafScale is the target:

```bash
git clone https://github.com/KafScale/kaf-mirror.git && cd kaf-mirror
mkdir -p bin
go build -o bin/kaf-mirror ./cmd/kaf-mirror
go build -o bin/mirror-cli ./cmd/mirror-cli
KAFMIRROR_DISABLE_IDEMPOTENT_WRITES=true ./bin/kaf-mirror
```

The admin password for `admin@localhost` is printed once to `logs/kaf-mirror.log`.

## 3. Add both clusters and start the job

```bash
./bin/mirror-cli login admin@localhost --password <password-from-log>
./bin/mirror-cli clusters add   # source: your WarpStream agents
./bin/mirror-cli clusters add   # target: the KafScale proxy or broker
./bin/mirror-cli jobs add       # pick source, target and topics
./bin/mirror-cli jobs start
./bin/mirror-cli jobs status full
```

`clusters add` and `jobs add` are interactive. Use the same bootstrap address and SASL settings for WarpStream that your clients use today.

## 4. Dual run

Leave producers on WarpStream while kaf-mirror copies every new record to KafScale. During this phase:

- Point a test consumer, or a copy of a real consumer under a new group id, at KafScale and compare output with what you see on WarpStream.
- Watch replication lag in the kaf-mirror dashboard and `jobs status full`.
- Check KafScale broker and S3 health with the [metrics](/metrics/) and [S3 health](/s3-health/) pages.

Keep the dual run going until lag stays near zero and the test consumers match.

## 5. Cut over

Offsets on KafScale will not match offsets on WarpStream, and consumer group positions are not copied. Plan the cutover around that:

1. Stop the producers on WarpStream.
2. Wait until kaf-mirror lag reaches zero, then stop the job.
3. Start consumers on KafScale. Use a new group id, and choose where to start: from the earliest offset if your consumers can handle duplicates, or from the latest offset if they can skip what they already processed on WarpStream.
4. Point producers at KafScale with `enable.idempotence=false` and no `transactional.id`.
5. Keep the WarpStream cluster for a rollback window, then shut it down.

For Bento pipelines that read or write WarpStream, see [Bento with KafScale](/bento/).

## Rollback

Until you shut down WarpStream, rollback is a config change: point producers and consumers back at the WarpStream bootstrap address. Records written only to KafScale after cutover can be copied back with a second kaf-mirror job in the other direction.
