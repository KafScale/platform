---
layout: doc
title: "Bento with KafScale"
description: Use Bento, the MIT-licensed stream processor, to read from and write to KafScale with the kafka_franz input and output.
permalink: /bento/
nav_title: Bento
nav_order: 12
---

# Bento with KafScale

[Bento](https://github.com/warpstreamlabs/bento) is an MIT-licensed stream processor maintained by WarpStream. It speaks the Kafka protocol through its `kafka_franz` input and output, so it works with KafScale without plugins.

WarpStream also embeds Bento in its agents as Managed Data Pipelines, but that mode only works with WarpStream BYOC. With KafScale you run Bento as its own process or container next to the cluster, and it works the same way.

## The one setting that matters

The Bento `kafka_franz` output has `idempotent_write: true` by default. KafScale does not support idempotent producers, so set it to `false`. Without it, writes fail.

## Write to KafScale

This config reads lines from stdin and writes them to the `orders` topic:

```yaml
input:
  stdin: {}

output:
  kafka_franz:
    seed_brokers: [ "127.0.0.1:39092" ]
    topic: orders
    idempotent_write: false
```

## Read from KafScale

```yaml
input:
  kafka_franz:
    seed_brokers: [ "127.0.0.1:39092" ]
    topics: [ orders ]
    consumer_group: bento-orders
    start_from_oldest: true

output:
  stdout: {}
```

## Transform between topics

Read `orders`, keep only paid orders, and write them to `orders-paid`:

```yaml
input:
  kafka_franz:
    seed_brokers: [ "127.0.0.1:39092" ]
    topics: [ orders ]
    consumer_group: bento-paid

pipeline:
  processors:
    - mapping: |
        root = if this.status != "paid" { deleted() }

output:
  kafka_franz:
    seed_brokers: [ "127.0.0.1:39092" ]
    topic: orders-paid
    idempotent_write: false
```

Run it with `bento -c config.yaml`.

## Notes

- `127.0.0.1:39092` is the broker from `make demo` in the [platform repo](https://github.com/KafScale/platform). In Kubernetes, use the KafScale proxy address.
- Add `tls` and `sasl` blocks the same way you would for any Kafka cluster. See the Bento [kafka_franz output](https://warpstreamlabs.github.io/bento/docs/components/outputs/kafka_franz) and [kafka_franz input](https://warpstreamlabs.github.io/bento/docs/components/inputs/kafka_franz) docs for every field.
- Moving pipelines off WarpStream? Replace any `kafka_franz_warpstream` block with a plain `kafka_franz` block that points at KafScale. See [Migrate from WarpStream](/migrate-from-warpstream/).
