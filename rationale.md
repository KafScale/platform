---
layout: doc
title: "KafScale Rationale: Why Stateless Brokers on S3"
description: Why KafScale uses stateless brokers and object storage instead of traditional Kafka clusters.
permalink: /rationale/
---

# Rationale

KafScale exists because the original assumptions behind Kafka brokers no longer hold for a large class of modern workloads. This page explains those assumptions, what changed, and why KafScale is designed the way it is.

---

## The original Kafka assumptions

Kafka was designed in a world where durability lived on local disks attached to long-running servers. Brokers owned their data. Replication, leader election, and partition rebalancing were necessary because broker state was the source of truth.

That model worked well when:

- Disks were the primary durable medium
- Brokers were expected to be long-lived
- Scaling events were rare and manual
- Recovery time could be measured in minutes or hours

Many Kafka deployments today still operate under these assumptions, even when the workload does not require them.

---

## Object storage changes the durability model

Object storage fundamentally changes where durability lives.

Modern object stores provide extremely high durability, elastic capacity, and simple lifecycle management. Once log segments are durable and immutable in object storage, keeping the same data replicated across broker-local disks stops adding resilience and starts adding operational cost.

With object storage:

- Data durability is decoupled from broker lifecycle
- Storage scales independently from compute
- Recovery no longer depends on copying large volumes of data between brokers

This enables a different design space where brokers no longer need to be stateful.

Storing Kafka data in S3 is not new. Multiple systems do this. What matters is what you do with that foundation.

---

## What KafScale actually changes

S3-native storage is table stakes. The real question is: how much operational complexity remains?

The same architectural shift already transformed data warehouses. Separating compute from storage did not just reduce costs. It simplified operations, enabled independent scaling, and changed what was possible. Streaming is following the same path.

KafScale removes four categories of coupling that other systems preserve:

<style>
  .decouple-stack {
    display: flex;
    flex-direction: column;
    gap: 0;
    margin: 2rem auto;
    max-width: 600px;
    border-radius: 12px;
    overflow: hidden;
    border: 2px solid var(--diagram-stroke);
  }
  .decouple-layer {
    display: flex;
    align-items: center;
    padding: 1.25rem 1.5rem;
    background: var(--diagram-fill);
    border-bottom: 1px solid var(--diagram-stroke);
  }
  .decouple-layer:last-child {
    border-bottom: none;
  }
  .decouple-layer-num {
    font-size: 1.25rem;
    font-weight: 700;
    color: #fff;
    width: 36px;
    height: 36px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-right: 1.25rem;
    flex-shrink: 0;
  }
  .decouple-layer-content h4 {
    margin: 0;
    font-size: 1rem;
    color: var(--diagram-text);
  }
  .decouple-layer-content p {
    margin: 0.25rem 0 0 0;
    font-size: 0.875rem;
    color: var(--diagram-label);
  }
  .decouple-1 .decouple-layer-num { background: #ffb347; }
  .decouple-2 .decouple-layer-num { background: #6aa7ff; }
  .decouple-3 .decouple-layer-num { background: #34d399; }
  .decouple-4 .decouple-layer-num { background: #a78bfa; }
  @media (max-width: 600px) {
    .decouple-layer {
      flex-direction: column;
      text-align: center;
    }
    .decouple-layer-num {
      margin-right: 0;
      margin-bottom: 0.75rem;
    }
  }
</style>

<div class="decouple-stack">
  <div class="decouple-layer decouple-1">
    <span class="decouple-layer-num">1</span>
    <div class="decouple-layer-content">
      <h4>Clients from topology</h4>
      <p>Proxy rewrites metadata. One endpoint, infinite brokers behind it. Clients never see scaling events.</p>
    </div>
  </div>
  <div class="decouple-layer decouple-2">
    <span class="decouple-layer-num">2</span>
    <div class="decouple-layer-content">
      <h4>Compute from storage</h4>
      <p>Brokers hold no durable state. S3 is the source of truth. Add or remove pods without moving data.</p>
    </div>
  </div>
  <div class="decouple-layer decouple-3">
    <span class="decouple-layer-num">3</span>
    <div class="decouple-layer-content">
      <h4>Streaming from analytics</h4>
      <p>Processors read S3 directly. Batch replay and AI workloads never compete with real-time consumers.</p>
    </div>
  </div>
  <div class="decouple-layer decouple-4">
    <span class="decouple-layer-num">4</span>
    <div class="decouple-layer-content">
      <h4>Format from implementation</h4>
      <p>The .kfs segment format is documented and open. Build processors without vendor dependency.</p>
    </div>
  </div>
</div>

Each layer removes a category of operational problems. Together, they enable minimal ops and unlimited scale.

---

## Why the Kafka protocol leaks topology

Traditional Kafka clients do not just connect to a cluster. They discover it.

When a client connects, it sends a Metadata request. The broker responds with a list of all brokers in the cluster and which broker leads each partition. The client then opens direct connections to each broker it needs.

This design made sense when brokers were stable, long-lived servers. It becomes a liability when brokers are ephemeral pods.

Every broker restart can break client connections. Every scaling event requires clients to rediscover the cluster. DNS and load balancers cannot fully abstract the topology because the protocol itself exposes it.

KafScale's proxy solves this by intercepting Metadata and FindCoordinator responses, substituting its own address. Clients believe they are talking to a single broker. The proxy routes requests to the actual brokers internally.

The result:

- Add brokers without client awareness
- Rotate brokers during deployments without connection drops
- Use standard Kubernetes networking patterns
- One ingress, one DNS name, standard TLS termination

One endpoint. Infinite scale behind it.

---

## Why processors should bypass brokers

Traditional Kafka architectures force all reads through brokers. Streaming consumers and batch analytics compete for the same resources. Backfills spike broker CPU. Training jobs block production consumers.

KafScale separates these concerns by design.

Brokers handle the Kafka protocol: accepting writes from producers, serving reads to streaming consumers, managing consumer groups. Processors read completed segments directly from S3, bypassing brokers entirely.

This separation has practical consequences:

- Historical replays do not affect streaming latency
- AI workloads get complete context without degrading production
- Iceberg materialization scales independently from Kafka consumers
- Processor development does not require broker changes

The architecture enables use cases that broker-mediated systems cannot serve efficiently.

---

## Why AI agents need this architecture

AI agents making decisions need context. That context comes from historical events: what happened, in what order, and why the current state exists.

Traditional stream processing optimizes for latency. Milliseconds matter for fraud detection or trading systems. But AI agents reasoning over business context have different requirements. They need completeness. They need replay capability. They need to reconcile current state with historical actions.

Storage-native streaming makes this practical. The immutable log in S3 becomes the source of truth that agents query, replay, and reason over. Processors convert that log to tables that analytical tools understand. Agents get complete historical context without competing with streaming workloads for broker resources.

Two-second latency for analytical access is acceptable when the alternative is incomplete context or degraded streaming performance. AI agents do not need sub-millisecond reads. They need the full picture.

---

## What KafScale deliberately does not do

KafScale is not trying to replace every Kafka deployment.

It deliberately does not target:

- Sub-10ms end-to-end latency workloads
- Exactly-once transactions across topics
- Compacted topics
- Embedded stream processing inside the broker

Those features increase broker statefulness and operational complexity. For many workloads, they are unnecessary.

KafScale focuses on the common case: durable message transport, replayability, predictable retention, and low operational overhead.

---

## Summary

Storing Kafka data in S3 is not the innovation. What matters is how much complexity remains after you do it.

KafScale removes the topology coupling that breaks clients during scaling. It removes the compute/storage coupling that makes recovery slow. It removes the streaming/analytics coupling that forces workloads to compete. It removes the format/vendor coupling that creates dependency.

The result is minimal ops and unlimited scale. That is the point.

---

## Further reading

- [Architecture](/architecture/) for detailed component diagrams and data flows
- [Comparison](/comparison/) for how KafScale compares to alternatives