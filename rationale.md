---
layout: doc
title: Rationale
description: Why KafScale uses stateless brokers and object storage instead of traditional Kafka clusters.
permalink: /rationale/

---

# Rationale

KafScale exists because the original assumptions behind Kafka brokers no longer hold for a large class of modern workloads. This page explains those assumptions, what changed, and why KafScale is designed the way it is.

This is not a comparison page and not a feature list. It documents the architectural reasoning behind the system.

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

<div class="diagram">
  <div class="diagram-label">What changed</div>
  <svg viewBox="0 0 750 280" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Traditional vs stateless broker model comparison">
    <defs>
      <marker id="ar-rat" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="var(--diagram-stroke)"/></marker>
    </defs>

    <!-- Traditional model -->
    <rect x="20" y="20" width="340" height="200" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="190" y="48" font-size="13" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Traditional Kafka</text>
    
    <text x="40" y="80" font-size="11" fill="var(--diagram-text)">• Brokers own durable data</text>
    <text x="40" y="102" font-size="11" fill="var(--diagram-text)">• Replication required for durability</text>
    <text x="40" y="124" font-size="11" fill="var(--diagram-text)">• Scaling moves data</text>
    <text x="40" y="146" font-size="11" fill="var(--diagram-text)">• Failures require repair</text>
    <text x="40" y="168" font-size="11" fill="var(--diagram-text)">• Disk management is critical</text>
    
    <rect x="40" y="185" width="300" height="24" rx="6" fill="rgba(248, 113, 113, 0.15)" stroke="#f87171" stroke-width="1"/>
    <text x="190" y="202" font-size="10" fill="#f87171" text-anchor="middle">Stateful brokers = operational complexity</text>

    <!-- Arrow -->
    <path d="M375,120 L405,120" stroke="var(--diagram-stroke)" stroke-width="2" fill="none" marker-end="url(#ar-rat)"/>

    <!-- Stateless model -->
    <rect x="420" y="20" width="310" height="200" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="575" y="48" font-size="13" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Stateless Brokers (KafScale)</text>
    
    <text x="440" y="80" font-size="11" fill="var(--diagram-text)">• Object storage owns durability</text>
    <text x="440" y="102" font-size="11" fill="var(--diagram-text)">• Replication is implicit in S3</text>
    <text x="440" y="124" font-size="11" fill="var(--diagram-text)">• Scaling adds compute only</text>
    <text x="440" y="146" font-size="11" fill="var(--diagram-text)">• Failures handled by replacement</text>
    <text x="440" y="168" font-size="11" fill="var(--diagram-text)">• Disk management disappears</text>
    
    <rect x="440" y="185" width="270" height="24" rx="6" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1"/>
    <text x="575" y="202" font-size="10" fill="#34d399" text-anchor="middle">Stateless brokers = simpler operations</text>

    <!-- Bottom caption -->
    <text x="375" y="260" font-size="11" fill="var(--diagram-label)" text-anchor="middle" font-style="italic">
      When durability moves out of the broker, the operational model changes with it.
    </text>
  </svg>
</div>

---

## Why brokers should be ephemeral

In KafScale, brokers are treated as ephemeral compute.

They serve the Kafka protocol, buffer and batch data, and flush immutable segments to object storage. They do not own durable state. Any broker can serve any partition.

This has several consequences:

- Scaling is a scheduling problem, not a data movement problem
- Broker restarts are cheap and predictable
- Failures are handled by replacement, not repair
- Kubernetes can manage brokers like any other stateless workload

This model matches how modern infrastructure platforms already operate.

<div class="diagram">
  <div class="diagram-label">Design flow</div>
  <svg viewBox="0 0 700 100" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale design flow">
    <defs>
      <marker id="af-rat" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#ffb347"/></marker>
    </defs>

    <!-- Durable storage -->
    <rect x="30" y="25" width="180" height="50" rx="10" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="1.5"/>
    <text x="120" y="55" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Durable storage (S3)</text>

    <path d="M215,50 L265,50" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#af-rat)"/>

    <!-- Ephemeral compute -->
    <rect x="270" y="25" width="180" height="50" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="360" y="55" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Ephemeral compute</text>

    <path d="M455,50 L505,50" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#af-rat)"/>

    <!-- Simpler operations -->
    <rect x="510" y="25" width="180" height="50" rx="10" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1.5"/>
    <text x="600" y="55" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Simpler operations</text>
  </svg>
</div>

---

## Why the storage format is open

Most streaming platforms treat their storage format as an internal implementation detail. KafScale takes a different approach: the .kfs segment format is documented as part of the public specification.

This is a deliberate architectural choice with specific consequences:

- Any processor that understands the format can read directly from S3 without connecting to a broker
- The streaming path and the analytical path can share the same data without competing for the same resources
- Users can build custom processors without waiting for vendors to ship features
- The format outlives any particular implementation

When the storage format is open, the platform becomes a building block rather than a boundary.

---

## Why processors should bypass brokers

Traditional Kafka architectures force all reads through brokers. Streaming consumers and batch analytics compete for the same resources. Backfills spike broker CPU. AI training jobs block production consumers.

KafScale separates these concerns by design.

Brokers handle the Kafka protocol: accepting writes from producers, serving reads to streaming consumers, managing consumer groups. Processors read completed segments directly from S3, bypassing brokers entirely.

<div class="diagram">
  <div class="diagram-label">Two read paths, one data source</div>
  <svg viewBox="0 0 700 160" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Streaming vs analytical read paths">
    <defs>
      <marker id="ar-blue" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 z" fill="#6aa7ff"/>
      </marker>
      <marker id="ar-green" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 z" fill="#34d399"/>
      </marker>
    </defs>

    <!-- S3 (center) -->
    <rect x="270" y="55" width="160" height="50" rx="10" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
    <text x="350" y="85" font-size="12" font-weight="600" fill="#ffb347" text-anchor="middle">S3 (.kfs segments)</text>

    <!-- Streaming path (top) -->
    <rect x="40" y="20" width="140" height="40" rx="8" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="110" y="45" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Brokers</text>

    <rect x="520" y="20" width="140" height="40" rx="8" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="590" y="45" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Consumers</text>

    <path d="M180,40 L265,70" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ar-blue)"/>
    <path d="M430,70 L515,40" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ar-blue)"/>
    <text x="350" y="28" font-size="10" font-weight="600" fill="#6aa7ff" text-anchor="middle">STREAMING</text>

    <!-- Analytical path (bottom) -->
    <rect x="40" y="100" width="140" height="40" rx="8" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1.5"/>
    <text x="110" y="125" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Processors</text>

    <rect x="520" y="100" width="140" height="40" rx="8" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="590" y="120" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Iceberg / Analytics</text>
    <text x="590" y="132" font-size="9" fill="var(--diagram-label)" text-anchor="middle">AI agents</text>

    <path d="M180,120 L265,95" stroke="#34d399" stroke-width="2" fill="none" marker-end="url(#ar-green)"/>
    <path d="M430,95 L515,120" stroke="#34d399" stroke-width="2" fill="none" marker-end="url(#ar-green)"/>
    <text x="350" y="148" font-size="10" font-weight="600" fill="#34d399" text-anchor="middle">ANALYTICAL</text>
  </svg>
</div>

This separation has practical consequences:

- Historical replays do not affect streaming latency
- AI agents get complete context without degrading production workloads
- Iceberg materialization scales independently from Kafka consumers
- Processor development does not require broker changes

The architecture enables use cases that broker-mediated systems cannot serve efficiently.

---

## Why AI agents need this architecture

AI agents making decisions need context. That context comes from historical events: what happened, in what order, and why the current state exists.

Traditional stream processing optimizes for latency. Milliseconds matter for fraud detection or trading systems. But AI agents reasoning over business context have different requirements. They need completeness. They need replay capability. They need to reconcile current state with historical actions.

Storage-native streaming makes this practical:

- The immutable log in S3 becomes the source of truth that agents query, replay, and reason over
- Processors convert that log to tables that analytical tools understand
- Agents get complete historical context without competing with streaming workloads for broker resources

Two-second latency for analytical access is acceptable when the alternative is incomplete context or degraded streaming performance. AI agents do not need sub-millisecond reads. They need the full picture.

---

## Why self-hosted control planes still matter

Some systems that adopt stateless brokers rely on vendor-managed control planes for metadata and coordination. That can be a good tradeoff for teams that want a fully managed service.

KafScale makes a different choice.

By keeping metadata, offsets, and consumer group state in a self-hosted store, KafScale can run entirely within your own infrastructure boundary. This matters for:

- Regulated and sovereign environments
- Private and air-gapped deployments
- Teams that require open licensing and forkability
- Platforms that want to avoid external control plane dependencies

The goal is not to reject managed services, but to make the architecture usable under stricter constraints.

---

## What KafScale deliberately does not do

KafScale is not trying to replace every Kafka deployment.

It deliberately does not target:

- Sub-10ms end-to-end latency workloads
- Exactly-once transactions
- Compacted topics
- Embedded stream processing inside the broker

Those features increase broker statefulness and operational complexity. For many workloads, they are unnecessary.

KafScale focuses on the common case: durable message transport, replayability, predictable retention, and low operational overhead.

---

## Summary

Stateless brokers backed by object storage are not a trend. They are a correction.

Once durability moves out of the broker, the system can be simpler, cheaper to operate, and easier to scale. When the storage format is open, processors can bypass brokers entirely, separating streaming and analytical workloads by design.

KafScale is built on these assumptions, while preserving Kafka protocol compatibility and self-hosted operation.

The architecture is inevitable. The design choices are deliberate.

## Further reading

- [Streaming Data Becomes Storage-Native](https://www.scalytics.io/blog/streaming-data-becomes-storage-native) explores the broader industry shift and why AI agents need this architecture.
- [Data Processing Does Not Belong in the Message Broker](https://www.novatechflow.com/2025/12/data-processing-does-not-belong-in.html) explains why KafScale keeps processing separate from the broker layer.