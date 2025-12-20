---
layout: doc
title: Architecture
description: Architecture and data flows for KafScale brokers, metadata, and S3 segment storage.
sidebar: false
---

# Architecture

KafScale brokers are stateless pods on Kubernetes. Metadata lives in etcd, while immutable log segments live in S3. Clients speak the Kafka protocol to brokers; brokers flush segments to S3 and serve reads with caching.

## Platform overview

<div class="diagram">
  <div class="diagram-label">Architecture overview</div>
  <svg viewBox="0 0 800 400" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale architecture overview">
    <defs>
      <marker id="ah" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="var(--diagram-stroke)"/></marker>
      <marker id="ao" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#ffb347"/></marker>
      <marker id="ag" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#4ab7f1"/></marker>
    </defs>

    <!-- Clients -->
    <rect x="310" y="15" width="180" height="55" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="400" y="38" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Kafka Clients</text>
    <text x="400" y="55" font-size="10" fill="var(--diagram-label)" text-anchor="middle">producers &amp; consumers</text>

    <!-- K8s boundary -->
    <rect x="80" y="95" width="480" height="240" rx="14" fill="var(--diagram-accent)" stroke="#326ce5" stroke-width="2" stroke-dasharray="8,4"/>
    <text x="105" y="120" font-size="11" font-weight="600" fill="var(--diagram-label)" letter-spacing="0.5px">KUBERNETES CLUSTER</text>

    <!-- Brokers -->
    <rect x="110" y="140" width="110" height="65" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="165" y="168" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Broker 0</text>
    <text x="165" y="185" font-size="9" fill="var(--diagram-label)" text-anchor="middle">stateless · Go</text>

    <rect x="240" y="140" width="110" height="65" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="295" y="168" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Broker 1</text>
    <text x="295" y="185" font-size="9" fill="var(--diagram-label)" text-anchor="middle">stateless · Go</text>

    <rect x="370" y="140" width="110" height="65" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="425" y="168" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Broker 2</text>
    <text x="425" y="185" font-size="9" fill="var(--diagram-label)" text-anchor="middle">stateless · Go</text>

    <text x="500" y="165" font-size="9" fill="#6aa7ff">← HPA</text>

    <!-- etcd -->
    <rect x="180" y="250" width="220" height="65" rx="10" fill="rgba(74, 183, 241, 0.15)" stroke="#4ab7f1" stroke-width="1.5"/>
    <text x="290" y="278" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">etcd (3 nodes)</text>
    <text x="290" y="295" font-size="9" fill="var(--diagram-label)" text-anchor="middle">topics · offsets · assignments</text>

    <!-- S3 Data -->
    <rect x="620" y="120" width="160" height="100" rx="12" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
    <circle cx="700" cy="160" r="28" fill="rgba(255, 179, 71, 0.3)" stroke="#ffb347" stroke-width="1.5"/>
    <text x="700" y="165" font-size="13" font-weight="700" fill="#ffb347" text-anchor="middle">S3</text>
    <text x="700" y="200" font-size="10" font-weight="600" fill="#ffb347" text-anchor="middle">Data Bucket</text>

    <!-- S3 Backup -->
    <rect x="620" y="250" width="160" height="65" rx="12" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
    <text x="700" y="278" font-size="10" font-weight="600" fill="#ffb347" text-anchor="middle">S3 Backup</text>
    <text x="700" y="295" font-size="9" fill="var(--diagram-label)" text-anchor="middle">etcd snapshots</text>

    <!-- Arrows -->
    <path d="M400,70 L400,135" stroke="var(--diagram-stroke)" stroke-width="2" fill="none" marker-end="url(#ah)"/>
    <text x="415" y="105" font-size="9" fill="var(--diagram-label)">:9092</text>

    <path d="M480,172 L615,165" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#ao)"/>
    <text x="525" y="155" font-size="9" fill="#ffb347">flush segments</text>

    <path d="M615,185 L480,185" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ah)"/>
    <text x="525" y="200" font-size="9" fill="#6aa7ff">fetch + cache</text>

    <path d="M290,205 L290,245" stroke="#4ab7f1" stroke-width="1.5" fill="none" marker-end="url(#ag)"/>
    <text x="305" y="230" font-size="9" fill="var(--diagram-label)">metadata</text>

    <path d="M400,282 L615,282" stroke="#ffb347" stroke-width="1.5" fill="none" marker-end="url(#ao)"/>
    <text x="490" y="272" font-size="9" fill="var(--diagram-label)">snapshots</text>

    <!-- Tagline -->
    <text x="400" y="375" font-size="10" fill="var(--diagram-label)" text-anchor="middle" font-style="italic">
      S3 is the source of truth · Brokers are stateless · etcd for coordination
    </text>
  </svg>
</div>

## Produce flow

<div class="diagram">
  <div class="diagram-label">Write path</div>
  <svg viewBox="0 0 750 130" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale produce flow">
    <defs>
      <marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#6aa7ff"/></marker>
      <marker id="ao2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#ffb347"/></marker>
    </defs>

    <!-- Producer -->
    <rect x="20" y="30" width="120" height="70" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="80" y="58" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Producer</text>
    <text x="80" y="75" font-size="9" fill="var(--diagram-label)" text-anchor="middle">Kafka client</text>

    <!-- Broker -->
    <rect x="200" y="30" width="140" height="70" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="270" y="55" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Broker</text>
    <text x="270" y="72" font-size="9" fill="var(--diagram-label)" text-anchor="middle">validate · batch</text>
    <text x="270" y="86" font-size="9" fill="var(--diagram-label)" text-anchor="middle">assign offsets</text>

    <!-- Buffer -->
    <rect x="400" y="30" width="130" height="70" rx="10" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1.5"/>
    <text x="465" y="58" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Buffer</text>
    <text x="465" y="75" font-size="9" fill="var(--diagram-label)" text-anchor="middle">in-memory batches</text>

    <!-- S3 -->
    <rect x="590" y="30" width="130" height="70" rx="12" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
    <circle cx="655" cy="58" r="18" fill="rgba(255, 179, 71, 0.3)" stroke="#ffb347" stroke-width="1"/>
    <text x="655" y="63" font-size="11" font-weight="700" fill="#ffb347" text-anchor="middle">S3</text>
    <text x="655" y="85" font-size="9" fill="var(--diagram-label)" text-anchor="middle">sealed segment</text>

    <!-- Arrows -->
    <path d="M140,65 L195,65" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ab)"/>
    <text x="165" y="55" font-size="10" font-weight="600" fill="#6aa7ff">1</text>

    <path d="M340,65 L395,65" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ab)"/>
    <text x="365" y="55" font-size="10" font-weight="600" fill="#6aa7ff">2</text>

    <path d="M530,65 L585,65" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#ao2)"/>
    <text x="555" y="55" font-size="10" font-weight="600" fill="#ffb347">3</text>

    <!-- Labels -->
    <text x="167" y="118" font-size="9" fill="var(--diagram-label)" text-anchor="middle">produce</text>
    <text x="367" y="118" font-size="9" fill="var(--diagram-label)" text-anchor="middle">batch</text>
    <text x="557" y="118" font-size="9" fill="var(--diagram-label)" text-anchor="middle">flush</text>
  </svg>
</div>

Producers send records to brokers. Brokers validate, batch, and assign offsets. When the buffer reaches **4MB** or **500ms**, it's sealed and flushed to S3 as an immutable segment.

## Fetch flow

<div class="diagram">
  <div class="diagram-label">Read path</div>
  <svg viewBox="0 0 750 160" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale fetch flow">
    <defs>
      <marker id="ab3" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#6aa7ff"/></marker>
      <marker id="ao3" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#ffb347"/></marker>
    </defs>

    <!-- Consumer -->
    <rect x="20" y="35" width="120" height="70" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="80" y="63" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Consumer</text>
    <text x="80" y="80" font-size="9" fill="var(--diagram-label)" text-anchor="middle">Kafka client</text>

    <!-- Broker -->
    <rect x="200" y="35" width="140" height="70" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="270" y="60" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Broker</text>
    <text x="270" y="77" font-size="9" fill="var(--diagram-label)" text-anchor="middle">locate segment</text>
    <text x="270" y="91" font-size="9" fill="var(--diagram-label)" text-anchor="middle">check cache</text>

    <!-- Cache -->
    <rect x="400" y="35" width="130" height="70" rx="10" fill="rgba(56, 189, 248, 0.15)" stroke="#38bdf8" stroke-width="1.5"/>
    <text x="465" y="63" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">LRU Cache</text>
    <text x="465" y="80" font-size="9" fill="var(--diagram-label)" text-anchor="middle">hit → fast path</text>

    <!-- S3 -->
    <rect x="590" y="35" width="130" height="70" rx="12" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
    <circle cx="655" cy="63" r="18" fill="rgba(255, 179, 71, 0.3)" stroke="#ffb347" stroke-width="1"/>
    <text x="655" y="68" font-size="11" font-weight="700" fill="#ffb347" text-anchor="middle">S3</text>
    <text x="655" y="90" font-size="9" fill="var(--diagram-label)" text-anchor="middle">miss → fetch</text>

    <!-- Forward arrows -->
    <path d="M140,70 L195,70" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ab3)"/>
    <text x="165" y="60" font-size="10" font-weight="600" fill="#6aa7ff">1</text>

    <path d="M340,70 L395,70" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ab3)"/>
    <text x="365" y="60" font-size="10" font-weight="600" fill="#6aa7ff">2</text>

    <path d="M530,70 L585,70" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#ao3)"/>
    <text x="555" y="60" font-size="10" font-weight="600" fill="#ffb347">3</text>

    <!-- Return arrows -->
    <path d="M585,90 Q465,145 340,90" stroke="#ffb347" stroke-width="1.5" stroke-dasharray="4,2" fill="none" marker-end="url(#ao3)"/>
    <text x="465" y="140" font-size="10" font-weight="600" fill="#ffb347">4</text>

    <path d="M195,90 Q130,130 80,105" stroke="#6aa7ff" stroke-width="1.5" stroke-dasharray="4,2" fill="none" marker-end="url(#ab3)"/>
    <text x="125" y="135" font-size="10" font-weight="600" fill="#6aa7ff">5</text>

    <!-- Labels -->
    <text x="167" y="25" font-size="9" fill="var(--diagram-label)" text-anchor="middle">fetch</text>
    <text x="367" y="25" font-size="9" fill="var(--diagram-label)" text-anchor="middle">cache?</text>
    <text x="557" y="25" font-size="9" fill="var(--diagram-label)" text-anchor="middle">miss</text>
  </svg>
</div>

Consumers request data from brokers. Brokers resolve the segment offset, check the LRU cache, and fetch from S3 on cache miss. Read-ahead prefetches likely-needed segments.

## Segment format

<div class="format-card">
  <div class="format-row header">
    <span>Field</span>
    <span>Size</span>
    <span>Description</span>
  </div>
  <div class="format-row">
    <span>Magic Number</span>
    <span>4 bytes</span>
    <span><code>0x4B414653</code> ("KAFS")</span>
  </div>
  <div class="format-row">
    <span>Version</span>
    <span>2 bytes</span>
    <span>Format version (currently 1)</span>
  </div>
  <div class="format-row">
    <span>Flags</span>
    <span>2 bytes</span>
    <span>Compression codec, etc.</span>
  </div>
  <div class="format-row">
    <span>Base Offset</span>
    <span>8 bytes</span>
    <span>First offset in segment</span>
  </div>
  <div class="format-row">
    <span>Message Count</span>
    <span>4 bytes</span>
    <span>Number of messages</span>
  </div>
  <div class="format-row">
    <span>Created Timestamp</span>
    <span>8 bytes</span>
    <span>Unix milliseconds</span>
  </div>
  <div class="format-row">
    <span>Message Batches</span>
    <span>variable</span>
    <span>Kafka-compatible RecordBatch format</span>
  </div>
  <div class="format-row">
    <span>CRC32</span>
    <span>4 bytes</span>
    <span>Checksum of all batches</span>
  </div>
  <div class="format-row">
    <span>Footer Magic</span>
    <span>4 bytes</span>
    <span><code>0x454E4421</code> ("END!")</span>
  </div>
</div>

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| **S3 as source of truth** | 11 9's durability, infinite capacity, $0.023/GB |
| **Stateless brokers** | Any pod serves any partition; HPA scales 0→N |
| **etcd for metadata** | Leverages existing K8s etcd or dedicated cluster |
| **~500ms latency** | Acceptable trade-off for ETL, logs, async events |
| **No transactions** | Simplifies architecture for 80% use case |
| **4MB segments** | Balances S3 PUT costs vs. flush latency |
