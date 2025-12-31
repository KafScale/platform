---
layout: doc
title: Architecture
description: Architecture and data flows for KafScale brokers, metadata, and S3 segment storage.
permalink: /architecture/
---

# Architecture

KafScale brokers are stateless pods on Kubernetes. Metadata lives in etcd, while immutable log segments live in S3. Clients speak the Kafka protocol to brokers; brokers flush segments to S3 and serve reads with caching.

---

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
    <text x="425" y="168" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Broker N</text>
    <text x="425" y="185" font-size="9" fill="var(--diagram-label)" text-anchor="middle">stateless · Go</text>

    <text x="500" y="165" font-size="9" fill="#6aa7ff">← HPA</text>

    <!-- etcd -->
    <rect x="180" y="250" width="220" height="65" rx="10" fill="rgba(74, 183, 241, 0.15)" stroke="#4ab7f1" stroke-width="1.5"/>
    <text x="290" y="278" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">etcd (3 nodes)</text>
    <text x="290" y="295" font-size="9" fill="var(--diagram-label)" text-anchor="middle">topics · offsets · group assignments</text>

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

    <path d="M480,165 L615,158" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#ao)"/>
    <text x="525" y="148" font-size="9" fill="#ffb347">flush segments</text>

    <path d="M615,178 L480,185" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ah)"/>
    <text x="525" y="198" font-size="9" fill="#6aa7ff">fetch + cache</text>

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

## Decoupled processing (addons)

KafScale keeps brokers focused on Kafka protocol and storage. Add-on processors handle downstream tasks by reading completed segments directly from S3, bypassing brokers entirely. Processors are stateless: offsets and leases live in etcd, input lives in S3, output goes to external catalogs.

<div class="diagram">
  <div class="diagram-label">Processor addon architecture</div>
  <svg viewBox="0 0 800 320" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale processor addon architecture">
    <defs>
      <marker id="ap1" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 z" fill="var(--diagram-stroke)"/>
      </marker>
      <marker id="ap2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 z" fill="#ffb347"/>
      </marker>
      <marker id="ap3" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 z" fill="#34d399"/>
      </marker>
      <marker id="ap4" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 z" fill="#4ab7f1"/>
      </marker>
      <marker id="ap5" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 z" fill="#a78bfa"/>
      </marker>
    </defs>

    <!-- Row 1: KafScale layer (source) -->
    <text x="30" y="28" font-size="10" font-weight="600" fill="var(--diagram-label)" letter-spacing="0.5px">KAFSCALE</text>
    
    <rect x="30" y="40" width="130" height="60" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="95" y="65" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Brokers</text>
    <text x="95" y="82" font-size="9" fill="var(--diagram-label)" text-anchor="middle">Kafka protocol</text>

    <rect x="190" y="40" width="130" height="60" rx="10" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
    <text x="255" y="65" font-size="11" font-weight="700" fill="#ffb347" text-anchor="middle">S3</text>
    <text x="255" y="82" font-size="9" fill="var(--diagram-label)" text-anchor="middle">.kfs segments</text>

    <rect x="350" y="40" width="130" height="60" rx="10" fill="rgba(74, 183, 241, 0.15)" stroke="#4ab7f1" stroke-width="1.5"/>
    <text x="415" y="65" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">etcd</text>
    <text x="415" y="82" font-size="9" fill="var(--diagram-label)" text-anchor="middle">metadata · offsets</text>

    <!-- Broker to S3 arrow -->
    <path d="M160,70 L185,70" stroke="#ffb347" stroke-width="1.5" fill="none" marker-end="url(#ap2)"/>

    <!-- Row 2: Processor layer -->
    <text x="30" y="138" font-size="10" font-weight="600" fill="var(--diagram-label)" letter-spacing="0.5px">PROCESSOR</text>

    <rect x="130" y="150" width="260" height="70" rx="12" fill="rgba(52, 211, 153, 0.12)" stroke="#34d399" stroke-width="2"/>
    <text x="260" y="175" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Iceberg Processor</text>
    <text x="260" y="195" font-size="9" fill="var(--diagram-label)" text-anchor="middle">stateless pods · topic-scoped leases · HPA</text>

    <!-- S3 to Processor arrow -->
    <path d="M255,100 L255,145" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#ap2)"/>
    <text x="268" y="125" font-size="9" fill="#ffb347">read segments</text>

    <!-- etcd to Processor arrow -->
    <path d="M415,100 L415,130 L360,130 L360,145" stroke="#4ab7f1" stroke-width="1.5" fill="none" marker-end="url(#ap4)"/>
    <text x="420" y="125" font-size="9" fill="#4ab7f1">offsets · leases</text>

    <!-- Row 3: Output layer -->
    <text x="30" y="258" font-size="10" font-weight="600" fill="var(--diagram-label)" letter-spacing="0.5px">OUTPUT</text>

    <rect x="80" y="270" width="150" height="40" rx="10" fill="rgba(167, 139, 250, 0.15)" stroke="#a78bfa" stroke-width="1.5"/>
    <text x="155" y="295" font-size="10" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Iceberg REST Catalog</text>

    <rect x="260" y="270" width="130" height="40" rx="10" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="1.5"/>
    <text x="325" y="295" font-size="10" font-weight="600" fill="#ffb347" text-anchor="middle">S3 Warehouse</text>

    <!-- Processor to Catalog arrow -->
    <path d="M200,220 L155,265" stroke="#a78bfa" stroke-width="2" fill="none" marker-end="url(#ap5)"/>
    <text x="145" y="245" font-size="9" fill="#a78bfa">metadata</text>

    <!-- Processor to Warehouse arrow -->
    <path d="M310,220 L325,265" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#ap2)"/>
    <text x="340" y="245" font-size="9" fill="#ffb347">parquet</text>

    <!-- Consumers on right side -->
    <text x="520" y="258" font-size="10" font-weight="600" fill="var(--diagram-label)" letter-spacing="0.5px">CONSUMERS</text>

    <rect x="520" y="270" width="130" height="40" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="585" y="290" font-size="9" fill="var(--diagram-text)" text-anchor="middle">Unity Catalog</text>
    <text x="585" y="302" font-size="9" fill="var(--diagram-label)" text-anchor="middle">Spark · Trino · etc</text>

    <!-- Catalog/Warehouse to Consumers -->
    <path d="M390,290 L515,290" stroke="var(--diagram-stroke)" stroke-width="1.5" stroke-dasharray="4,2" fill="none" marker-end="url(#ap1)"/>
    <text x="452" y="282" font-size="9" fill="var(--diagram-label)">query</text>

    <!-- Caption -->
    <text x="400" y="30" font-size="10" fill="var(--diagram-label)" text-anchor="middle" font-style="italic">
      Processors bypass brokers entirely · State lives in etcd · Output to any Iceberg catalog
    </text>
  </svg>
</div>

The processor reads .kfs segments from S3, tracks progress in etcd, and writes Parquet files to an Iceberg warehouse. Any Iceberg-compatible catalog (Unity Catalog, Polaris, AWS Glue, etc.) can serve the tables to downstream consumers.

For deployment and configuration, see the [Iceberg Processor](/processors/iceberg/) docs.

---

## Key design decisions

| Decision | Rationale |
|----------|-----------|
| **S3 as source of truth** | 11 nines durability, unlimited capacity, ~$0.023/GB/month |
| **Stateless brokers** | Any pod serves any partition; HPA scales 0→N instantly |
| **etcd for metadata** | Leverages existing K8s etcd or dedicated cluster; strong consistency |
| **~500ms latency** | Acceptable trade-off for ETL, logs, async events |
| **No transactions** | Simplifies architecture; covers 80% of Kafka use cases |
| **4MB segment size** | Balances S3 PUT costs (~$0.005/1000) vs flush latency |

---

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

1. **Produce**: Client sends records to any broker via Kafka protocol
2. **Batch**: Broker validates, batches records, assigns offsets
3. **Flush**: When buffer reaches 4MB or 500ms, segment is sealed and uploaded to S3

Data is not acknowledged until S3 upload completes. This guarantees 11 nines durability on ACK.

---

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

1. **Fetch**: Consumer requests data from broker
2. **Cache check**: Broker looks up segment in LRU cache
3. **S3 fetch**: On cache miss, broker fetches from S3
4. **Populate**: Fetched segment is cached for future requests
5. **Return**: Data returned to consumer

Read-ahead prefetch is bounded and best-effort to avoid memory pressure.

---

## Multi-region reads (S3 CRR)

KafScale writes to a primary S3 bucket and can optionally read from replica buckets in other regions. With S3 Cross-Region Replication (CRR), objects written to the primary are asynchronously copied to replicas. Brokers attempt reads from their local replica and fall back to the primary on cache miss or CRR lag.

<div class="diagram">
  <div class="diagram-label">Cross-region replication</div>
  <svg viewBox="0 0 800 400" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="KafScale multi-region S3 reads">
    <defs>
      <marker id="ao-mr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#ffb347"/></marker>
      <marker id="ab-mr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#6aa7ff"/></marker>
      <marker id="ag-mr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#34d399"/></marker>
    </defs>

    <!-- ROW 1: Producers (left) and Consumers (right) -->
    <rect x="40" y="20" width="160" height="50" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="120" y="42" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Producers</text>
    <text x="120" y="57" font-size="9" fill="var(--diagram-label)" text-anchor="middle">write to primary region</text>

    <rect x="600" y="20" width="160" height="50" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
    <text x="680" y="42" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Consumers</text>
    <text x="680" y="57" font-size="9" fill="var(--diagram-label)" text-anchor="middle">read from nearest region</text>

    <!-- ROW 2: Primary region (center) -->
    <rect x="250" y="100" width="300" height="100" rx="12" fill="rgba(255, 179, 71, 0.08)" stroke="#ffb347" stroke-width="1.5" stroke-dasharray="6,3"/>
    <text x="400" y="122" font-size="11" font-weight="600" fill="#ffb347" text-anchor="middle">US-EAST-1 (Primary)</text>

    <rect x="275" y="140" width="110" height="48" rx="8" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="330" y="160" font-size="10" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Brokers</text>
    <text x="330" y="175" font-size="9" fill="var(--diagram-label)" text-anchor="middle">stateless pods</text>

    <rect x="415" y="140" width="110" height="48" rx="8" fill="rgba(255, 179, 71, 0.15)" stroke="#ffb347" stroke-width="1.5"/>
    <text x="470" y="160" font-size="10" font-weight="600" fill="#ffb347" text-anchor="middle">S3 Primary</text>
    <text x="470" y="175" font-size="9" fill="var(--diagram-label)" text-anchor="middle">segments</text>

    <!-- Producer to Primary Broker -->
    <path d="M200,45 L240,45 L240,164 L270,164" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#ab-mr)"/>
    <text x="120" y="85" font-size="9" fill="var(--diagram-label)">Kafka protocol</text>

    <!-- Primary Broker to S3 -->
    <path d="M385,164 L410,164" stroke="#ffb347" stroke-width="2" fill="none" marker-end="url(#ao-mr)"/>
    <text x="397" y="155" font-size="9" fill="#ffb347">write</text>

    <!-- Consumer to Primary -->
    <path d="M600,45 L560,45 L560,164 L530,164" stroke="#6aa7ff" stroke-width="2" stroke-dasharray="4,2" fill="none" marker-end="url(#ab-mr)"/>
    <text x="680" y="85" font-size="9" fill="var(--diagram-label)">nearest brokers</text>

    <!-- ROW 3: CRR label and arrows -->
    <text x="400" y="230" font-size="10" font-weight="600" fill="#34d399" text-anchor="middle">S3 Cross-Region Replication (async)</text>

    <path d="M440,200 L440,215 L180,215 L180,260" stroke="#34d399" stroke-width="1.5" stroke-dasharray="5,3" fill="none" marker-end="url(#ag-mr)"/>
    <path d="M500,200 L500,215 L620,215 L620,260" stroke="#34d399" stroke-width="1.5" stroke-dasharray="5,3" fill="none" marker-end="url(#ag-mr)"/>

    <!-- ROW 4: Replica regions -->
    <!-- EU Replica -->
    <rect x="40" y="255" width="280" height="100" rx="12" fill="rgba(106, 167, 255, 0.06)" stroke="#6aa7ff" stroke-width="1.5" stroke-dasharray="6,3"/>
    <text x="180" y="277" font-size="11" font-weight="600" fill="#6aa7ff" text-anchor="middle">EU-WEST-1 (Replica)</text>

    <rect x="65" y="295" width="110" height="48" rx="8" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="120" y="315" font-size="10" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Brokers</text>
    <text x="120" y="330" font-size="9" fill="var(--diagram-label)" text-anchor="middle">read local first</text>

    <rect x="195" y="295" width="100" height="48" rx="8" fill="rgba(255, 179, 71, 0.15)" stroke="#ffb347" stroke-width="1.5"/>
    <text x="245" y="315" font-size="10" font-weight="600" fill="#ffb347" text-anchor="middle">S3 Replica</text>
    <text x="245" y="330" font-size="9" fill="var(--diagram-label)" text-anchor="middle">CRR copy</text>

    <!-- Asia Replica -->
    <rect x="480" y="255" width="280" height="100" rx="12" fill="rgba(106, 167, 255, 0.06)" stroke="#6aa7ff" stroke-width="1.5" stroke-dasharray="6,3"/>
    <text x="620" y="277" font-size="11" font-weight="600" fill="#6aa7ff" text-anchor="middle">AP-SOUTHEAST-1 (Replica)</text>

    <rect x="505" y="295" width="110" height="48" rx="8" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
    <text x="560" y="315" font-size="10" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Brokers</text>
    <text x="560" y="330" font-size="9" fill="var(--diagram-label)" text-anchor="middle">read local first</text>

    <rect x="635" y="295" width="100" height="48" rx="8" fill="rgba(255, 179, 71, 0.15)" stroke="#ffb347" stroke-width="1.5"/>
    <text x="685" y="315" font-size="10" font-weight="600" fill="#ffb347" text-anchor="middle">S3 Replica</text>
    <text x="685" y="330" font-size="9" fill="var(--diagram-label)" text-anchor="middle">CRR copy</text>

    <!-- Local read arrows -->
    <path d="M175,319 L190,319" stroke="#6aa7ff" stroke-width="1.5" fill="none" marker-end="url(#ab-mr)"/>
    <text x="182" y="310" font-size="9" fill="#6aa7ff">read</text>

    <path d="M615,319 L630,319" stroke="#6aa7ff" stroke-width="1.5" fill="none" marker-end="url(#ab-mr)"/>
    <text x="622" y="310" font-size="9" fill="#6aa7ff">read</text>

    <!-- Caption -->
    <text x="400" y="385" font-size="10" fill="var(--diagram-label)" text-anchor="middle" font-style="italic">
      Brokers read from local replica first, fall back to primary on miss or CRR lag
    </text>
  </svg>
</div>

This pattern enables low-latency reads in multiple regions while maintaining a single write primary.

---

## Component responsibilities

| Component | Responsibilities |
|-----------|------------------|
| **Broker** | Kafka protocol, batching, offset assignment, S3 read/write, caching |
| **etcd** | Topic metadata, consumer offsets, group assignments, leader election |
| **S3** | Durable segment storage, source of truth, lifecycle-based retention |
| **Operator** | CRD reconciliation, etcd snapshots, broker lifecycle management |

---

## Segment format summary

Segments are self-contained files with header, Kafka-compatible record batches, and footer.

| Field | Size | Description |
|-------|------|-------------|
| Magic | 4 bytes | `0x4B414653` ("KAFS") |
| Version | 2 bytes | Format version (1) |
| Flags | 2 bytes | Compression codec |
| Base Offset | 8 bytes | First offset in segment |
| Message Count | 4 bytes | Number of messages |
| Timestamp | 8 bytes | Created (Unix ms) |
| Batches | variable | Kafka RecordBatch format |
| CRC32 | 4 bytes | Checksum |
| Footer Magic | 4 bytes | `0x454E4421` ("END!") |

See [Storage Format](/storage-format/) for complete details on segment structure, indexes, and S3 key layout.

---

## Next steps

- [Operations](/operations/) for S3 health states and failure modes
- [Storage Format](/storage-format/) for detailed segment and index layouts
- [Rationale](/rationale/) for why we made these design choices
