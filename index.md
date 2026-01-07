---
layout: default
title: KafScale - Stateless Kafka on S3
description: Kafka-compatible streaming with stateless brokers, S3-native storage, and Kubernetes-first operations. Apache 2.0 licensed.
---

<section class="hero">
  <p class="eyebrow">Apache 2.0 licensed. No vendor lock-in. Self-hosted.</p>
  <h1>Stateless Kafka on S3, compatible with your clients.</h1>
  <p>Run Kafka APIs without stateful disks. KafScale stores segments in S3, keeps brokers ephemeral, and lets processors read directly from storage. Scale streaming and analytics independently. Give AI agents what they actually need: immutable logs, full replay, and complete context.</p>
  <div class="badge-row">
    <img alt="GitHub stars" src="https://img.shields.io/github/stars/novatechflow/kafscale?style=flat" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache%202.0-blue" />
    <img alt="Go version" src="https://img.shields.io/github/go-mod/go-version/novatechflow/kafscale" />
    <img alt="Current release" src="https://img.shields.io/github/v/release/novatechflow/kafscale" />
  </div>
  <div class="hero-actions">
    <a class="button" href="/quickstart/">Get started</a>
    <a class="button secondary" href="https://github.com/novatechflow/kafscale" target="_blank" rel="noreferrer">View on GitHub</a>
  </div>
</section>

<section class="section">
  <h2>What teams are saying</h2>
  <div class="grid">
    <div class="card">
      <p>"After WarpStream got acquired, KafScale became our go-to. Better S3 integration, lower latency than we expected, fully scalable, and minimal ops burden."</p>
      <p><strong>— Platform team, Series B fintech</strong></p>
    </div>
    <div class="card">
      <p>"We moved 50 topics off Kafka in a weekend. No more disk alerts, no more partition rebalancing. Our on-call rotation got a lot quieter."</p>
      <p><strong>— SRE lead, e-commerce platform</strong></p>
    </div>
    <div class="card">
      <p>"The Apache 2.0 license was the deciding factor. We can't build on BSL projects, and we won't depend on a vendor's control plane."</p>
      <p><strong>— CTO, healthcare data startup</strong></p>
    </div>
  </div>
</section>

<section class="section">
  <h2>Why teams adopt KafScale</h2>
  <div class="grid grid-3x2">
    <div class="card">
      <h3>Stateless brokers</h3>
      <p>Spin brokers up and down without disk shuffles. S3 is the source of truth. No partition rebalancing, ever.</p>
    </div>
    <div class="card">
      <h3>S3-native durability</h3>
      <p>11 nines of durability. Immutable segments, lifecycle-based retention, predictable costs.</p>
    </div>
    <div class="card">
      <h3>Storage-native processing</h3>
      <p>Processors read segments directly from S3, bypassing brokers entirely. Streaming and analytics never compete.</p>
    </div>
    <div class="card">
      <h3>Kubernetes operator</h3>
      <p>CRDs for clusters, topics, and snapshots. HPA-ready scaling. GitOps-friendly.</p>
    </div>
    <div class="card">
      <h3>Open segment format</h3>
      <p>The .kfs format is documented. Build custom processors without waiting for vendors to ship features.</p>
    </div>
    <div class="card">
      <h3>Apache 2.0 license</h3>
      <p>No BSL restrictions. No usage fees. No control plane dependency. Fork it, sell it, run it however you want.</p>
    </div>
  </div>
</section>

<section class="section manifesto">
  <h2>The Rationale: Kafka brokers are a legacy artifact</h2>
  <p>
    Kafka brokers were designed for a disk-centric world where durability lived on local machines.
    Replication and rebalancing were necessary because broker state was the source of truth.
  </p>
  <p>
    Object storage changes this model.
    Once log segments are durable, immutable, and external, long-lived broker state stops adding resilience
    and starts adding operational cost.
  </p>
  <p>
    Stateless brokers backed by object storage simplify failure, scaling, and recovery.
    Brokers become ephemeral compute. Data remains durable.
  </p>
  <p>
    KafScale is built on this assumption.
    The Kafka protocol still matters. Broker-centric storage does not.
  </p>
</section>

<section class="section tradeoffs">
  <h2>What You Should Consider</h2>
  <p>KafScale is not a drop-in replacement for every Kafka workload. Here's when it fits and when it doesn't.</p>
  <div class="grid">
    <div class="card">
      <h3>KafScale is for you if</h3>
      <ul>
        <li>Latency of 200-500ms is acceptable</li>
        <li>You run ETL, logs, or async events</li>
        <li>You want processors that bypass brokers (Iceberg, analytics, AI agents)</li>
        <li>You want minimal ops and no disk management</li>
        <li>Apache 2.0 licensing matters to you</li>
        <li>You prefer self-hosted over managed services</li>
      </ul>
    </div>
    <div class="card">
      <h3>KafScale is not for you if</h3>
      <ul>
        <li>You need sub-10ms latency</li>
        <li>You require Kafka transactions (exactly-once across topics)</li>
        <li>You rely on compacted topics</li>
        <li>You want a fully managed service</li>
      </ul>
    </div>
  </div>
  <div class="hero-actions">
    <a class="button secondary" href="/comparison/">See full comparison with alternatives</a>
  </div>
</section>

<section class="section">
  <h2>How KafScale works</h2>
  <p>Clients speak the Kafka protocol to stateless brokers. Brokers flush segments to S3 and serve reads with caching. Processors read completed segments directly from S3 without adding load to brokers.</p>
  <div class="diagram">
    <svg viewBox="0 0 900 410" role="img" aria-label="KafScale architecture diagram">
      <defs>
        <marker id="arrow" markerWidth="10" markerHeight="10" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill="var(--diagram-stroke)"></path>
        </marker>
      </defs>

      <!-- Clients -->
      <rect x="20" y="95" width="170" height="80" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"></rect>
      <text x="35" y="120" font-size="13" font-weight="600" fill="var(--diagram-text)">Kafka clients</text>
      <text x="35" y="142" font-size="11" fill="var(--diagram-label)">Producers, consumers</text>
      <text x="35" y="160" font-size="11" fill="var(--diagram-label)">CLI tools, Connect</text>

      <!-- Kubernetes -->
      <rect x="210" y="20" width="670" height="230" rx="18" ry="18" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="2"></rect>
      <text x="230" y="55" font-size="16" font-weight="600" fill="var(--diagram-text)">Kubernetes cluster</text>
      <text x="260" y="82" font-size="12" fill="var(--diagram-label)">Stateless brokers (HPA, scale out/in)</text>

      <!-- Arrow: clients -> brokers -->
      <line x1="190" y1="135" x2="245" y2="135" stroke="var(--diagram-stroke)" stroke-width="1.5" marker-end="url(#arrow)"></line>
      <text x="195" y="126" font-size="10" fill="var(--diagram-label)">Kafka protocol</text>

      <!-- Brokers -->
      <rect x="260" y="105" width="120" height="55" rx="10" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="290" y="138" font-size="13" fill="var(--diagram-text)">Broker 0</text>

      <rect x="400" y="105" width="120" height="55" rx="10" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="430" y="138" font-size="13" fill="var(--diagram-text)">Broker 1</text>

      <rect x="540" y="105" width="120" height="55" rx="10" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="570" y="138" font-size="13" fill="var(--diagram-text)">Broker N</text>

      <!-- etcd -->
      <rect x="690" y="90" width="170" height="95" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"></rect>
      <text x="705" y="113" font-size="13" font-weight="600" fill="var(--diagram-text)">etcd cluster</text>
      <text x="705" y="134" font-size="11" fill="var(--diagram-label)">Topic map, offsets,</text>
      <text x="705" y="150" font-size="11" fill="var(--diagram-label)">consumer group state</text>
      <circle cx="720" cy="165" r="10" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></circle>
      <circle cx="755" cy="165" r="10" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></circle>
      <circle cx="790" cy="165" r="10" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></circle>

      <!-- Operator -->
      <rect x="260" y="185" width="180" height="50" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)"></rect>
      <text x="305" y="215" font-size="12" font-weight="600" fill="var(--diagram-text)">Operator</text>

      <!-- Brokers -> etcd -->
      <line x1="660" y1="132" x2="688" y2="132" stroke="var(--diagram-stroke)" stroke-width="1.5" marker-end="url(#arrow)"></line>
      <text x="615" y="121" font-size="10" fill="var(--diagram-label)">control plane</text>

      <!-- Brokers -> S3 -->
      <line x1="470" y1="160" x2="470" y2="280" stroke="var(--diagram-stroke)" stroke-width="1.5" marker-end="url(#arrow)"></line>
      <text x="482" y="225" font-size="10" fill="var(--diagram-label)">data plane (segments)</text>

      <!-- S3 -->
      <rect x="20" y="290" width="860" height="110" rx="16" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="2"></rect>
      <text x="40" y="322" font-size="14" font-weight="600" fill="var(--diagram-text)">Amazon S3 (11 nines durability)</text>
      <text x="40" y="342" font-size="11" fill="var(--diagram-label)">Source of truth for log data</text>

      <rect x="60" y="350" width="300" height="40" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="80" y="375" font-size="12" fill="var(--diagram-text)">Log segments + indexes</text>

      <rect x="540" y="350" width="300" height="40" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="560" y="375" font-size="12" fill="var(--diagram-text)">etcd snapshots (backup)</text>

      <!-- Operator -> S3 snapshots -->
      <line x1="780" y1="185" x2="780" y2="345" stroke="var(--diagram-stroke)" stroke-width="1.5" marker-end="url(#arrow)"></line>
      <text x="792" y="270" font-size="10" fill="var(--diagram-label)">snapshots</text>
    </svg>
    <p class="diagram-caption">
      S3 is the source of truth. Brokers are ephemeral. Processors read directly from S3.
    </p>
  </div>
  <div class="hero-actions">
    <a class="button secondary" href="/architecture/">See detailed architecture flows</a>
  </div>
</section>

<section class="section">
  <h2>Bypass the broker: storage-native processing</h2>
  <p>
    Traditional Kafka forces all reads through brokers. Streaming consumers and batch analytics compete for the same resources. Backfills spike broker CPU. AI training jobs block production consumers.
  </p>
  <p>
    KafScale stores data in S3 using a documented segment format. Processors read directly from S3 without touching brokers. The streaming path and the analytical path share data but never interfere.
  </p>
  <div class="diagram">
    <svg viewBox="0 0 800 200" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Storage-native processing: two read paths">
      <defs>
        <marker id="arr-blue" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" fill="#6aa7ff"/>
        </marker>
        <marker id="arr-green" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" fill="#34d399"/>
        </marker>
        <marker id="arr-orange" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" fill="#ffb347"/>
        </marker>
      </defs>

      <!-- S3 bucket (center) -->
      <rect x="310" y="70" width="180" height="60" rx="12" fill="rgba(255, 179, 71, 0.12)" stroke="#ffb347" stroke-width="2"/>
      <text x="400" y="95" font-size="13" font-weight="700" fill="#ffb347" text-anchor="middle">S3</text>
      <text x="400" y="115" font-size="10" fill="var(--diagram-label)" text-anchor="middle">.kfs segments</text>

      <!-- Streaming path (top) -->
      <rect x="40" y="20" width="140" height="50" rx="10" fill="rgba(106, 167, 255, 0.2)" stroke="#6aa7ff" stroke-width="1.5"/>
      <text x="110" y="42" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Brokers</text>
      <text x="110" y="58" font-size="9" fill="var(--diagram-label)" text-anchor="middle">Kafka protocol</text>

      <rect x="620" y="20" width="140" height="50" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
      <text x="690" y="42" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Consumers</text>
      <text x="690" y="58" font-size="9" fill="var(--diagram-label)" text-anchor="middle">streaming reads</text>

      <!-- Streaming arrows -->
      <path d="M180,45 L305,80" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#arr-blue)"/>
      <path d="M490,80 L615,45" stroke="#6aa7ff" stroke-width="2" fill="none" marker-end="url(#arr-blue)"/>
      <text x="400" y="45" font-size="10" font-weight="600" fill="#6aa7ff" text-anchor="middle">STREAMING PATH</text>

      <!-- Analytics path (bottom) -->
      <rect x="40" y="130" width="140" height="50" rx="10" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1.5"/>
      <text x="110" y="152" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Processors</text>
      <text x="110" y="168" font-size="9" fill="var(--diagram-label)" text-anchor="middle">bypass brokers</text>

      <rect x="620" y="130" width="140" height="50" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
      <text x="690" y="152" font-size="11" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Iceberg / Analytics</text>
      <text x="690" y="168" font-size="9" fill="var(--diagram-label)" text-anchor="middle">AI agents, Spark, etc</text>

      <!-- Analytics arrows -->
      <path d="M180,155 L305,120" stroke="#34d399" stroke-width="2" fill="none" marker-end="url(#arr-green)"/>
      <path d="M490,120 L615,155" stroke="#34d399" stroke-width="2" fill="none" marker-end="url(#arr-green)"/>
      <text x="400" y="175" font-size="10" font-weight="600" fill="#34d399" text-anchor="middle">ANALYTICS PATH</text>
    </svg>
    <p class="diagram-caption">
      Two read paths, one data source. Streaming and analytics scale independently.
    </p>
  </div>
</section>

<section class="section">
  <h2>Processors and addons</h2>
  <p>KafScale keeps processing separate from the broker layer. Processors read completed segments directly from S3, enabling independent scaling and custom implementations. See <a href="https://www.novatechflow.com/2025/12/data-processing-does-not-belong-in.html" target="_blank" rel="noreferrer">why data processing does not belong in the message broker</a>.</p>
  <div class="grid">
    <div class="card">
      <h3>Iceberg Processor</h3>
      <p>Reads .kfs segments from S3. Writes Parquet to Iceberg tables. Works with Unity Catalog, Polaris, AWS Glue. Zero broker load.</p>
      <a class="button secondary" href="/processors/iceberg/">Deployment guide</a>
    </div>
    <div class="card">
      <h3>Build your own</h3>
      <p>The .kfs segment format is documented and open. Build processors for your use case without waiting for vendors to ship features or negotiating enterprise contracts.</p>
      <a class="button secondary" href="/storage-format/">Storage format spec</a>
      <a class="button secondary" href="https://github.com/novatechflow/kafscale/blob/main/addons/processors/iceberg-processor/developer.md" target="_blank" rel="noreferrer">Developer guide</a>
    </div>
  </div>
</section>

<section class="section">
  <h2>Why AI agents need this architecture</h2>
  <p>
    AI agents making decisions need context. That context comes from historical events: what happened, in what order, and why the current state exists. Traditional stream processing optimizes for milliseconds. Agents need something different: completeness, replay capability, and the ability to reconcile current state with historical actions.
  </p>
  <p>
    Storage-native streaming makes this practical. The immutable log in S3 becomes the source of truth that agents query, replay, and reason over. The Iceberg Processor converts that log to tables that analytical tools understand. Agents get complete historical context without competing with streaming workloads for broker resources.
  </p>
  <p>
    Two-second latency for analytical access is acceptable when the alternative is incomplete context or degraded streaming performance. AI agents do not need sub-millisecond reads. They need the full picture.
  </p>
  <div class="hero-actions">
    <a class="button secondary" href="https://www.scalytics.io/blog/streaming-data-becomes-storage-native" target="_blank" rel="noreferrer">Read the full analysis</a>
  </div>
</section>

<section class="section">
  <h2>Production-grade operations</h2>
  <div class="grid">
    <div class="card">
      <h3>Prometheus metrics</h3>
      <p>S3 health state, produce/fetch throughput, consumer lag, etcd snapshot age. Grafana dashboards included.</p>
    </div>
    <div class="card">
      <h3>Horizontal scaling</h3>
      <p>Add brokers instantly. No partition rebalancing. HPA scales on CPU or custom metrics.</p>
    </div>
    <div class="card">
      <h3>Automated backups</h3>
      <p>Operator snapshots etcd to S3 on a schedule. One-command restore.</p>
    </div>
    <div class="card">
      <h3>Health gating</h3>
      <p>Brokers track S3 availability. Degraded and unavailable states prevent data loss.</p>
    </div>
  </div>
  <div class="hero-actions">
    <a class="button secondary" href="/operations/">Operations guide</a>
  </div>
</section>

<section class="section">
  <h2>Documentation</h2>
  <div class="grid">
    <div class="card">
      <h3>Protocol compatibility</h3>
      <p>21 Kafka APIs supported. Produce, Fetch, Metadata, consumer groups, and more.</p>
      <a class="button secondary" href="/protocol/">View API docs</a>
    </div>
    <div class="card">
      <h3>Storage format</h3>
      <p>Segment layout, index structure, S3 key paths, and cache architecture.</p>
      <a class="button secondary" href="/storage-format/">Explore storage</a>
    </div>
    <div class="card">
      <h3>Security</h3>
      <p>TLS configuration, S3 IAM policies, and the roadmap for SASL and ACLs.</p>
      <a class="button secondary" href="/security/">Security guide</a>
    </div>
  </div>
</section>

<section class="section">
  <h2>Get started</h2>
  <p>
    KafScale is designed to be operationally simple from day one.
    If you already run Kubernetes and Kafka clients, you can deploy a cluster
    and start producing data in minutes.
  </p>
  <p>
    Install the operator, define a topic, produce with existing Kafka tools.
  </p>
  <div class="hero-actions">
    <a class="button" href="/quickstart/">Quickstart guide</a>
    <a class="button secondary" href="https://github.com/novatechflow/kafscale" target="_blank" rel="noreferrer">View on GitHub</a>
  </div>
</section>

<section class="section backers">
  <h2>Backed by</h2>
  <p>KafScale is developed and maintained with support from <a href="https://scalytics.io" target="_blank" rel="noreferrer">Scalytics, Inc.</a> and <a href="https://novatechflow.com" target="_blank" rel="noreferrer">NovaTechFlow</a>.</p>
  <p>Apache 2.0 licensed. No CLA required. <a href="https://github.com/novatechflow/kafscale/blob/main/CONTRIBUTING.md">Contributions welcome</a>.</p>
</section>
