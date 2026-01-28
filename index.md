---
layout: default
title: KafScale - Stateless Kafka on S3
description: Kafka-compatible streaming with stateless brokers, S3-native storage, and Kubernetes-first operations. Apache 2.0 licensed.
---

<section class="hero">
  <p class="eyebrow">Apache 2.0 licensed. No vendor lock-in. Self-hosted.</p>
  <h1>One endpoint. Infinite scale.</h1>
  <p>Kafka-compatible streaming platform. <br>
  Scale streaming and analytics cloud-native on S3. Automated.</p>
  <div class="badge-row">
    <a href="https://github.com/KafScale/platform/stargazers" target="_blank" rel="noreferrer">
      <img alt="GitHub stars" src="https://img.shields.io/github/stars/KafScale/platform?style=social" />
    </a>
    <img alt="License" src="https://img.shields.io/badge/license-Apache%202.0-blue" />
    <img alt="Go version" src="https://img.shields.io/github/go-mod/go-version/KafScale/platform" />
    <a href="https://github.com/KafScale/platform/releases/latest" target="_blank" rel="noreferrer">
      <img alt="Current release" src="https://img.shields.io/github/v/release/KafScale/platform" />
    </a>
  </div>
  <div class="hero-actions">
    <a class="button" href="/quickstart/">Get started</a>
    <a class="button secondary" href="https://github.com/KafScale/platform" target="_blank" rel="noreferrer">View on GitHub</a>
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
      <h3>One endpoint, infinite producers</h3>
      <p>Kafka clients discover partition leaders and connect to each broker directly. KafScale's proxy rewrites metadata responses. One DNS name. Brokers scale behind it. Clients never break.</p>
    </div>
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
      <p>Processors read segments directly from S3, bypassing brokers entirely. Streaming and analytics never compete for the same resources.</p>
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
  <p>Clients connect to a single proxy endpoint. The proxy rewrites Kafka metadata responses so clients never see broker topology. Brokers flush segments to S3. Processors read directly from S3 without touching brokers.</p>
  
  <div class="diagram" style="margin: 2rem auto; max-width: 1200px;">
    <svg viewBox="0 0 800 380" role="img" aria-label="KafScale architecture diagram" style="width: 100%; height: auto;">
      <defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill="var(--diagram-stroke)"/>
        </marker>
        <marker id="arrow-green" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill="#34d399"/>
        </marker>
      </defs>

      <!-- Clients -->
      <rect x="40" y="30" width="140" height="60" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="1.5"/>
      <text x="110" y="55" font-size="13" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Kafka clients</text>
      <text x="110" y="75" font-size="11" fill="var(--diagram-label)" text-anchor="middle">Any library</text>

      <!-- Arrow to proxy -->
      <line x1="180" y1="60" x2="230" y2="60" stroke="var(--diagram-stroke)" stroke-width="1.5" marker-end="url(#arrow)"/>
      <text x="205" y="50" font-size="10" fill="var(--diagram-label)" text-anchor="middle">single IP</text>

      <!-- Proxy -->
      <rect x="240" y="30" width="140" height="60" rx="10" fill="rgba(255, 179, 71, 0.15)" stroke="#ffb347" stroke-width="2"/>
      <text x="310" y="55" font-size="13" font-weight="600" fill="#ffb347" text-anchor="middle">Proxy</text>
      <text x="310" y="75" font-size="11" fill="var(--diagram-label)" text-anchor="middle">One endpoint</text>

      <!-- Arrow to K8s -->
      <line x1="380" y1="60" x2="430" y2="60" stroke="var(--diagram-stroke)" stroke-width="1.5" marker-end="url(#arrow)"/>

      <!-- Kubernetes cluster -->
      <rect x="440" y="15" width="340" height="170" rx="14" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="2"/>
      <text x="460" y="40" font-size="12" font-weight="600" fill="var(--diagram-text)">Kubernetes</text>

      <!-- Brokers row -->
      <rect x="460" y="55" width="90" height="45" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"/>
      <text x="505" y="83" font-size="11" fill="var(--diagram-text)" text-anchor="middle">Broker 0</text>

      <rect x="560" y="55" width="90" height="45" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"/>
      <text x="605" y="83" font-size="11" fill="var(--diagram-text)" text-anchor="middle">Broker 1</text>

      <rect x="660" y="55" width="90" height="45" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"/>
      <text x="705" y="83" font-size="11" fill="var(--diagram-text)" text-anchor="middle">Broker N</text>

      <text x="610" y="120" font-size="10" fill="var(--diagram-label)" text-anchor="middle">Stateless, scale with HPA</text>

      <!-- Processors -->
      <rect x="460" y="130" width="140" height="45" rx="8" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1.5"/>
      <text x="530" y="158" font-size="11" font-weight="600" fill="#34d399" text-anchor="middle">Processors</text>

      <!-- S3 layer -->
      <rect x="40" y="280" width="740" height="80" rx="14" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="2"/>
      <text x="410" y="315" font-size="14" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">S3</text>
      <text x="410" y="340" font-size="11" fill="var(--diagram-label)" text-anchor="middle">Source of truth. 11 nines durability. Immutable .kfs segments.</text>

      <!-- Brokers to S3 arrow -->
      <line x1="610" y1="120" x2="610" y2="275" stroke="var(--diagram-stroke)" stroke-width="1.5" marker-end="url(#arrow)"/>
      <text x="625" y="220" font-size="10" fill="var(--diagram-label)">segments</text>

      <!-- Processors to S3 arrow (direct read) -->
      <line x1="530" y1="175" x2="530" y2="275" stroke="#34d399" stroke-width="1.5" marker-end="url(#arrow-green)"/>
      <text x="545" y="230" font-size="10" fill="#34d399">direct read</text>

    </svg>
    <p class="diagram-caption" style="text-align: center; font-size: 0.9rem; color: var(--diagram-label); margin-top: 1rem;">
      Streaming and analytics share data but never compete for resources.
    </p>
  </div>
  
  <div class="hero-actions">
    <a class="button secondary" href="/architecture/">Full architecture diagram</a>
  </div>
</section>

<section class="section">
  <h2>Built for AI agent infrastructure</h2>
  <p>
    AI agents making decisions need context. That context comes from historical events: what happened, in what order, and why the current state exists. Traditional stream processing optimizes for milliseconds. Agents need something different: completeness, replay capability, and the ability to reconcile current state with historical actions.
  </p>
  <p>
    Storage-native streaming makes this practical. The immutable log in S3 becomes the source of truth that agents query, replay, and reason over. The Iceberg Processor converts that log to tables that analytical tools understand. Agents get complete historical context without competing with streaming workloads for broker resources.
  </p>
  <p>
    Two-second latency for analytical access is acceptable when the alternative is incomplete context or degraded streaming performance. AI agents do not need sub-millisecond reads. They need the full picture.
  </p>
</section>

<section class="section">
  <h2>Processors</h2>
  <p>Processors read completed segments directly from S3, enabling independent scaling and custom implementations. The .kfs segment format is open and documented.</p>
  <div class="grid">
    <div class="card">
      <h3>Iceberg Processor</h3>
      <p>Reads .kfs segments from S3. Writes Parquet to Iceberg tables. Works with Unity Catalog, Polaris, AWS Glue. Zero broker load.</p>
      <a class="button secondary" href="/processors/iceberg/">Deployment guide</a>
    </div>
    <div class="card">
      <h3>SQL Processor (KAFSQL)</h3>
      <p>Query KafScale segments with Postgres-compatible SQL. No Flink, no Spark, no complex pipelines. Just SQL against your Kafka data in S3.</p>
      <a class="button secondary" href="/processors/sql/">Documentation</a>
    </div>
    <div class="card">
      <h3>Build your own</h3>
      <p>The .kfs segment format is documented. Build processors for your use case without waiting for vendors or negotiating enterprise contracts.</p>
      <a class="button secondary" href="/storage-format/">Storage format spec</a>
    </div>
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
      <h3>Operations</h3>
      <p>Scaling, backups, monitoring, and production hardening.</p>
      <a class="button secondary" href="/operations/">Operations guide</a>
    </div>
  </div>
</section>

<section class="section">
  <h2>Get started</h2>
  <p>
    Install the operator, define a topic, produce with existing Kafka tools.
    If you already run Kubernetes and Kafka clients, you can deploy a cluster
    and start producing data in minutes.
  </p>
  <div class="hero-actions">
    <a class="button" href="/quickstart/">Quickstart guide</a>
    <a class="button secondary" href="https://github.com/KafScale/platform" target="_blank" rel="noreferrer">View on GitHub</a>
  </div>
</section>

<section class="section backers">
  <h2>Backed by</h2>
  <p>KafScale is developed and maintained with support from <a href="https://www.scalytics.io" target="_blank" rel="noreferrer">Scalytics, Inc.</a> and <a href="https://www.novatechflow.com" target="_blank" rel="noreferrer">NovaTechflow</a>.</p>
  <p>Apache 2.0 licensed. No CLA required. <a href="https://github.com/KafScale/platform/blob/main/CONTRIBUTING.md">Contributions welcome</a>.</p>
</section>
