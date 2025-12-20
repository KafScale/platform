---
layout: default
title: KafScale — Stateless Kafka on S3
description: Kafka-compatible streaming with stateless brokers, S3-native storage, and Kubernetes-first operations.
---

<section class="hero">
  <p class="eyebrow">Kafka-compatible streaming. Stateless brokers. S3 storage. Self-hosted.</p>
  <h1>Stateless Kafka on S3, compatible with your clients.</h1>
  <p>Run Kafka APIs without stateful disks. KafScale stores log segments in S3, keeps brokers ephemeral, and uses etcd for metadata so you scale fast and recover cleanly.</p>
  <ul class="hero-list">
    <li>Stateless brokers that scale fast on Kubernetes</li>
    <li>S3-native segments with lifecycle-based retention</li>
    <li>Kafka protocol compatibility for existing clients</li>
  </ul>
  <div class="badge-row">
    <img alt="GitHub stars" src="https://img.shields.io/github/stars/novatechflow/kafscale?style=flat" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache%202.0-blue" />
  </div>
  <div class="hero-actions">
    <a class="button" href="/quickstart/">Get started</a>
    <a class="button secondary" href="https://github.com/novatechflow/kafscale" target="_blank" rel="noreferrer">View on GitHub</a>
  </div>
</section>

<section class="section">
  <h2>Why teams adopt KafScale</h2>
  <div class="grid">
    <div class="card">
      <h3>Stateless brokers</h3>
      <p>Spin brokers up and down without disk shuffles. S3 is the source of truth.</p>
    </div>
    <div class="card">
      <h3>S3-native durability</h3>
      <p>Immutable segments, predictable retention, and lifecycle in object storage.</p>
    </div>
    <div class="card">
      <h3>etcd metadata</h3>
      <p>Offsets, topics, and group state live in etcd for fast consensus and control.</p>
    </div>
    <div class="card">
      <h3>Kubernetes operator</h3>
      <p>CRDs manage clusters, topics, snapshots, and UI access with clean drift control.</p>
    </div>
  </div>
</section>

<section class="section">
  <h2>How KafScale works</h2>
  <div class="diagram">
    <div class="diagram-label">Architecture overview</div>
    <svg viewBox="0 0 900 370" role="img" aria-label="KafScale architecture diagram">
      <defs>
        <marker id="arrow" markerWidth="10" markerHeight="10" refX="6" refY="3" orient="auto">
          <path d="M0,0 L0,6 L6,3 z" fill="var(--text)"></path>
        </marker>
      </defs>
      <rect x="20" y="20" width="860" height="220" rx="18" ry="18" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="2"></rect>
      <text x="40" y="55" font-size="16" font-weight="600">Kubernetes cluster</text>
      <text x="70" y="82" font-size="12" opacity="0.75">Stateless brokers (HPA, scale out/in)</text>

      <rect x="70" y="90" width="140" height="70" rx="12" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="88" y="132" font-size="14">Broker 0</text>

      <rect x="230" y="90" width="140" height="70" rx="12" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="248" y="132" font-size="14">Broker 1</text>

      <rect x="390" y="90" width="140" height="70" rx="12" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="408" y="132" font-size="14">Broker N</text>
      <text x="375" y="132" font-size="18" opacity="0.7">...</text>

      <rect x="600" y="82" width="220" height="90" rx="12" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)"></rect>
      <text x="615" y="104" font-size="13" font-weight="600">etcd (HA)</text>
      <rect x="615" y="112" width="50" height="38" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <rect x="675" y="112" width="50" height="38" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <rect x="735" y="112" width="50" height="38" rx="8" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="631" y="135" font-size="14" opacity="0.7">•</text>
      <text x="691" y="135" font-size="14" opacity="0.7">••</text>
      <text x="751" y="135" font-size="14" opacity="0.7">•••</text>
      <text x="615" y="164" font-size="11" opacity="0.7">Metadata + offsets</text>

      <rect x="70" y="175" width="200" height="55" rx="10" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)"></rect>
      <text x="90" y="198" font-size="12" font-weight="600">Operator (HA)</text>
      <rect x="90" y="205" width="70" height="20" rx="6" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <rect x="175" y="205" width="70" height="20" rx="6" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="118" y="219" font-size="12" opacity="0.7">•</text>
      <text x="203" y="219" font-size="12" opacity="0.7">••</text>

      <line x1="530" y1="125" x2="600" y2="125" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow)"></line>
      <text x="532" y="112" font-size="10" opacity="0.7">metadata</text>

      <line x1="300" y1="160" x2="300" y2="250" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow)"></line>
      <text x="312" y="220" font-size="10" opacity="0.7">segments</text>

      <rect x="20" y="250" width="860" height="100" rx="16" fill="var(--diagram-fill)" stroke="var(--diagram-stroke)" stroke-width="2"></rect>
      <text x="40" y="278" font-size="14" font-weight="600">S3 storage (durable)</text>

      <rect x="60" y="285" width="340" height="52" rx="12" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="80" y="317" font-size="12" font-weight="600">Segments + index</text>

      <rect x="500" y="285" width="340" height="52" rx="12" fill="var(--diagram-accent)" stroke="var(--diagram-stroke)"></rect>
      <text x="520" y="317" font-size="12" font-weight="600">etcd snapshots bucket</text>

      <line x1="695" y1="180" x2="695" y2="285" stroke="var(--diagram-stroke)" stroke-width="2" marker-end="url(#arrow)"></line>
      <text x="707" y="238" font-size="10" opacity="0.7">snapshots</text>
    </svg>
  </div>
  <div class="hero-actions">
    <a class="button secondary" href="/architecture/">See full architecture flows</a>
  </div>
</section>

<section class="section">
  <h2>Quickstart in minutes</h2>
  <div class="quickstart-flow">
    <div class="quickstart-card">
      <h3>1. Install operator</h3>
      <pre class="code-block"><code>helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints={} \
  --set operator.image.tag=latest \
  --set console.image.tag=latest</code></pre>
    </div>
    <div class="quickstart-card">
      <h3>2. Create your first topic</h3>
      <pre class="code-block"><code>kubectl apply -n kafscale -f - &lt;&lt;'EOF'
apiVersion: kafscale.novatechflow.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: orders
spec:
  clusterRef: demo
  partitions: 3
EOF</code></pre>
    </div>
    <div class="quickstart-card">
      <h3>3. Produce and consume</h3>
      <pre class="code-block"><code>kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic orders
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic orders --from-beginning</code></pre>
    </div>
  </div>
  <div class="hero-actions">
    <a class="button secondary" href="/quickstart/">Read the full quickstart</a>
  </div>
</section>

<section class="section">
  <h2>Operations you can trust</h2>
  <div class="grid">
    <div class="card">
      <h3>Prometheus metrics</h3>
      <p>Track S3 health state, broker throughput, and operator snapshots.</p>
    </div>
    <div class="card">
      <h3>Scale instantly</h3>
      <p>Stateless brokers pair with HPA and avoid disk rebalancing delays.</p>
    </div>
    <div class="card">
      <h3>Clear failure modes</h3>
      <p>Startup gating, S3 health states, and etcd snapshot checks keep you safe.</p>
    </div>
  </div>
</section>

<section class="section">
  <h2>Deep technical reference</h2>
  <div class="grid">
    <div class="card">
      <h3>Protocol coverage</h3>
      <p>Support for Produce, Fetch, Metadata, consumer groups, and admin APIs.</p>
      <a class="button secondary" href="/api/">View API docs</a>
    </div>
    <div class="card">
      <h3>Storage format</h3>
      <p>Segment and index file layout plus S3 key structure for lifecycle design.</p>
      <a class="button secondary" href="/storage-format/">Explore storage format</a>
    </div>
    <div class="card">
      <h3>Security posture</h3>
      <p>Current TLS and auth status, plus the roadmap for SASL and ACLs.</p>
      <a class="button secondary" href="/security/">Read security overview</a>
    </div>
  </div>
</section>
