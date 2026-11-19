---
layout: default
title: "KafScale Demos: Terminal Walkthroughs"
description: Short terminal videos that show KafScale install, CLI, schema registry, MCP and day-2 operations, each following the docs step by step.
permalink: /demos/
---

<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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


<section class="demos-intro">
  <h1>Demos</h1>
  <p>Each video is a real terminal session on a kind cluster. It follows one doc page step by step, so you can run the same commands yourself.</p>
</section>

<section class="demo" id="admin">
  <h2>Admin: install and first topic</h2>
  <p>For platform admins setting up KafScale for the first time.</p>
  <p>Installs KafScale with Helm, creates a cluster and a topic, then produces and consumes through a broker and the proxy.</p>
  <div class="term"><div class="term-bar" aria-hidden="true"><i></i><i></i><i></i><span>admin: helm install, cluster, topic</span></div><video autoplay muted loop playsinline preload="metadata" width="960" height="540" poster="/media/demos/admin/admin-poster.webp" aria-label="Terminal demo: KafScale is installed with Helm, a cluster and a topic are created, and records are produced and consumed through a broker and the proxy"><source src="/media/demos/admin/admin.webm" type="video/webm"><source src="/media/demos/admin/admin.mp4" type="video/mp4"></video></div>
  <div class="hero-actions"><a class="button secondary" href="/quickstart/">Follow along: Quickstart</a></div>
</section>

<section class="demo" id="cli">
  <h2>CLI: restore and prune topics</h2>
  <p>For operators who manage topic data with the kafscale CLI.</p>
  <p>Restores a topic from its segments in S3 into a new topic, deletes that topic, then prunes its leftover data from S3.</p>
  <div class="term"><div class="term-bar" aria-hidden="true"><i></i><i></i><i></i><span>cli: restore from S3, prune</span></div><video data-demo controls muted loop playsinline preload="none" width="960" height="540" poster="/media/demos/cli/cli-poster.webp" aria-label="Terminal demo: the kafscale CLI restores a topic from S3 and then prunes deleted topics"><source src="/media/demos/cli/cli.webm" type="video/webm"><source src="/media/demos/cli/cli.mp4" type="video/mp4"></video></div>
  <div class="hero-actions"><a class="button secondary" href="/operations/">Follow along: Operations</a></div>
</section>

<section class="demo" id="schema-registry">
  <h2>Schema registry: import and compatibility</h2>
  <p>For teams moving schemas from Apicurio to the KafScale schema registry.</p>
  <p>Imports schemas from Apicurio, then sets BACKWARD compatibility, checks one compatible and one incompatible change, and registers the compatible one.</p>
  <div class="term"><div class="term-bar" aria-hidden="true"><i></i><i></i><i></i><span>schema-registry: Apicurio import, BACKWARD</span></div><video data-demo controls muted loop playsinline preload="none" width="960" height="540" poster="/media/demos/schema-registry/schema-registry-poster.webp" aria-label="Terminal demo: schemas are imported from Apicurio into the KafScale schema registry and BACKWARD compatibility is enforced"><source src="/media/demos/schema-registry/schema-registry.webm" type="video/webm"><source src="/media/demos/schema-registry/schema-registry.mp4" type="video/mp4"></video></div>
  <div class="hero-actions"><a class="button secondary" href="https://github.com/KafScale/platform/blob/main/docs/schema-registry.md" target="_blank" rel="noreferrer">Follow along: Schema registry</a></div>
</section>

<section class="demo" id="mcp">
  <h2>MCP: connect an AI client</h2>
  <p>For teams that want an AI assistant to read cluster state.</p>
  <p>Connects an MCP client with a bearer token stored in a Kubernetes Secret, then queries cluster status, a consumer group and cluster metrics summed over all brokers.</p>
  <div class="term"><div class="term-bar" aria-hidden="true"><i></i><i></i><i></i><span>mcp: token, status, group, metrics</span></div><video data-demo controls muted loop playsinline preload="none" width="960" height="540" poster="/media/demos/mcp/mcp-poster.webp" aria-label="Terminal demo: an MCP client connects to the KafScale MCP service with a token and queries cluster status, a consumer group and cluster metrics"><source src="/media/demos/mcp/mcp.webm" type="video/webm"><source src="/media/demos/mcp/mcp.mp4" type="video/mp4"></video></div>
  <div class="hero-actions"><a class="button secondary" href="/ops-mcp/">Follow along: Ops MCP</a></div>
</section>

<section class="demo" id="ops">
  <h2>Ops: day-2 operations</h2>
  <p>For on-call operators running KafScale in production.</p>
  <p>Restarts a broker, rotates the S3 keys, takes an etcd snapshot and scales brokers out with the HPA.</p>
  <div class="term"><div class="term-bar" aria-hidden="true"><i></i><i></i><i></i><span>ops: restart, S3 rotation, snapshot, HPA</span></div><video data-demo controls muted loop playsinline preload="none" width="960" height="540" poster="/media/demos/ops/ops-poster.webp" aria-label="Terminal demo: a broker is restarted, S3 keys are rotated, an etcd snapshot is taken and the broker HPA scales out"><source src="/media/demos/ops/ops.webm" type="video/webm"><source src="/media/demos/ops/ops.mp4" type="video/mp4"></video></div>
  <div class="hero-actions"><a class="button secondary" href="/operations/">Follow along: Operations</a></div>
</section>

<script src="/assets/js/demos.js" defer></script>
