---
layout: doc
title: "KafScale Roadmap: Shipped Work and Open Gaps"
description: Completed milestones, current work, and what is planned next.
permalink: /roadmap/
nav_title: Roadmap
nav_order: 8
---

# Kafscale Roadmap

This roadmap tracks completed work and open gaps. It is intentionally high level; detailed specs live in `kafscale-spec.md`.

## Milestones (Completed)

- Core protocol parsing and metadata support
- Produce and fetch paths with S3-backed durability
- Consumer group coordination with offset and group persistence
- DescribeGroups/ListGroups ops visibility
- OffsetForLeaderEpoch consumer recovery
- DescribeConfigs/AlterConfigs ops tuning
- CreatePartitions/DeleteGroups ops APIs
- etcd topic/partition management
- Observability (structured logging, Grafana dashboard templates, expanded Prometheus metrics)
- Kubernetes operator with managed etcd + snapshot backups
- End-to-end tests for broker durability and operator resilience
- Admin ops API e2e coverage
- Security review (TLS/auth)
- End-to-end tests multi-segment restart durability
- Authentication / ACL and security hardening
- Kafka client mTLS at the proxy with authenticated principal forwarding to broker ACLs ([PR #214](https://github.com/KafScale/platform/pull/214), merged 27 October 2026)
