---
layout: doc
title: Security Overview
description: Current security posture, hardening guidance, and roadmap for KafScale.
---

# Security Overview

KafScale is a Kubernetes-native platform focused on Kafka protocol parity and operational stability. This document summarizes the current security posture and the boundaries of what is and is not supported in v1.

## Current security posture (v1)

- **Authentication**: none at the Kafka protocol layer. Brokers accept any client connection. The console UI supports basic auth via `KAFSCALE_UI_USERNAME` / `KAFSCALE_UI_PASSWORD`.
- **Authorization**: none. Admin APIs are unauthenticated and authorized implicitly.
- **Transport security**: TLS is optional and must be enabled by operators via `KAFSCALE_BROKER_TLS_*` and `KAFSCALE_CONSOLE_TLS_*`.
- **Secrets handling**: S3 credentials are read from Kubernetes secrets and are not written to etcd or source control.
- **Data at rest**: stored in S3 and etcd; encryption at rest depends on your provider policies.
- **Network trust**: assumes a private network or cluster-level controls (SecurityGroups, NetworkPolicies, ingress rules).

## Operational guidance

- Deploy brokers and the console behind private networking or VPNs.
- Enable TLS for broker and console endpoints in production.
- Restrict ingress to trusted clients and operator components.
- Use least-privilege IAM roles for S3 access and restrict etcd endpoints.
- Treat the console as privileged; do not expose it publicly without auth.

## Known gaps

- No SASL or mTLS authentication for Kafka protocol clients.
- No ACLs or RBAC at the broker layer.
- No multi-tenant isolation.
- Admin APIs are writable without auth; UI is read-only by policy, not enforcement.

## Roadmap

- TLS enabled by default in production templates.
- SASL/PLAIN and SASL/SCRAM for Kafka client authentication.
- Authorization / ACL layer for broker admin and data plane APIs.
- Optional mTLS for broker and console endpoints.

## Reporting security issues

Follow the process in [SECURITY.md](https://github.com/novatechflow/kafscale/blob/main/SECURITY.md).
