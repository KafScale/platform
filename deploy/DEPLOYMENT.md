# KafScale Deployment Guide

Three deployment modes for getting KafScale running on a remote server or locally.

## Overview

| Mode | Name | Best for | What it does |
|------|------|----------|--------------|
| **A** | Full Distribution | Dev/staging with full build infra | rsync entire repo, build on remote via `make` |
| **B** | Containerized | Production-like with Docker | Sync docker-compose files, `docker compose up` |
| **C** | Binary + Scripts | Bare-metal, no Docker on target | Cross-compile Go binaries, deploy with systemd/shell |

## Prerequisites

### All modes
- SSH key-based auth to the target host (`ssh-copy-id user@host`)
- `rsync` installed locally and on the remote

### Mode A
- `make`, `docker`, `docker compose` on the **remote** host
- Full repo access (run from repo root)

### Mode B
- `docker`, `docker compose` on the **remote** host
- Container images accessible (local registry or pre-built)

### Mode C
- `go` installed **locally** (for cross-compilation)
- `etcd` installed on the **remote** (or use Docker for etcd)
- S3-compatible storage accessible from remote (MinIO, AWS S3, etc.)

## Quick Start

```bash
# Deploy full repo to a server
/deploy 192.168.1.50

# Docker-compose deploy
/deploy 192.168.1.50 --mode B

# Binary deploy
/deploy 192.168.1.50 --mode C

# Dry run (show plan without executing)
/deploy 192.168.1.50 --mode A --dry-run
```

Or invoke the script directly:

```bash
bash ~/.claude/scripts/kafscale-deploy/deploy.sh 192.168.1.50 --mode A
```

---

## Mode A — Full Distribution

Syncs the entire repository to the remote host and runs `make` targets to build and bootstrap.

### What happens

1. `rsync` the repo (excluding build artifacts per `rsync-exclude.txt`) to `~/kafscale/` on the remote
2. `ssh` into remote, run `make docker-build` then `make demo-platform-bootstrap`

### Walkthrough

```bash
# Preview what will be synced
/deploy 192.168.1.50 --mode A --dry-run

# Full deploy
/deploy 192.168.1.50 --mode A

# Clean deploy (wipe remote dir first)
/deploy 192.168.1.50 --mode A --clean
```

### Services started
- **etcd**: metadata store (via kind/docker)
- **MinIO**: S3 storage
- **Broker**: KafScale broker on `:9092`
- **LFS Proxy**: HTTP on `:8080`, Kafka on `:9093`
- **Console**: Web UI on `:3080`

---

## Mode B — Containerized

Deploys using `docker compose` with pre-built or remotely-built container images.

### What happens (default)

1. `rsync` the `deploy/docker-compose/` directory to remote
2. `ssh` into remote, run `docker compose up -d`

### What happens (with `--build-on-remote`)

1. `rsync` the full repo to remote
2. `ssh` into remote, run `make docker-build`
3. `docker compose up -d`

### Walkthrough

```bash
# Deploy with pre-built images
/deploy 192.168.1.50 --mode B

# Build images on remote first
/deploy 192.168.1.50 --mode B --build-on-remote

# Manage running deployment
/deploy 192.168.1.50 --mode B --action status
/deploy 192.168.1.50 --mode B --action logs
/deploy 192.168.1.50 --mode B --action restart
/deploy 192.168.1.50 --mode B --action down
```

### Container image registry

By default, images are pulled from the registry configured in `docker-compose.yaml` (`192.168.0.131:5100`). Override with:

```bash
REGISTRY=myregistry.example.com TAG=v1.0 /deploy 192.168.1.50 --mode B
```

---

## Mode C — Binary + Scripts

Cross-compiles Go binaries locally and deploys them with systemd units or shell scripts.

### What happens

1. Cross-compile `broker`, `lfs-proxy`, `console` for `linux/amd64` (or `arm64`)
2. Render systemd unit and shell script templates
3. `rsync` binaries, rendered templates, and env file to remote
4. Install systemd units (or fall back to shell start script)

### Walkthrough

```bash
# Default (amd64, core binaries)
/deploy 192.168.1.50 --mode C

# ARM64 target
/deploy 192.168.1.50 --mode C --arch arm64

# Include all binaries (operator, proxy, mcp)
/deploy 192.168.1.50 --mode C --all-binaries

# Dry run
/deploy 192.168.1.50 --mode C --dry-run
```

### Remote directory layout

```
~/kafscale/
  bin/
    broker
    lfs-proxy
    console
    start-kafscale.sh
    stop-kafscale.sh
  etc/
    kafscale.env
    systemd/
      kafscale-broker.service
      kafscale-lfs-proxy.service
      kafscale-console.service
      kafscale-etcd.service
  var/
    run/          # PID files
    log/          # Log files
    data/         # Broker data
```

### Managing services

With systemd:
```bash
ssh user@host "sudo systemctl status kafscale-broker"
ssh user@host "sudo journalctl -u kafscale-broker -f"
```

With shell scripts:
```bash
ssh user@host "~/kafscale/bin/start-kafscale.sh"
ssh user@host "~/kafscale/bin/stop-kafscale.sh"
```

---

## Local Testing

Test deployment modes locally without a remote host.

```bash
/deploy local --mode A    # kind cluster bootstrap
/deploy local --mode B    # docker compose up
/deploy local --mode C    # native binaries + background procs
```

### Mode B local

```bash
/deploy local --mode B
# Verify:
curl http://localhost:9094/readyz    # LFS Proxy health
curl http://localhost:3080/health    # Console health
```

### Mode C local

Starts etcd and MinIO in Docker, then runs KafScale binaries as background processes. Press Ctrl+C to stop all services and clean up.

```bash
/deploy local --mode C
# Services run in foreground, Ctrl+C to stop
```

---

## Environment Variables

All configuration is done via environment variables. See `deploy/templates/kafscale.env.template` for the full reference.

### Key variables

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `KAFSCALE_BROKER_ADDR` | Broker | `:9092` | Broker listen address |
| `KAFSCALE_BROKER_ETCD_ENDPOINTS` | Broker | `http://localhost:2379` | etcd endpoints |
| `KAFSCALE_S3_ENDPOINT` | Broker | `http://localhost:9000` | S3 endpoint for blob storage |
| `KAFSCALE_S3_BUCKET` | Broker | `kafscale` | S3 bucket name |
| `KAFSCALE_LFS_PROXY_HTTP_ADDR` | LFS Proxy | `:8080` | HTTP API listen address |
| `KAFSCALE_LFS_PROXY_BACKENDS` | LFS Proxy | `localhost:9092` | Upstream broker(s) |
| `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE` | LFS Proxy | `7516192768` | Max blob size (7GB) |
| `KAFSCALE_CONSOLE_HTTP_ADDR` | Console | `:3080` | Console web UI address |
| `KAFSCALE_UI_USERNAME` | Console | `kafscaleadmin` | Console login username |
| `KAFSCALE_UI_PASSWORD` | Console | `kafscale` | Console login password |

### Deploy-time variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DEPLOY_USER` | `$(whoami)` | SSH user for remote deployment |
| `REMOTE_DIR` | `~/kafscale` | Installation directory on remote |
| `SSH_OPTS` | (see deploy-env.sh) | Extra SSH options |

---

## Troubleshooting

### SSH connection fails

```
[ERROR] Cannot connect to 192.168.1.50 via SSH.
```

- Verify key-based auth: `ssh user@192.168.1.50 "echo ok"`
- Copy your SSH key: `ssh-copy-id user@192.168.1.50`
- The script uses `BatchMode=yes` — no password prompts

### rsync not found

```
[ERROR] rsync is not installed.
```

- macOS: `brew install rsync`
- Ubuntu: `sudo apt install rsync`
- Also needs to be installed on the remote host

### Mode C: Go not installed

```
[ERROR] Go is not installed.
```

- Install Go: https://go.dev/dl/
- Verify: `go version`

### Services won't start on remote

1. Check logs: `ssh user@host "cat ~/kafscale/var/log/broker.log"`
2. Verify etcd is running: `ssh user@host "etcdctl endpoint health"`
3. Verify S3/MinIO is accessible from remote
4. Check env file: `ssh user@host "cat ~/kafscale/etc/kafscale.env"`

### Ports already in use

Default ports: 9092 (broker), 8080/9093/9094/9095 (LFS proxy), 3080 (console), 2379 (etcd), 9000 (MinIO).

Check what's using a port:
```bash
ssh user@host "ss -tlnp | grep 9092"
```
