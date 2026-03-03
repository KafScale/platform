# KafScale Deployment Templates

Canonical, version-controlled templates used by the KafScale deployment scripts.

## Contents

| Path | Description |
|------|-------------|
| `systemd/kafscale-broker.service` | Systemd unit for the KafScale broker |
| `systemd/kafscale-lfs-proxy.service` | Systemd unit for the LFS proxy |
| `systemd/kafscale-console.service` | Systemd unit for the web console |
| `systemd/kafscale-etcd.service` | Systemd unit for etcd (metadata store) |
| `shell/start-kafscale.sh` | Shell script to start all services (nohup + PID files) |
| `shell/stop-kafscale.sh` | Shell script to stop all services gracefully |
| `kafscale.env.template` | Environment variable template with all config options |

## Placeholders

Templates use these placeholders, rendered at deploy time:

- `{{REMOTE_DIR}}` — Installation directory on the target host (default: `~/kafscale`)
- `{{DEPLOY_USER}}` — OS user running the services (default: current user)

## Usage

These templates are consumed by the deployment script (`~/.claude/scripts/kafscale-deploy/deploy.sh`) in Mode C (Binary + Scripts). See `deploy/DEPLOYMENT.md` for full documentation.
