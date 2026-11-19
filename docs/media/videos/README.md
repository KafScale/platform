<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Persona videos

Terminal recordings made with [VHS](https://github.com/charmbracelet/vhs) at 960x540, font size 21, Dracula theme. Each persona has four files with stable names:

- `<persona>/<persona>.webm` (VP9)
- `<persona>/<persona>.mp4` (H.264, yuv420p, faststart, no audio), the fallback for browsers without WebM
- `<persona>/<persona>-poster.webp`, a still frame for the video poster
- `<persona>/<persona>.gif`, for places that cannot embed video

The names carry no version or size. Docs and the website can link them once, and a new recording replaces the files without any doc change.

## The videos

All five were recorded on `f277cd344f5d5d9e56ea094acbb712e87d07d934` (f277cd3, main, 2026-11-19) by following only the docs listed here. They run on one kind cluster in this order, and each one starts from the state the previous one left.

| Persona | Follows | Length | Poster at |
|---|---|---|---|
| `admin` | `docs/quickstart.md` steps 1 to 6, image tags from `docs/operations.md` (Installing with Helm) | 2:14 | 107.5 s |
| `cli` | `docs/operations.md` (Installing kafscale-cli, Topic recovery on the DR spine, Pruning deleted topic data), `docs/quickstart.md` step 7 | 1:22 | 32 s |
| `schema-registry` | `docs/schema-registry.md` (Running it, Moving schemas into the built in registry), source Apicurio Registry 3.0.6 | 1:11 | 63 s |
| `mcp` | `docs/ops-mcp.md` (Helm, Creating a bearer token, Quick Test) | 1:31 | 85 s |
| `ops` | `docs/operations.md` (Partition leases, S3 credentials secret, Operator-managed etcd, Scaling brokers) | 1:50 | 6.5 s |

Places where the docs did not work as written on f277cd3 are tracked in #389 and #386.

## Re-recording

### Prerequisites

- Docker, [kind](https://kind.sigs.k8s.io/) with `networking.kubeProxyMode: nftables` (the setup script writes that config), kubectl, helm, Go
- [VHS](https://github.com/charmbracelet/vhs) with ttyd, and ffmpeg
- jq, curl, openssl, python3 with venv, npx (Node.js) for the MCP inspector
- the MinIO client `mc`
- this repository checked out at the commit you want to record

Build the images from the checkout. They are tagged `:dev`:

```bash
make docker-build-broker docker-build-operator docker-build-console docker-build-proxy \
  docker-build-mcp docker-build-schema-registry docker-build-etcd-tools docker-build-minio
```

### Work directory and secrets

The scripts in `tapes/prep/` share `tapes/prep/rc.sh`. It sets `KUBECONFIG`, `PATH`, the prompt and a `platform` link to the checkout, and works in `KSV_DIR` (default `/tmp/kafscale-videos`). Other settings: `KSV_CLUSTER` (kind cluster name, default `kafscale-videos`) and `KSV_SUDO` (set it to `sudo -n` if docker and kind need sudo).

No secret is stored in the repository or typed on screen. `tapes/prep/secrets.sh` creates them with `umask 077` in `/tmp/kafscale-videos/secrets` (mode 700, files mode 600): random MinIO root credentials, the S3 secret keys for `kafscale-demo` and `kafscale-demo-2`, the console password, and two VHS files, `s3-secret.tape` and `console-pass.tape`, each one line of the form `Type@5ms "<value>"`. Existing files are kept, so you can write your own values into them first:

```bash
umask 077; mkdir -p /tmp/kafscale-videos/secrets
read -rs v; printf '%s' "$v" > /tmp/kafscale-videos/secrets/s3-secret; unset v
```

Where a doc block reads a secret with `read -rs`, the tape hides the screen and replays the matching `.tape` file with VHS `Source`. `Source` takes a literal path, so the admin and cli tapes point at `/tmp/kafscale-videos/secrets`. If you move the secrets, change those `Source` lines too. MinIO users are managed with `mc` through a `MC_HOST_` environment variable and the secret on stdin, never on the command line.

### Steps

Run everything from the repository root, in this order. Steps that only wait, or start a port-forward, are hidden in the tapes.

```bash
docs/media/videos/tapes/prep/setup.sh
vhs docs/media/videos/tapes/admin.tape
docs/media/videos/tapes/prep/before-cli.sh
vhs docs/media/videos/tapes/cli.tape
docs/media/videos/tapes/prep/before-schema-registry.sh
vhs docs/media/videos/tapes/schema-registry.tape
docs/media/videos/tapes/prep/before-mcp.sh
vhs docs/media/videos/tapes/mcp.tape
docs/media/videos/tapes/prep/before-ops.sh
vhs docs/media/videos/tapes/ops.tape
```

What the prep scripts do:

- `setup.sh`: creates the secrets, copies the demo files from `prep/demo/` to `$KSV_DIR/demo` (without the license header, since the admin tape shows them), creates the kind cluster, loads the images, runs MinIO from `prep/minio.yaml` with root credentials from a Secret, adds the MinIO user `kafscale-demo`, and makes a python venv with `confluent-kafka`.
- `before-cli.sh`: makes sure the `kcat` pod runs, adds 30 records to `orders`, and port-forwards etcd to `:2379` and MinIO to `:9000`.
- `before-schema-registry.sh`: starts Apicurio Registry 3.0.6 on `127.0.0.1:8085` and seeds it with `demo/seed_apicurio.py`.
- `before-mcp.sh`: commits offsets for the consumer group `billing-worker` and starts light produce traffic.
- `before-ops.sh`: stops the traffic, writes the second key pair to `$KSV_DIR/new-s3/` (mode 600), adds the MinIO user `kafscale-demo-2`, and port-forwards `demo-broker-0:9093`.
- `teardown.sh`: stops the port-forwards and Apicurio and deletes the kind cluster.

Each tape writes the GIF, WebM and MP4. Then make the web MP4 (re-encoded from the WebM with `+faststart`) and the poster:

```bash
docs/media/videos/tapes/prep/media.sh admin 107.5
```

Pick a poster time that shows a result and nothing secret.
