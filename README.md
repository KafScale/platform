# Kafscale

See `kscale-spec.md` for the full architecture and implementation blueprint. This repository currently contains the project scaffolding:

- `cmd/` – binary entrypoints (broker, operator)
- `pkg/` – shared Go libraries (protocol parsing, storage, metadata, broker internals, caches, operator helpers)
- `proto/` – protobuf definitions for metadata + control-plane RPCs
- `deploy/` – dockerfiles and Helm chart
- `config/` – CRDs, RBAC, and sample manifests
- `docs/` – user-facing (`user-guide.md`), developer (`development.md`), storage + S3 design notes (`storage.md`)
- `test/` – integration and end-to-end suites

Everything else should follow the structure laid out in the spec. Start with `docs/user-guide.md` if you're trying to use the platform, or `docs/development.md` if you're contributing.
