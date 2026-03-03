#!/usr/bin/env bash
set -euo pipefail

STAGE_REGISTRY="${STAGE_REGISTRY:-192.168.0.131:5100}"
STAGE_TAG="${STAGE_TAG:-stage}"
STAGE_PLATFORMS="${STAGE_PLATFORMS:-linux/amd64,linux/arm64}"
STAGE_NO_CACHE="${STAGE_NO_CACHE:-1}"

if [[ -z "${STAGE_REGISTRY}" ]]; then
  echo "STAGE_REGISTRY must be set" >&2
  exit 1
fi

if [[ -z "${STAGE_TAG}" ]]; then
  echo "STAGE_TAG must be set" >&2
  exit 1
fi

buildkit_config="$(mktemp)"
trap 'rm -f "${buildkit_config}"' EXIT
cat >"${buildkit_config}" <<EOF
[registry."${STAGE_REGISTRY}"]
  http = true
  insecure = true
EOF

if docker buildx inspect stage-release-builder >/dev/null 2>&1; then
  docker buildx rm stage-release-builder >/dev/null 2>&1 || true
fi

docker buildx create --name stage-release-builder --use --config "${buildkit_config}" >/dev/null

declare -a images=(
  "kafscale-broker|.|deploy/docker/broker.Dockerfile|"
  "kafscale-lfs-proxy|.|deploy/docker/lfs-proxy.Dockerfile|"
  "kafscale-operator|.|deploy/docker/operator.Dockerfile|"
  "kafscale-console|.|deploy/docker/console.Dockerfile|"
  "kafscale-etcd-tools|.|deploy/docker/etcd-tools.Dockerfile|"
  "kafscale-iceberg-processor|addons/processors/iceberg-processor|addons/processors/iceberg-processor/Dockerfile|--build-arg USE_LOCAL_PLATFORM=1 --build-arg REPO_ROOT=. --build-arg MODULE_DIR=addons/processors/iceberg-processor"
  "kafscale-sql-processor|addons/processors/sql-processor|addons/processors/sql-processor/Dockerfile|"
  "kafscale-e72-browser-demo|examples/E72_browser-lfs-sdk-demo|examples/E72_browser-lfs-sdk-demo/Dockerfile|"
)

iceberg_context_dir=""
cleanup() {
  if [[ -n "${iceberg_context_dir}" ]] && [[ -d "${iceberg_context_dir}" ]]; then
    rm -rf "${iceberg_context_dir}"
  fi
}
trap cleanup EXIT

for entry in "${images[@]}"; do
  IFS="|" read -r image context dockerfile build_args <<<"${entry}"
  if [[ "${image}" == "kafscale-iceberg-processor" ]]; then
    iceberg_context_dir="$(mktemp -d)"
    rsync -a --delete \
      --exclude ".git" \
      --exclude ".dockerignore" \
      --exclude ".build" \
      --exclude ".gocache" \
      --exclude ".idea" \
      --exclude ".vscode" \
      --exclude "_site" \
      --exclude "bin" \
      --exclude "coverage.out" \
      --exclude "dist" \
      --exclude "docs" \
      --exclude "deploy/helm" \
      --exclude "test" \
      --exclude "tmp" \
      --exclude "**/.DS_Store" \
      --exclude "**/*.log" \
      --exclude "**/*.swp" \
      --exclude "**/*_test.go" \
      --exclude "**/node_modules" \
      --exclude "ui/.next" \
      --exclude "ui/dist" \
      --exclude "ui/build" \
      ./ "${iceberg_context_dir}/"
    context="${iceberg_context_dir}"
  fi
  tag="${STAGE_REGISTRY}/kafscale/${image}:${STAGE_TAG}"
  echo "==> Building ${tag}"
  cache_flags=()
  if [[ "${STAGE_NO_CACHE}" == "1" ]]; then
    cache_flags+=(--no-cache --pull)
  fi
  if [[ "${image}" == "kafscale-iceberg-processor" ]]; then
    docker buildx build \
      --platform "${STAGE_PLATFORMS}" \
      --file "${dockerfile}" \
      --tag "${tag}" \
      --build-arg USE_LOCAL_PLATFORM=1 \
      --build-arg REPO_ROOT=. \
      --build-arg MODULE_DIR=addons/processors/iceberg-processor \
      "${cache_flags[@]}" \
      --push \
      "${context}"
    continue
  fi
  docker buildx build \
    --platform "${STAGE_PLATFORMS}" \
    --file "${dockerfile}" \
    --tag "${tag}" \
    ${build_args} \
    "${cache_flags[@]}" \
    --push \
    "${context}"
done
