#!/usr/bin/env bash
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <tag> <sha256sums-file> <formula-path>" >&2
  exit 1
fi

tag="$1"
sums="$2"
out="$3"

if [[ ! "$tag" =~ ^v[0-9][A-Za-z0-9._-]*$ ]]; then
  echo "Invalid release tag: $tag" >&2
  exit 1
fi

version="${tag#v}"
base="https://github.com/KafScale/platform/releases/download/${tag}"

sum_for() {
  local name="$1"
  local line hash
  line="$(grep "  ${name}$" "$sums" || true)"
  hash="${line%%  *}"
  if [[ ! "$hash" =~ ^[0-9a-f]{64}$ ]]; then
    echo "missing or invalid sha256 for $name" >&2
    exit 1
  fi
  printf '%s' "$hash"
}

darwin_amd64="kafscale-cli_${tag}_darwin_amd64.tar.gz"
darwin_arm64="kafscale-cli_${tag}_darwin_arm64.tar.gz"
linux_amd64="kafscale-cli_${tag}_linux_amd64.tar.gz"
linux_arm64="kafscale-cli_${tag}_linux_arm64.tar.gz"

sha_darwin_amd64="$(sum_for "$darwin_amd64")"
sha_darwin_arm64="$(sum_for "$darwin_arm64")"
sha_linux_amd64="$(sum_for "$linux_amd64")"
sha_linux_arm64="$(sum_for "$linux_arm64")"

mkdir -p "$(dirname "$out")"
cat > "$out" <<RUBY
# typed: false
# frozen_string_literal: true

class KafscaleCli < Formula
  desc "Command line tool for KafScale"
  homepage "https://kafscale.io"
  version "${version}"
  license "Apache-2.0"

  on_macos do
    if Hardware::CPU.intel?
      url "${base}/${darwin_amd64}"
      sha256 "${sha_darwin_amd64}"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
    if Hardware::CPU.arm?
      url "${base}/${darwin_arm64}"
      sha256 "${sha_darwin_arm64}"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
  end

  on_linux do
    if Hardware::CPU.intel? && Hardware::CPU.is_64_bit?
      url "${base}/${linux_amd64}"
      sha256 "${sha_linux_amd64}"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
    if Hardware::CPU.arm? && Hardware::CPU.is_64_bit?
      url "${base}/${linux_arm64}"
      sha256 "${sha_linux_arm64}"

      define_method(:install) do
        bin.install "kafscale-cli"
      end
    end
  end

  test do
    system "#{bin}/kafscale-cli", "--version"
  end
end
RUBY
