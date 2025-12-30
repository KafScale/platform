#!/bin/sh
set -eu

CONFIG_PATH=${1:-config/config.yaml}

go run ./cmd/processor -config "$CONFIG_PATH"
