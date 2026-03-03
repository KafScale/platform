#!/usr/bin/env bash
# stop-kafscale.sh — Stop KafScale services using PID files
set -euo pipefail

KAFSCALE_DIR="{{REMOTE_DIR}}"
RUN_DIR="$KAFSCALE_DIR/var/run"

stop_service() {
    local name="$1"
    local pid_file="$RUN_DIR/$name.pid"

    if [[ ! -f "$pid_file" ]]; then
        echo "[INFO] $name: no PID file (not running)"
        return 0
    fi

    local pid
    pid=$(cat "$pid_file")

    if kill -0 "$pid" 2>/dev/null; then
        echo "[INFO] Stopping $name (PID $pid)..."
        kill "$pid"
        # Wait up to 10 seconds for graceful shutdown
        local count=0
        while kill -0 "$pid" 2>/dev/null && [[ $count -lt 10 ]]; do
            sleep 1
            count=$((count + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "[WARN] $name didn't stop gracefully, sending SIGKILL..."
            kill -9 "$pid" 2>/dev/null || true
        fi
        echo "[INFO] $name stopped."
    else
        echo "[INFO] $name: process $pid not running."
    fi

    rm -f "$pid_file"
}

echo "==> Stopping KafScale services..."
echo ""

# Stop in reverse order
stop_service "console"
stop_service "lfs-proxy"
stop_service "broker"

echo ""
echo "==> All services stopped."
