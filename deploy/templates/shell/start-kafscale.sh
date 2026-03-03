#!/usr/bin/env bash
# start-kafscale.sh — Start KafScale services using nohup + PID files
set -euo pipefail

KAFSCALE_DIR="{{REMOTE_DIR}}"
VAR_DIR="$KAFSCALE_DIR/var"
RUN_DIR="$VAR_DIR/run"
LOG_DIR="$VAR_DIR/log"
DATA_DIR="$VAR_DIR/data"
BIN_DIR="$KAFSCALE_DIR/bin"
ENV_FILE="$KAFSCALE_DIR/etc/kafscale.env"

mkdir -p "$RUN_DIR" "$LOG_DIR" "$DATA_DIR"

# Load environment
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +a
fi

start_service() {
    local name="$1"
    local binary="$BIN_DIR/$name"
    local pid_file="$RUN_DIR/$name.pid"
    local log_file="$LOG_DIR/$name.log"

    if [[ ! -x "$binary" ]]; then
        echo "[WARN] Binary not found or not executable: $binary"
        return 1
    fi

    # Check if already running
    if [[ -f "$pid_file" ]]; then
        local existing_pid
        existing_pid=$(cat "$pid_file")
        if kill -0 "$existing_pid" 2>/dev/null; then
            echo "[INFO] $name already running (PID $existing_pid)"
            return 0
        fi
        rm -f "$pid_file"
    fi

    echo "[INFO] Starting $name..."
    nohup "$binary" >> "$log_file" 2>&1 &
    echo $! > "$pid_file"
    echo "[INFO] $name started (PID $(cat "$pid_file"))"
}

echo "==> Starting KafScale services..."
echo ""

# Start in dependency order
start_service "broker"
sleep 2
start_service "lfs-proxy"
sleep 1
start_service "console"

echo ""
echo "==> All services started."
echo "    Logs: $LOG_DIR/"
echo "    PIDs: $RUN_DIR/"
echo ""
echo "    Broker:    :9092"
echo "    LFS Proxy: :8080 (HTTP), :9093 (Kafka)"
echo "    Console:   :3080"
