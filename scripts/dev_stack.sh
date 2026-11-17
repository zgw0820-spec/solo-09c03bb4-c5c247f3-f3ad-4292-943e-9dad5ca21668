#!/usr/bin/env bash
# Restart the local (non-Docker) end-to-end stack used during development.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$HOME/.local/bin:$PATH"
REDIS_PORT="${REDIS_PORT:-6390}"
Q1_PORT="${Q1_PORT:-8101}"
Q2_PORT="${Q2_PORT:-8102}"
ORIGIN_PORT="${ORIGIN_PORT:-9100}"
LOG_DIR="${LOG_DIR:-/tmp/e2e}"
mkdir -p "$LOG_DIR"

pkill -f 'uvicorn app.main:app' >/dev/null 2>&1 || true
pkill -f 'uvicorn origin.main:app' >/dev/null 2>&1 || true
sleep 1

if ! redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
  redis-server --port "$REDIS_PORT" --save '' --appendonly no \
    --daemonize yes --pidfile "$LOG_DIR/redis.pid" --logfile "$LOG_DIR/redis.log"
fi

cd "$ROOT"
INSTANCE_NAME=origin ORIGIN_SEED='v1.0.0=sha256:old1111' \
  setsid nohup uvicorn origin.main:app --host 127.0.0.1 --port "$ORIGIN_PORT" --workers 1 \
  >"$LOG_DIR/origin.log" 2>&1 </dev/null &
sleep 1

for n in 1 2; do
  port_var="Q${n}_PORT"
  port="${!port_var}"
  INSTANCE_NAME="query-$n" \
    REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0" \
    ORIGIN_URL="http://127.0.0.1:${ORIGIN_PORT}" \
    PUBLIC_INSTANCES="http://127.0.0.1:${Q1_PORT},http://127.0.0.1:${Q2_PORT}" \
    setsid nohup uvicorn app.main:app --host 127.0.0.1 --port "$port" --workers 1 \
    >"$LOG_DIR/q$n.log" 2>&1 </dev/null &
done

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${Q1_PORT}/api/health" >/dev/null 2>&1 \
    && curl -fsS "http://127.0.0.1:${Q2_PORT}/api/health" >/dev/null 2>&1; then
    echo "stack ready: query-1=$Q1_PORT query-2=$Q2_PORT origin=$ORIGIN_PORT redis=$REDIS_PORT"
    exit 0
  fi
  sleep 0.5
done
echo "stack did not become ready; see $LOG_DIR/*.log" >&2
exit 1
