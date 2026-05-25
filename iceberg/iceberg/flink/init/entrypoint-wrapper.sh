#!/bin/bash
set -e

echo "[wrapper] Running Flink entrypoint to apply FLINK_PROPERTIES..."
/docker-entrypoint.sh bash -c "echo '[wrapper] Flink environment initialized.'"

echo "[wrapper] Starting SQL Gateway manually..."
/opt/flink/bin/sql-gateway.sh start-foreground &

GATEWAY_PID=$!

echo "[wrapper] Waiting for SQL Gateway REST API..."
until curl -s http://127.0.0.1:8087/v1/sessions > /dev/null; do
  sleep 1
done

echo "[wrapper] REST API is up. Running init-runner..."
/opt/flink/init/init-runner.sh &

wait $GATEWAY_PID
