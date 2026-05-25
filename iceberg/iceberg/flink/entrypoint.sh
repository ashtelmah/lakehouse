#!/bin/bash
set -e

echo "[entrypoint] Starting Flink SQL Gateway..."
/opt/flink/bin/sql-gateway.sh start-foreground &
GATEWAY_PID=$!

# Wait for REST API
echo "[entrypoint] Waiting for SQL Gateway REST API on :8083..."
RETRIES=30
until curl -s http://localhost:8083/v1/sessions > /dev/null; do
  RETRIES=$((RETRIES-1))
  if [ $RETRIES -le 0 ]; then
    echo "[entrypoint] ERROR: SQL Gateway REST API did not start in time."
    exit 1
  fi
  sleep 1
done

echo "[entrypoint] SQL Gateway REST API is ready."

# Create session
echo "[entrypoint] Creating session..."
SESSION_RESPONSE=$(curl -s -X POST http://localhost:8083/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"sessionName": "init"}')

SESSION_ID=$(echo "$SESSION_RESPONSE" | sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')

if [ -z "$SESSION_ID" ]; then
  echo "[entrypoint] ERROR: Failed to create session."
  echo "Response: $SESSION_RESPONSE"
  exit 1
fi

echo "[entrypoint] Session created: $SESSION_ID"

# Load SQL file
if [ ! -f /opt/flink/conf/init.sql ]; then
  echo "[entrypoint] ERROR: init.sql not found!"
  exit 1
fi

SQL=$(tr '\n' ' ' < /opt/flink/conf/init.sql)

echo "[entrypoint] Executing init.sql..."
EXEC_RESPONSE=$(curl -s -X POST http://localhost:8083/v1/sessions/$SESSION_ID/statements \
  -H "Content-Type: application/json" \
  -d "{\"statement\": \"$SQL\"}")

echo "[entrypoint] init.sql execution response:"
echo "$EXEC_RESPONSE"

echo "[entrypoint] Initialization complete. Attaching to SQL Gateway logs..."
wait $GATEWAY_PID
