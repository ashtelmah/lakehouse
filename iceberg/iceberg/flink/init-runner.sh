#!/bin/bash
set -e

# ------------------------------------------------------------
# Start the Flink SQL Gateway in the background
# ------------------------------------------------------------
echo "[init-runner] Starting SQL Gateway..."
nohup /opt/flink/bin/sql-gateway.sh start-foreground > /opt/flink/log/gateway.log 2>&1 &
GATEWAY_PID=$!

# ------------------------------------------------------------
# Wait until the SQL Gateway REST API becomes available
# ------------------------------------------------------------
echo "[init-runner] Waiting for SQL Gateway REST API..."
until curl -s http://127.0.0.1:8083/v1/sessions > /dev/null; do
  sleep 1
done


# ------------------------------------------------------------
# Create a dedicated initialization session
# ------------------------------------------------------------
echo "[init-runner] Creating init session..."
SESSION=$(curl -s -X POST http://127.0.0.1:8083/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"sessionName": "init"}' \
  | sed -E 's/.*"sessionHandle":"([^"]+)".*/\1/')

echo "[init-runner] Session ID: $SESSION"

# ------------------------------------------------------------
# Execute init.sql line-by-line, accumulating statements
# until a semicolon is reached
# ------------------------------------------------------------
echo "[init-runner] Executing init.sql statement-by-statement..."

BUFFER=""

while IFS= read -r line; do
  # Skip empty lines
  [[ -z "$line" ]] && continue

  # Append line to buffer
  BUFFER="${BUFFER}${line} "

  # When a semicolon is found, treat it as end of SQL statement
  if [[ "$line" == *";" ]]; then
    # Escape double quotes for JSON payload
    STATEMENT=$(echo "$BUFFER" | sed 's/"/\\"/g')

    echo "[init-runner] Executing statement: $STATEMENT"

    # Send the SQL statement to the SQL Gateway REST API
    curl -s -X POST http://127.0.0.1:8083/v1/sessions/$SESSION/statements \
      -H "Content-Type: application/json" \
      -d "{\"statement\": \"$STATEMENT\"}" > /dev/null

    # Reset buffer for next statement
    BUFFER=""
  fi
done < /opt/flink/conf/init.sql

# ------------------------------------------------------------
# Initialization complete — hand control back to SQL Gateway
# ------------------------------------------------------------
echo "[init-runner] Init complete. Handing over to SQL Gateway."
wait $GATEWAY_PID

