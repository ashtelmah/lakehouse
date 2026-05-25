#!/bin/sh

GATEWAY="http://flink-sql-gateway:8087/v1"

echo "[init-runner] Waiting for Flink SQL Gateway..."
until curl -s "$GATEWAY/sessions" > /dev/null; do
  sleep 2
done
echo "[init-runner] Flink SQL Gateway is UP."

# Create session
SESSION=$(curl -s -X POST "$GATEWAY/sessions" \
  -H "Content-Type: application/json" \
  -d '{"sessionName": "init"}' \
  | sed -n 's/.*"sessionHandle":"\([^"]*\)".*/\1/p')

echo "[init-runner] Session created: $SESSION"

# Execute SQL with polling
run_sql() {
  SQL="$1"
  echo "[init-runner] Executing: $SQL"

  ESCAPED=$(printf "%s" "$SQL" | sed 's/"/\\"/g')

  # 1. submit
  SUBMIT=$(curl -s -X POST "$GATEWAY/sessions/$SESSION/statements" \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"$ESCAPED\"}")

  OP=$(printf "%s" "$SUBMIT" | sed -n 's/.*"operationHandle":"\([^"]*\)".*/\1/p')

  if [ -z "$OP" ]; then
    echo "[init-runner] ❌ No operationHandle"
    echo "[init-runner] SUBMIT: $SUBMIT"
    exit 1
  fi

  echo "[init-runner] Operation: $OP"

  # 2. poll
  for i in $(seq 1 20); do
    RESP=$(curl -s "$GATEWAY/sessions/$SESSION/operations/$OP/status")
    echo "[init-runner] RESPONSE $i: $RESP"

    STATUS=$(printf "%s" "$RESP" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')

    if [ "$STATUS" = "FINISHED" ]; then
      echo "[init-runner] ✅ SUCCESS (status: $STATUS)"
      return 0
    fi

    if [ "$STATUS" = "ERROR" ]; then
      echo "[init-runner] ❌ ERROR"
      exit 1
    fi

    sleep 1
  done

  echo "[init-runner] ⏰ TIMEOUT for operation $OP"
  exit 1
}

# Read init.sql
BUFFER=""
trim() { printf "%s" "$1" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'; }

while IFS= read -r line; do
  CLEAN=$(trim "$line")

  # Skip empty lines
  [ -z "$CLEAN" ] && continue

  # Skip SQL comments
  case "$CLEAN" in
    --*) continue ;;
  esac

  BUFFER="$BUFFER $CLEAN"

  case "$CLEAN" in
    *\; )
      run_sql "$BUFFER"
      BUFFER=""
      ;;
  esac
done < /opt/flink/init/init.sql

echo "[init-runner] Init SQL completed successfully."
