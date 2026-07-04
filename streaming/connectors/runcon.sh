#!/bin/bash

MODE=$1

# Видаляємо старий конектор
curl -X DELETE http://localhost:8083/connectors/Himalia

# Чекаємо 2 секунди
sleep 2

if [ "$MODE" == "snap" ]; then
    echo "Запускаю SNAPSHOT конектор..."
    curl -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @snapms.json
elif [ "$MODE" == "cdc" ]; then
    echo "Запускаю CDC конектор..."
    curl -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @cdcms.json
else
    echo "Вкажи snap або cdc"
fi
