Опис автозапуску SQL для Flink SQL Gateway
🎯 Призначення

Файл init.sql виконується автоматично при старті Flink SQL Gateway через init-runner.sh.
Він створює:

    Iceberg каталог (iceberg_catalog)

    Kafka universal source (kafka_all_src)

Це забезпечує автоматичну готовність Flink до ingestion Kafka → Iceberg.
📌 Що робить init.sql
1. Створює Iceberg каталог (якщо не існує)

Каталог зберігає:

    метадані в Nessie

    файли в MinIO

2. Переключається в default_catalog

Тому що Kafka‑таблиці не можуть бути створені в Iceberg каталозі.
3. Створює універсальну Kafka таблицю

kafka_all_src читає всі топіки:
Code

_.DataPlatform.dbo.*

і автоматично підтягує Avro схеми з Schema Registry.
📌 Як працює init-runner.sh

    чекає поки SQL Gateway підніметься

    створює сесію

    читає init.sql построчно

    виконує кожен SQL через REST API

    пропускає пусті рядки

    не падає при повторному запуску

📌 Чому не можна чіпати conf/

Flink SQL Gateway 2.1.2:

    не підтримує init.sql у conf/

    не виконує SQL при старті

    не має параметра --init-file

    ігнорує sql-gateway-defaults.yaml для DDL

Єдиний робочий спосіб — REST API + init-runner.

