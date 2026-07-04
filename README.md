Lakehouse Platform
Unified Streaming + Iceberg Architecture
Author: ashtelmah

MS SQL → Debezium → Kafka (KRaft, Avro) → Flink 2.0 (Dynamic Sink) → Iceberg 1.11 (Nessie + MinIO) → Trino

1. Streaming Layer
This layer ingests data from databases and publishes structured events into Kafka.

Kafka (KRaft mode)
Modern Kafka without ZooKeeper

Broker + Controller in one node

Ports: 9092, 29092, 29093

Storage: /opt/data/streaming/kafka-data

Schema Registry
Stores Avro schemas

Ensures compatibility for Debezium and Flink

Debezium Connect
CDC ingestion from relational databases

Avro converters

Plugins mounted from /opt/data/streaming/plugins

Redpanda Console
UI for Kafka topics, schemas, Debezium connectors

2. Iceberg Layer
This layer stores data in Iceberg tables with full ACID guarantees.

MinIO (S3 storage)
S3 endpoints: 9000, 9001

Stores Iceberg tables, Flink checkpoints, savepoints

Postgres (Nessie metadata)
Metadata backend for Nessie

Nessie Catalog
Git‑like versioning of Iceberg tables

API: 19120

UI: 9009

Trino (SQL engine)
Reads Iceberg V2 tables

Supports time‑travel queries

Port: 8082

Flink 2.x (JobManager + TaskManager)
Streaming runtime

SinkV2 → Iceberg dynamic routing

Checkpoints/savepoints stored in MinIO

Plugins in /opt/flink/lib

JobManager port: 8085

Flink SQL Gateway
REST SQL endpoint: 8087

Executes Iceberg SQL

Init Runner
Automatically initializes catalogs, tables, functions

Runs SQL scripts on startup

3. Directory Layout
Код
/opt/data/
│
├── streaming/      # Kafka (KRaft), Schema Registry, Debezium
│
├── iceberg/        # Flink 2.x, SQL Gateway, MinIO, Nessie, Trino
│
├── dwh/            # Airflow (future)
│
└── superset/       # BI (future)
Author
ashtelmah — Lakehouse Architect
