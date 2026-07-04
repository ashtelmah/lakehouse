# <img src="https://www.apache.org/foundation/press/kit/feather.svg" width="40"> Lakehouse Platform

### Unified Streaming + Iceberg Architecture

MS SQL → Debezium → Kafka (KRaft, Avro) → Flink 2.0 (Dynamic Sink) → Iceberg 1.11 (Nessie + MinIO) → Trino

---
| **[Streaming Layer](ca://s?q=Streaming_Layer_details)** | **[Iceberg Layer](ca://s?q=Iceberg_Layer_details)** |
| --- | --- |
| **Kafka (KRaft mode)** — modern Kafka without ZooKeeper; broker + controller; ports 9092/29092/29093 | **MinIO (S3 storage)** — endpoints 9000/9001; stores Iceberg tables, checkpoints |
| **Schema Registry** — Avro schemas; compatibility for Debezium & Flink | **Postgres (Nessie metadata)** — metadata backend for Nessie |
| **Debezium Connect** — CDC ingestion; Avro converters; plugins in ``/opt/data/streaming/plugins`` | **Nessie Catalog** — Git‑like versioning; API 19120; UI 9009 |
| **Redpanda Console** — UI for Kafka topics, schemas, connectors | **Trino (SQL engine)** — reads Iceberg V2; time‑travel; port 8082 |
| **MS SQL → Debezium → Kafka** — full CDC pipeline | **Flink 2.x (JM/TM)** — SinkV2 → Iceberg; dynamic routing; checkpoints in MinIO |
|  | **Flink SQL Gateway** — REST SQL endpoint 8087; executes Iceberg SQL |
|  | **Init Runner** — initializes catalogs, tables, functions |

---
📁 /opt/data/

├── streaming/      # Kafka (KRaft), Schema Registry, Debezium

├── iceberg/        # Flink 2.x, SQL Gateway, MinIO, Nessie, Trino

├── dwh/            # Airflow (future)

└── superset/       # BI (future)

---
## Author

👤 **[ashtelmah](https://github.com/ashtelmah)**  
*Lakehouse Architect*


