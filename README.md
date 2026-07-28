
# <img src="https://github.com/ashtelmah/lakehouse/blob/main/assets/icons/feather.svg" width="40"> Lakehouse Platform

### Unified Streaming + Iceberg Architecture

## MS SQL → Debezium → Kafka (KRaft, Avro) → Apache Flink 2.x (Dynamic CDC Sink) → Iceberg 1.11 (Nessie + MinIO) → Trino
A production-style Modern Data Lakehouse built entirely on open-source technologies.

The platform demonstrates an end-to-end CDC pipeline that captures changes from Microsoft SQL Server, processes them in real time, automatically synchronizes schema evolution, and stores analytics-ready data in Apache Iceberg.

---

# Architecture

```text
                      MS SQL Server
                            │
                            ▼
                        Debezium CDC
                            │
                            ▼
             Kafka (KRaft) + Schema Registry
                            │
                            ▼
                     BronzeDecode Job
             (Avro → Bronze JSON → KafkaBronze)
                            │
                            ▼
        ┌──────────────────────────────────────────────┐
        │                                              │
        ▼                                              ▼
    Schema Sync Job                               FSilver Job
 (DDL & Schema Evolution)                  (UPSERT → Iceberg)
        │                                              │
        └──────────────────────┬───────────────────────┘
                               ▼
                 Iceberg Catalog (Nessie + MinIO)
                               │
                               ▼
                           Trino SQL
```

---

# Project Structure

```
📁 /opt/data/
├── streaming/          # Kafka (KRaft), Schema Registry, Debezium
│   └── job/            # BronzeDecode, SyncJob, FsSilver
├── iceberg/            # Flink 2.x, SQL Gateway, MinIO, Nessie, Trino
├── dwh/                # Airflow (future)
└── superset/           # BI (future)
```
---
# Technology Stack & Capabilities
| Layer                | Technology                    | Purpose                                              |
| -------------------- | ----------------------------- | ---------------------------------------------------- |
| Source Database      | Microsoft SQL Server          | Enterprise transactional data source                 |
| Change Data Capture  | Debezium                      | Real-time capture of INSERT / UPDATE / DELETE events |
| Streaming Platform   | Apache Kafka (KRaft)          | Distributed event streaming without ZooKeeper        |
| Serialization        | Apache Avro + Schema Registry | Schema management and data contracts                 |
| Stream Processing    | Apache Flink DataStream API   | Real-time CDC processing and transformations         |
| Table Format         | Apache Iceberg V2             | ACID lakehouse tables with schema evolution          |
| Catalog              | Project Nessie                | Versioned Iceberg metadata management                |
| Object Storage       | MinIO (S3 API)                | Data lake storage layer                              |
| Query Engine         | Trino                         | Interactive SQL analytics                            |
| Language             | Java 17                       | Core implementation language                         |
| Architecture Pattern | Metadata-driven pipeline      | Dynamic multi-table ingestion                        |

---
# Custom Services

| Job | Responsibility | Uses | Why it exists |
| --- | --- | --- | --- |
| **BronzeDecodeJob** | Converts Debezium **Avro** into enriched **Bronze JSON**. Adds metadata (topic/database/schema/table). Writes to ``KafkaBronze``. Supports ``snapshot`` + ``normal`` modes. | KafkaConsumer, KafkaProducer, AvroDecoder | Silver cannot ingest binary Avro. BronzeDecode normalizes CDC into JSON and provides routing metadata for downstream UPSERT. |
| **Schema Sync Job** | Automatic **schema evolution**. Reads Debezium schema changes, compares Avro ↔ Iceberg, applies CREATE/ALTER, manages PK, processes manual ``schema.events``. | SchemaRegistryClient, KafkaConsumer, Flink SQL API | Iceberg schema must always match upstream DB schema. SyncJob ensures deterministic schema evolution and metadata consistency. |
| **FsSilver Job** | Main **UPSERT engine**. Reads Bronze JSON, applies INSERT/UPDATE/DELETE, converts to RowData, writes to Iceberg using SinkV2. One DAG branch per table. | Flink DataStream API, IcebergSinkFactory | Iceberg does not support UPDATE directly. Silver performs equality deletes + inserts and guarantees correct CDC semantics. |

Detailed documentation for each service is available inside its own directory.

---

## Current Capabilities

| Streaming | Lakehouse | Analytics | Infrastructure |
|-----------|-----------|-----------|----------------|
| SQL Server CDC | Iceberg V2 | Trino SQL | Docker |
| Debezium | Schema Evolution | | KRaft |
| Kafka | CDC UPSERT | | MinIO |
| Bronze Layer | Silver Layer | | Nessie |

---

# Repository Guide

| Directory | Description |
|-----------|-------------|
| `streaming/` | Kafka, Debezium, Schema Registry and custom streaming services |
| `iceberg/` | Iceberg infrastructure including Flink, Nessie, MinIO and Trino |

Each major component contains its own **README.md** with implementation details and design decisions.
---
# Architecture Decisions

| Area                         | Decision                                                                  | Rationale                                                             |
| ---------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| **Multi-Database Scale**     | Designed for ~20 SQL Server databases and thousands of tables             | Supports enterprise-scale CDC ingestion without architectural changes |
| **Kafka Topic Strategy**     | Database-level topic isolation with table metadata routing                | Provides better scalability, monitoring and operational control       |
| **Bronze Layer Design**      | Centralized Debezium event normalization through BronzeDecode Job         | Decouples source CDC format from downstream processing layers         |
| **Metadata-Driven Pipeline** | Dynamic table processing based on database/schema/table metadata          | New tables can be onboarded without creating dedicated pipelines      |
| **Schema Management**        | Independent Schema Sync service                                           | Keeps Iceberg metadata synchronized with source database changes      |
| **Processing Model**         | Flink-based Silver synchronization jobs                                   | Provides real-time CDC processing with scalable stateful streaming    |
| **Deployment Strategy**      | Services can be deployed centrally or independently per database          | Enables workload isolation and horizontal scaling                     |
| **Lakehouse Storage**        | Iceberg V2 + Nessie + MinIO                                               | Provides ACID tables, schema evolution and scalable object storage    |
| **Analytics Layer**          | Trino for SQL analytics, Apache Druid for real-time dashboards (optional) | Separates analytical queries from operational monitoring workloads    |


---
## Roadmap

| Core Platform | Data Processing | Analytics |
|---------------|-----------------|-----------|
| Native Iceberg Catalog API | Gold Layer | Apache Superset |
| Full Schema Evolution (ADD / DROP / MODIFY / RENAME) | dbt | Semantic Layer |
| Java SchemaSync (Iceberg API) | MERGE INTO Optimization | |
| Apache Airflow | Data Quality Framework | |


---
## Author

👤 **Andrii Shtelmakh**

GitHub: https://github.com/ashtelmah
