# Lakehouse Platform  
**Flink 2.0 + Iceberg 1.1 + Nessie 0.105 + Trino 481 + MinIO + Debezium + Kafka KRaft (Avro)**

This repository contains a minimal but fully functional Lakehouse architecture built around:

- **[Apache Flink](ca://s?q=Explain_Apache_Flink)** for streaming and SQL processing  
- **[Apache Iceberg](ca://s?q=Explain_Apache_Iceberg)** as the table format  
- **[Project Nessie](ca://s?q=Explain_Project_Nessie)** as the Iceberg catalog  
- **[MinIO](ca://s?q=Explain_MinIO)** as S3-compatible object storage  
- **[Trino](ca://s?q=Explain_Trino)** for analytics  
- **[Debezium](ca://s?q=Explain_Debezium_CDC)** for CDC ingestion  
- **[Kafka KRaft](ca://s?q=Explain_Kafka_KRaft)** with **Avro + Schema Registry** for event streaming  

The goal is to provide a clean, reproducible environment for building modern Lakehouse pipelines.

---

## 🧩 Component Versions

| Component | Version |
|----------|---------|
| Apache Flink | 2.0.0 |
| Apache Iceberg | 1.1.0 |
| Project Nessie | 0.107.x |
| Trino | 481 |
| MinIO | RELEASE.2024 |
| Kafka KRaft | 3.7.x |
| Debezium SQL Server Connector | 2.7.3.Final |
| Confluent Schema Registry | 7.7.0 |
| Avro Serialization | Confluent Avro |

## 🏗 Architecture Overview

flowchart LR

    subgraph Source
        MSSQL[(MS SQL Server)]
    end

    subgraph CDC
        DEB[Debezium<br/>SQL Server Connector]
    end

    subgraph Kafka
        KAFKA[(Kafka KRaft<br/>+ Schema Registry)]
    end

    subgraph Compute
        FLINK[Flink 2.0 SQL Gateway]
        TRINO[Trino 481]
    end

    subgraph Metadata
        NESSIE[Nessie 0.105<br/>Iceberg Catalog]
    end

    subgraph Storage
        MINIO[(MinIO S3<br/>Iceberg Warehouse)]
    end

    MSSQL --> DEB -->|CDC Avro| KAFKA
    KAFKA -->|Avro → Iceberg| FLINK

    FLINK -->|Write Data Files| MINIO
    TRINO -->|Query Data Files| MINIO

    FLINK -->|Catalog| NESSIE
    TRINO -->|Catalog| NESSIE

# 👤 Author
Andrii Shtelmakh  
GitHub: https://github.com/ashtelmah
