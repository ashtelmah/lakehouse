```
Kafka (Bronze JSON) 
     → FsSilverJob 
     → KafkaEvent 
     → SilverUpsert (UPSERT engine) 
     → IcebergSinkFactory (snapshot + CDC sinks) 
     → Iceberg (Nessie + MinIO)

fsilver/
└── src/main/java/com/fsilver/
    ├── FsSilverJob.java
    ├── KafkaEvent.java
    ├── SilverUpsert.java
    ├── IcebergCatalogFactory.java
    ├── IcebergSinkFactory.java
    └── IcebergPKExtractor.java
```

| Module | Responsibility (expanded) | Uses | Why it exists (expanded) |
| --- | --- | --- | --- |
| **[FsSilverJob](ca://s?q=Explain_FsSilverJob)** | Main Flink streaming job. Reads Kafka CDC, parses JSON into KafkaEvent, loads Iceberg table list, builds a separate DAG branch per table, splits snapshot/CDC streams, applies UPSERT via SilverUpsert, and writes results using Iceberg sinks. | Flink Streaming API, KafkaSource, Iceberg TableIdentifier | Flink requires a static DAG. Each table has its own schema, PK, UPSERT logic, and sink. FsSilverJob orchestrates the entire ingestion pipeline and ensures deterministic, fault‑tolerant processing across all tables. |
| **[KafkaEvent](ca://s?q=Explain_KafkaEvent)** | Normalizes Debezium JSON into a structured event: table name, operation type, before/after maps, composite PK key. Handles JSON parsing, field normalization, and PK extraction. | Jackson JSON, Iceberg PK | UPSERT logic needs structured before/after values and a deterministic PK key. KafkaEvent provides a stable, schema‑agnostic representation of CDC events that downstream operators can safely process. |
| **[SilverUpsert](ca://s?q=Explain_SilverUpsert)** | UPSERT engine. Loads Iceberg schema, converts normalized CDC events into RowData, applies INSERT, UPDATE (delete+insert), DELETE, handles type conversion (string, int, decimal, timestamp), and produces Iceberg‑compatible mutations. | Flink KeyedProcessFunction, RowData, Iceberg Types | Iceberg does not support UPDATE directly — it requires DELETE + INSERT using equality deletes. SilverUpsert guarantees correct mutation semantics and produces RowData that exactly matches Iceberg schema and type system. |
| **[IcebergCatalogFactory](ca://s?q=Explain_IcebergCatalogFactory)** | Creates and caches Iceberg Nessie catalog, configures S3FileIO for MinIO, loads table metadata, retrieves table list, and exposes PK columns. Ensures consistent catalog access across all operators. | Iceberg CatalogLoader, Table, Hadoop Configuration | Silver needs Iceberg schema and PK before building the DAG. Catalog creation is expensive and must be reused. This module centralizes all catalog configuration and ensures consistent metadata access for the entire job. |
| **[IcebergSinkFactory](ca://s?q=Explain_IcebergSinkFactory)** | Builds Iceberg sinks for snapshot and CDC streams, configures writer properties (file size, commit behavior), loads table schema via TableLoader, and connects Flink RowData to Iceberg append operations. | IcebergSink.forRowData, TableLoader, Flink DataStreamSink | Snapshot and CDC workloads require different writer strategies. This module produces optimized sinks for each flow and performs the final step of converting RowData into Parquet files and committing them to Nessie. |
| **[IcebergPKExtractor](ca://s?q=Explain_IcebergPKExtractor)** | Determines primary key columns for a table: first from ``cdc.pk`` property, then from Iceberg identifier fields. Normalizes PK names and supports composite keys. | Iceberg Table, TableIdentifier | UPSERT cannot work without PK. PK must match both CDC source and Iceberg schema. This module ensures deterministic keyBy grouping and correct equality deletes, making UPSERT possible. |

| Variable | Description |
| --- | --- |
| ``KAFKA_BOOTSTRAP`` | Kafka bootstrap servers |
| ``KAFKA_TOPIC`` | CDC topic name |
| ``S3_ENDPOINT`` | MinIO endpoint |
| ``S3_ACCESS_KEY`` | MinIO access key |
| ``S3_SECRET_KEY`` | MinIO secret key |
| ``S3_REGION`` | S3 region (default: us-east-1) |
| ``S3_PATH_STYLE`` | Path-style access (true for MinIO) |
| ``NESSIE_URI`` | Nessie API endpoint |
| ``NESSIE_REF`` | Nessie branch (default: main) |

How Silver is Started

Silver runs as a separate container that submits the job to an already running Flink cluster.

The Flink cluster (JobManager + TaskManagers) must be started first.

The fsilver container:

    waits for JobManager to become available

    submits the Silver job using flink run

    exits or stays alive (job continues running on TaskManagers)

yaml

entrypoint: [
  "bash", "-c",
  "sleep 5 && flink run -m flink-jobmanager:8081 /opt/flink/jobs/fsilver.jar"
]

Silver does not run inside JobManager or TaskManager.
It is submitted to the cluster and executed by TaskManagers.
