```
Kafka (Debezium Avro)
  → Sync Orchestrator  
  → Schema Registry  
  → AvroUtils  
  → DiffEngine  
  → AlterExecutor (CREATE + ALTER)
  → Iceberg Catalog (Nessie)  
  → metadata.json (schema evolution)

sync/
└── src/main/java/com/sync/
    ├── Orchestrator.java
    ├── EventParser.java
    ├── SchemaEvent.java
    ├── SchemaEventPublisher.java
    ├── AvroUtils.java
    ├── DiffEngine.java
    ├── AlterExecutorSave.java
    ├── BufferSchema.java
    └── PKExtractor.java
```
| Module | Responsibility | Uses | Why it exists |
| --- | --- | --- | --- |
| **[Orchestrator](ca://s?q=Explain_sync_Orchestrator_class)** | Main coordinator. Reads Debezium CDC schema changes from Kafka, loads Avro schema from Schema Registry, compares with Iceberg schema, applies CREATE/ALTER, processes manual schema.events. | KafkaConsumer, SchemaRegistryClient, DiffEngine, AlterExecutorSave | Central brain of schema evolution. Ensures Iceberg schema always matches upstream CDC schema. |
| **[EventParser](ca://s?q=Explain_sync_EventParser_class)** | Parses JSON from ``schema.events`` into SchemaEvent DTO. | Jackson | Manual schema evolution channel requires structured event parsing. |
| **[SchemaEvent](ca://s?q=Explain_sync_SchemaEvent_class)** | DTO describing schema change: CREATE_TABLE, ADD_COLUMN, MODIFY, DROP, RENAME, PK updates. | — | Unified representation of schema evolution events for both automatic and manual flows. |
| **[SchemaEventPublisher](ca://s?q=Explain_sync_SchemaEventPublisher_class)** | Publishes SchemaEvent into Kafka (``schema.events``). | KafkaProducer, Jackson | Allows manual schema evolution, CI/CD migrations, audit, and replayable schema changes. |
| **[AvroUtils](ca://s?q=Explain_sync_AvroUtils_class)** | Extracts column names/types from Debezium Avro schema, resolves logical types, adds CDC system fields. | Jackson, Avro | Debezium Avro schemas are complex; this module normalizes them into a stable column/type map. |
| **[DiffEngine](ca://s?q=Explain_sync_DiffEngine_class)** | Compares Iceberg schema vs Avro schema. Detects added, removed, typeChanged, nullableChanged fields. | — | Core of schema evolution. Determines what ALTER operations are required. |
| **[AlterExecutorSave](ca://s?q=Explain_sync_AlterExecutorSave_class)** | Executes CREATE TABLE and ALTER TABLE (ADD COLUMN) via Flink SQL. | Flink TableEnvironment | Temporary implementation. Provides minimal schema evolution until full Iceberg API migration. |
| **[BufferSchema](ca://s?q=Explain_sync_BufferSchema_class)** | Stores last processed schemaId + schema JSON for each table. | — | Prevents reprocessing identical schema versions and identifies first‑time bootstrap. |
| **[PKExtractor](ca://s?q=Explain_sync_PKExtractor_class)** | Extracts primary key from Debezium key-schema. | SchemaRegistryClient, Avro | PK is required for CREATE TABLE and later for UPSERT in Silver. |

🧬 How SyncJob Works
1. Bootstrap from Schema Registry

On startup:

    loads all *-value subjects

    extracts Avro schema

    creates Iceberg tables if missing

    caches schemaId in BufferSchema

    loads PK from *-key subjects

This ensures Iceberg has all tables before ingestion starts.
2. Streaming Loop

Orchestrator continuously polls Kafka:
a) schema.events

Manual events such as:

    CREATE_TABLE

    ADD_COLUMN

    PK updates

Parsed via EventParser → applied via AlterExecutor.
b) Debezium CDC schema changes

When Debezium sends a new schemaId:

    Extract schemaId from Avro payload
    Load Avro schema from Schema Registry
    Compare with Iceberg schema via DiffEngine
    Apply ALTER TABLE
    Update BufferSchema

## How SyncJob is Started

SyncJob is not a Flink streaming job.  
It runs as a standalone Java process inside its own container.

The container launches the Orchestrator class directly:

entrypoint: [
  "java",
  "-cp", "app.jar:/app/lib/*:/app/flink-lib/*",
  "com.sync.Orchestrator",
  "snapshot"
]

SyncJob does not submit anything to the Flink cluster.  
It uses Flink’s TableEnvironment locally to execute CREATE/ALTER TABLE
operations against the Iceberg catalog (Nessie + MinIO).

SyncJob lifecycle:

1. Start container
2. Initialize Schema Registry client
3. Initialize Kafka consumer
4. Bootstrap all tables from Schema Registry
5. Enter infinite loop:
   - read Debezium CDC schema changes
   - read manual schema.events
   - compute DIFF
   - apply CREATE/ALTER
6. Persist schema evolution into Iceberg metadata.json
7. Continue running as a daemon


This keeps Iceberg schema in sync with upstream database schema.

| Variable | Description |
| --- | --- |
| ``KAFKA_BOOTSTRAP`` | Kafka bootstrap servers |
| ``SCHEMA_REGISTRY_URL`` | Confluent Schema Registry endpoint |
| ``S3_ENDPOINT`` | MinIO endpoint |
| ``S3_ACCESS_KEY`` | MinIO access key |
| ``S3_SECRET_KEY`` | MinIO secret key |
| ``S3_REGION`` | S3 region |
| ``S3_PATH_STYLE`` | MinIO path-style access |
| ``NESSIE_URI`` | Nessie API endpoint |
| ``NESSIE_REF`` | Nessie branch |


Why SyncJob Exists

    Debezium schema changes must be reflected in Iceberg metadata
    Iceberg ingestion (Silver) requires correct schema before writing
    Schema evolution must be automatic and deterministic
    Manual schema changes must be possible (schema.events)
    Iceberg metadata.json must always match upstream database schema
    Nessie must track schema evolution as versioned commits
SyncJob ensures schema correctness, schema consistency, and schema versioning across the entire lakehouse.

