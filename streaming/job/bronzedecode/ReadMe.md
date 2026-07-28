```
BronzeDecodeJob — Debezium Avro → Bronze JSON Normalizer

Debezium Avro (Kafka)
 → BronzeDecodeJob  
 → AvroDecoder  
 → JSON enrichment (topic/database/schema/table)
 → KafkaBronze  
 → Silver UPSERT ingestion

bronzedecode/
└── src/main/java/com/bronzedecode/
    ├── BronzeDecodeJob.java
    └── AvroDecoder.java
```

| Module | Responsibility | Uses | Why it exists |
| --- | --- | --- | --- |
| **[BronzeDecodeJob](ca://s?q=Explain_BronzeDecodeJob)** | Main Kafka→Kafka transformer. Reads Debezium CDC Avro, decodes binary payload, enriches JSON with routing metadata, writes normalized Bronze JSON into ``KafkaBronze``. Supports snapshot mode. | KafkaConsumer, KafkaProducer, AvroDecoder | Debezium sends binary Avro that Silver cannot ingest. BronzeDecode normalizes CDC into JSON and adds metadata needed for routing and UPSERT. |
| **[AvroDecoder](ca://s?q=Explain_AvroDecoder)** | Decodes Confluent‑encoded Debezium Avro payload. Extracts schemaId, loads Avro schema from Schema Registry, deserializes binary Avro into GenericRecord, returns JSON. Uses schema cache for performance. | SchemaRegistryClient, AvroSchema, GenericDatumReader, DecoderFactory | Debezium uses Confluent wire format (magic byte + schemaId). Without decoding, payload is unreadable. This module performs mandatory Avro→JSON conversion. |

How BronzeDecodeJob Works

BronzeDecodeJob is a standalone Java service, not a Flink job.
It runs inside its own container and continuously transforms Debezium Avro → Bronze JSON.

The container simply launches the Java class

Startup Flow

    Container starts  
    Docker launches a plain Java process inside the container.

    BronzeDecodeJob starts in snapshot mode
        Reads Kafka from earliest offset
        Processes the full historical CDC snapshot
        Continues in streaming mode afterwards

    Kafka consumer initializes
        Uses raw byte deserializers
        Subscribes to all Debezium SQL Server topics:
        _.DataPlatform.dbo.*

| Module | Responsibility | Uses | Why it exists |
| --- | --- | --- | --- |
| **[BronzeDecodeJob](ca://s?q=Explain_BronzeDecodeJob)** | Main Kafka→Kafka transformer. Reads Debezium CDC Avro, decodes binary payload, enriches JSON with routing metadata, writes normalized Bronze JSON into ``KafkaBronze``. Supports both ``snapshot`` and ``normal`` modes. | KafkaConsumer, KafkaProducer, AvroDecoder | Debezium sends binary Avro that Silver cannot ingest. BronzeDecode normalizes CDC into JSON and adds metadata needed for routing and UPSERT. |
| **[AvroDecoder](ca://s?q=Explain_AvroDecoder)** | Decodes Confluent‑encoded Debezium Avro payload. Extracts schemaId, loads Avro schema from Schema Registry, deserializes binary Avro into GenericRecord, returns JSON. Uses schema cache for performance. | SchemaRegistryClient, AvroSchema, GenericDatumReader, DecoderFactory | Debezium uses Confluent wire format (magic byte + schemaId). Without decoding, payload is unreadable. This module performs mandatory Avro→JSON conversion. |

```
Topic Pattern (Configurable)
BronzeDecodeJob subscribes to Debezium SQL Server topics using a configurable pattern:
environment:
  BRONZE_TOPIC_PATTERN: "_.DataPlatform.dbo.*"

This allows:
    multi‑database ingestion
    custom schemas
    future routing flexibility
able Name Extraction

BronzeDecodeJob extracts table metadata directly from the topic name:
Split by ".":
    database → DataPlatform
    schema → dbo
    table → Products
This supports the standard Debezium pattern:
✔ 1 database → many tables → 1 topic per table (1:1)

Debezium SQL Server always produces one topic per table, so BronzeDecodeJob can reliably extract table names without Schema Registry.

BronzeDecodeJob supports two execution modes:
1️⃣ snapshot
    Reads from earliest offset
    Processes full historical CDC
    Continues in streaming mode afterwards
2️⃣ normal
    Reads from latest offset
    Processes only new CDC events
```
| Reason | Explanation |
| --- | --- |
| Debezium sends **binary Avro** | Silver cannot ingest Avro; Bronze must decode it first |
| Silver needs **JSON with routing metadata** | Bronze adds database/schema/table fields |
| Topic names encode table identity | Supports 1 DB → many tables → 1 topic per table |
| Snapshot mode required | Allows full historical replay before CDC |
| Normal mode required | Real‑time CDC ingestion |
| No need for Flink | This is not a streaming DAG — just a Kafka transformer |
| Simple, reliable architecture | A small Java service is easier to restart and maintain |


