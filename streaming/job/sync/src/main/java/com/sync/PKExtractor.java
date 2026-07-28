package com.sync;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

import org.apache.avro.Schema;

public class PKExtractor {

    private final SchemaRegistryClient registry;

    public PKExtractor(SchemaRegistryClient registry) {
        this.registry = registry;
    }

    /**
     * Reads Debezium key-schema and extracts the primary key column name.
     *
     * @param topic Debezium topic name: _.DataPlatform.dbo.Products-value
     * @return PK column name (e.g. "gid") or null if not found
     */
    public String extractPK(String topic) {
        try {
            // Convert value-topic → key-topic
            // _.DataPlatform.dbo.Products-value → _.DataPlatform.dbo.Products-key
            String keySubject = topic.replace("-value", "-key");

            ParsedSchema ps = registry.getLatestSchemaMetadata(keySubject) != null
                    ? registry.getSchemaById(registry.getLatestSchemaMetadata(keySubject).getId())
                    : null;

            if (ps == null) {
                System.out.println(">>> PKExtractor: no key-schema for " + keySubject);
                return null;
            }

            Schema avro = ((AvroSchema) ps).rawSchema();

            // Debezium key-schema always has "fields"
            if (!avro.getFields().isEmpty()) {
                // We assume single-column PK (your case)
                return avro.getFields().get(0).name().toLowerCase();
            }

            return null;

        } catch (Exception e) {
            System.out.println(">>> PKExtractor ERROR: " + e.getMessage());
            return null;
        }
    }
}
