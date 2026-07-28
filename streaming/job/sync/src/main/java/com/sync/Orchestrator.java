package com.sync;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class Orchestrator {

    private final Map<String, BufferSchema> buffers = new HashMap<>();
    private final Map<String, String> tablePk = new HashMap<>();

    private final SchemaRegistryClient registry;
    private final DiffEngine diffEngine;
    private final KafkaConsumer<String, byte[]> consumer;
    private final AlterExecutorSave alterExecutor;
    private final PKExtractor pkExtractor;

    private static final String SCHEMA_EVENTS_TOPIC = "schema.events";

    public Orchestrator() {

        this.registry = new CachedSchemaRegistryClient("http://schema-registry:8081", 200);
        this.diffEngine = new DiffEngine();
        this.alterExecutor = new AlterExecutorSave();
        this.pkExtractor = new PKExtractor(registry);

        createSchemaEventsTopicIfMissing();

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("group.id", "schema-sync-orchestrator");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset", "earliest");

        this.consumer = new KafkaConsumer<>(props);

        Pattern pattern = Pattern.compile("^(schema\\.events|_\\.DataPlatform\\..*)$");
        this.consumer.subscribe(pattern);
    }

    private void createSchemaEventsTopicIfMissing() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> topics = admin.listTopics().names().get();

            if (!topics.contains(SCHEMA_EVENTS_TOPIC)) {
                NewTopic nt = new NewTopic(SCHEMA_EVENTS_TOPIC, 1, (short) 1);
                admin.createTopics(Collections.singleton(nt)).all().get();
                System.out.println(">>> Created topic: " + SCHEMA_EVENTS_TOPIC);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create schema.events topic", e);
        }
    }

    // ---------------------------------------------------------
    // BOOTSTRAP FROM SCHEMA REGISTRY
    // ---------------------------------------------------------
    public void bootstrapFromSchemaRegistry() throws Exception {
        System.out.println(">>> Bootstrap from Schema Registry...");

        Collection<String> subjects;
        try {
            subjects = registry.getAllSubjects();
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Failed to load subjects from Schema Registry", e);
        }

        for (String subject : subjects) {

            if (!subject.endsWith("-value")) continue;

            String[] parts = subject.split("\\.");
            if (parts.length < 4) continue;

            String tableWithSuffix = parts[3];
            String table = tableWithSuffix.replace("-value", "");

            int schemaId;
            try {
                schemaId = registry.getLatestSchemaMetadata(subject).getId();
            } catch (IOException | RestClientException e) {
                throw new RuntimeException("Failed to load latest schema metadata for " + subject, e);
            }

            String schemaJson;
            try {
                schemaJson = registry.getSchemaById(schemaId).toString();
            } catch (IOException | RestClientException e) {
                throw new RuntimeException("Failed to load schema " + schemaId, e);
            }

            System.out.println(">>> BOOTSTRAP TABLE = " + table + ", schemaId = " + schemaId);

            alterExecutor.createTableIfMissing(table, schemaJson);

            buffers.putIfAbsent(table, new BufferSchema());
            buffers.get(table).update(schemaId, schemaJson);
        }

        bootstrapPrimaryKeys();

        System.out.println(">>> Bootstrap finished");
    }

    // ---------------------------------------------------------
    // BOOTSTRAP PRIMARY KEYS (only store PK, no ALTER)
    // ---------------------------------------------------------
    private void bootstrapPrimaryKeys() {
        System.out.println(">>> Bootstrap PK from key-schemas...");

        Collection<String> subjects;
        try {
            subjects = registry.getAllSubjects();
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Failed to load subjects for PK bootstrap", e);
        }

        for (String subject : subjects) {

            if (!subject.endsWith("-key")) continue;

            String[] parts = subject.split("\\.");
            if (parts.length < 4) continue;

            String table = parts[3].replace("-key", "");

            String pk = pkExtractor.extractPK(subject);
            if (pk == null || pk.isBlank()) {
                System.out.println(">>> No PK for table " + table + " (subject " + subject + ")");
                continue;
            }

            tablePk.put(table.toLowerCase(), pk);
            System.out.println(">>> PK bootstrap for " + table + ": " + pk);
        }
    }

    // ---------------------------------------------------------
    // STREAMING LOOP
    // ---------------------------------------------------------
    public void run() {
        System.out.println(">>> Orchestrator started...");

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, byte[]> rec : records) {
                try {
                    if (SCHEMA_EVENTS_TOPIC.equals(rec.topic())) {
                        handleSchemaEvent(rec);
                    } else {
                        handleDebeziumSchema(rec);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // ---------------------------------------------------------
    // HANDLE schema.events
    // ---------------------------------------------------------
    private void handleSchemaEvent(ConsumerRecord<String, byte[]> rec) {
        String json = new String(rec.value());
        System.out.println(">>> schema.events: " + json);

        SchemaEvent event = EventParser.parse(json);
        if (event == null) {
            System.out.println(">>> SchemaEvent parse failed");
            return;
        }

        String table = event.table.toLowerCase();

        if (event.primaryKey != null && !event.primaryKey.isBlank()) {
            tablePk.put(table, event.primaryKey);
            System.out.println(">>> PK updated for " + table + ": " + event.primaryKey);
        }

        switch (event.eventType) {

            case "CREATE_TABLE" -> {
                try {
                    if (event.fullSchema != null && !event.fullSchema.isBlank()) {
                        alterExecutor.createTableIfMissing(table, event.fullSchema);
                        System.out.println(">>> CREATE_TABLE applied for " + table);
                    } else {
                        System.out.println(">>> CREATE_TABLE skipped — no fullSchema");
                    }
                } catch (Exception e) {
                    System.out.println(">>> CREATE_TABLE ERROR: " + e.getMessage());
                }
            }

            case "ADD_COLUMN" -> {
                DiffEngine.DiffResult diff = new DiffEngine.DiffResult();
                diff.added.add(event.column);
                try {
                    alterExecutor.applyAlter(table, diff);
                    System.out.println(">>> ADD_COLUMN applied for " + table + ": " + event.column);
                } catch (Exception e) {
                    System.out.println(">>> ADD_COLUMN ERROR: " + e.getMessage());
                }
            }

            default -> System.out.println(">>> Unsupported eventType: " + event.eventType);
        }
    }

    // ---------------------------------------------------------
    // HANDLE Debezium SCHEMA CHANGE
    // ---------------------------------------------------------
    private void handleDebeziumSchema(ConsumerRecord<String, byte[]> rec) throws Exception {

        String topic = rec.topic();
        System.out.println(">>> TOPIC = " + topic);

        String[] parts = topic.split("\\.");
        if (parts.length < 4) {
            System.out.println(">>> Unexpected topic format: " + topic);
            return;
        }

        String table = parts[3];

        buffers.putIfAbsent(table, new BufferSchema());
        BufferSchema buffer = buffers.get(table);

        byte[] value = rec.value();
        if (value == null || value.length < 5) {
            System.out.println(">>> Invalid value payload for topic " + topic);
            return;
        }

        int schemaId =
                ((value[1] & 0xFF) << 24) |
                ((value[2] & 0xFF) << 16) |
                ((value[3] & 0xFF) << 8) |
                (value[4] & 0xFF);

        System.out.println(">>> schemaId = " + schemaId);

        String newSchema;
        try {
            newSchema = registry.getSchemaById(schemaId).toString();
        } catch (IOException | RestClientException e) {
            System.out.println(">>> ERROR reading schemaId " + schemaId + ": " + e.getMessage());
            return;
        }

        if (!buffer.isInitialized()) {
            System.out.println(">>> FIRST SCHEMA — creating table");
            alterExecutor.createTableIfMissing(table, newSchema);
            buffer.update(schemaId, newSchema);
            return;
        }

        if (!buffer.isNewSchema(schemaId)) {
            System.out.println(">>> schemaId already processed — skip");
            return;
        }

        Map<String, String> icebergCols = alterExecutor.getTableSchema(table);
        Map<String, String> avroCols = AvroUtils.extractColumnTypes(newSchema);

        DiffEngine.DiffResult diff = diffEngine.diff(icebergCols, avroCols);

        System.out.println(">>> DIFF RESULT:");
        System.out.println("Added: " + diff.added);
        System.out.println("Removed: " + diff.removed);
        System.out.println("TypeChanged: " + diff.typeChanged);
        System.out.println("NullableChanged: " + diff.nullableChanged);

        if (diff.hasChanges()) {
            System.out.println(">>> APPLYING ALTER...");
            alterExecutor.applyAlter(table, diff);
        }

        buffer.update(schemaId, newSchema);
    }

    public static void main(String[] args) throws Exception {
        Orchestrator o = new Orchestrator();
        o.bootstrapFromSchemaRegistry();
        o.run();
    }
}
