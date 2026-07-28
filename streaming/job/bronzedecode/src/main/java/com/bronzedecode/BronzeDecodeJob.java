package com.bronzedecode;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.admin.*;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

public class BronzeDecodeJob {

    private static final String OUTPUT_TOPIC = "KafkaBronze";

    public static void main(String[] args) throws Exception {

        String mode = args.length > 0 ? args[0] : "normal";
        String groupId = "bronze-decode-" + mode + "-" + System.currentTimeMillis();
        String schemaRegistry = "http://schema-registry:8081";

        System.out.println("=== BronzeDecodeJob SNAPSHOT-CAPABLE ===");
        System.out.println("mode      = " + mode);
        System.out.println("group.id  = " + groupId);
        System.out.println("registry  = " + schemaRegistry);
        System.out.println("output    = " + OUTPUT_TOPIC);

        createOutputTopicIfMissing();

        Properties cProps = new Properties();
        cProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        cProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        if (mode.equals("snapshot")) {
            cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        } else {
            cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cProps);

        Pattern topicPattern = Pattern.compile("_\\.DataPlatform\\.dbo\\..*");
        System.out.println("Subscribing to: " + topicPattern.pattern());
        consumer.subscribe(topicPattern);

        Properties pProps = new Properties();
        pProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(pProps);

        AvroDecoder decoder = new AvroDecoder(schemaRegistry);

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<byte[], byte[]> rec : records) {
                try {
                    String payload = decoder.decode(rec.value());
                    String enriched = enrichJson(payload, rec.topic());

                    ProducerRecord<String, String> out =
                            new ProducerRecord<>(OUTPUT_TOPIC, rec.topic(), enriched);

                    producer.send(out);

                } catch (Exception e) {
                    System.err.println("[ERROR] topic=" + rec.topic() +
                            " offset=" + rec.offset() + " : " + e.getMessage());
                }
            }
        }
    }

    private static String enrichJson(String json, String topic) {

        String[] parts = topic.split("\\.");

        String database = parts.length > 1 ? parts[1] : "";
        String schema = parts.length > 2 ? parts[2] : "";
        String table = parts.length > 3 ? parts[3] : "";

        String trimmed = json.trim();
        if (trimmed.startsWith("{")) {
            trimmed = trimmed.substring(1);
        }

        return "{"
                + "\"topic\":\"" + topic + "\","
                + "\"database\":\"" + database + "\","
                + "\"schema\":\"" + schema + "\","
                + "\"table\":\"" + table + "\","
                + trimmed;
    }

    private static void createOutputTopicIfMissing() throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> topics = admin.listTopics().names().get();

            if (!topics.contains(OUTPUT_TOPIC)) {
                NewTopic nt = new NewTopic(OUTPUT_TOPIC, 3, (short) 1);
                admin.createTopics(Collections.singleton(nt)).all().get();
                System.out.println("Created topic: " + OUTPUT_TOPIC);
            } else {
                System.out.println("Topic exists: " + OUTPUT_TOPIC);
            }
        }
    }
}
