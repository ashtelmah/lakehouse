package com.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class SchemaEventPublisher {

    private static final String TOPIC = "schema.events";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    public SchemaEventPublisher() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void publish(SchemaEvent event) {
        try {
            String key = event.namespace != null ? event.namespace : event.table;
            String json = mapper.writeValueAsString(event);

            ProducerRecord<String, String> rec =
                    new ProducerRecord<>(TOPIC, key, json);

            producer.send(rec, (metadata, ex) -> {
                if (ex != null) {
                    System.out.println(">>> SchemaEventPublisher ERROR: " + ex.getMessage());
                }
            });

        } catch (Exception e) {
            System.out.println(">>> SchemaEventPublisher serialize ERROR: " + e.getMessage());
        }
    }

    public void close() {
        try {
            producer.flush();
            producer.close();
        } catch (Exception ignored) {}
    }
}
