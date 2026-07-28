package com.bronzedecode;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Decoder;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AvroDecoder {

    private final SchemaRegistryClient registry;
    private final Map<Integer, Schema> cache = new ConcurrentHashMap<>();

    public AvroDecoder(String registryUrl) {
        this.registry = new CachedSchemaRegistryClient(registryUrl, 1000);
    }

    public String decode(byte[] bytes) {
        try {
            if (bytes == null) {
                return "{\"tombstone\":true}";
            }

            if (bytes.length < 5) {
                return "{\"error\":\"short-bytes\"}";
            }

            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            byte magic = buffer.get();

            if (magic != 0) {
                return "{\"error\":\"not-confluent-avro\",\"magic\":" + magic + "}";
            }

            int schemaId = buffer.getInt();
            byte[] avroPayload = new byte[buffer.remaining()];
            buffer.get(avroPayload);

            Schema schema = cache.computeIfAbsent(schemaId, id -> {
                try {
                    ParsedSchema ps = registry.getSchemaById(id);
                    return ((AvroSchema) ps).rawSchema();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(avroPayload, null);
            GenericRecord record = reader.read(null, decoder);

            return record.toString();

        } catch (Exception e) {
            return "{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}";
        }
    }
}
