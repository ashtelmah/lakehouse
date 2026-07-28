package com.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

public class AvroUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Debezium SQL Server CDC system fields
    private static final Map<String, String> CDC_FIELDS = Map.of(
            "`__$start_lsn`", "STRING",
            "`__$end_lsn`", "STRING",
            "`__$seqval`", "STRING",
            "`__$operation`", "INT",
            "`__$update_mask`", "STRING",
            "`__$command_id`", "BIGINT"
    );

    public static Map<String, String> extractColumnTypes(String schemaJson) throws Exception {
        JsonNode root = MAPPER.readTree(schemaJson);

        JsonNode valueRecord = findValueRecord(root);
        if (valueRecord == null || !valueRecord.has("fields"))
            return Collections.emptyMap();

        Map<String, String> map = new LinkedHashMap<>();

        // Business fields from Avro schema
        for (JsonNode f : valueRecord.get("fields")) {
            String col = f.get("name").asText().toLowerCase();
            String name = "`" + col + "`";   // escape
            String type = resolveFlinkType(f.get("type"));
            map.put(name, type);
        }

        // Add CDC system fields
        map.putAll(CDC_FIELDS);

        return map;
    }

    public static List<String> extractColumns(String schemaJson) throws Exception {
        return new ArrayList<>(extractColumnTypes(schemaJson).keySet());
    }

    private static JsonNode findValueRecord(JsonNode root) {
        JsonNode fields = root.get("fields");
        if (fields == null) return null;

        for (JsonNode f : fields) {
            if (f.get("name").asText().equals("before")) {

                JsonNode typeNode = f.get("type");

                if (typeNode.isArray()) {
                    for (JsonNode t : typeNode) {
                        if (t.isObject() && t.has("fields")) return t;
                    }
                }

                if (typeNode.isObject() && typeNode.has("type") && typeNode.get("type").isArray()) {
                    for (JsonNode t : typeNode.get("type")) {
                        if (t.isObject() && t.has("fields")) return t;
                    }
                }

                if (typeNode.isObject() && typeNode.has("fields")) {
                    return typeNode;
                }
            }
        }

        return null;
    }

    private static String resolveFlinkType(JsonNode typeNode) {

        if (typeNode.isArray()) {
            for (JsonNode t : typeNode) {
                if (!t.isTextual()) return resolveFlinkType(t);
            }
            return "STRING";
        }

        if (typeNode.isTextual()) {
            return switch (typeNode.asText()) {
                case "string" -> "STRING";
                case "int" -> "INT";
                case "long" -> "BIGINT";
                case "boolean" -> "BOOLEAN";
                case "float" -> "FLOAT";
                case "double" -> "DOUBLE";
                case "bytes" -> "BYTES";
                default -> "STRING";
            };
        }

        if (typeNode.isObject()) {

            if (typeNode.has("connect.name")) {
                String logical = typeNode.get("connect.name").asText();

                return switch (logical) {
                    case "io.debezium.time.Date" -> "DATE";
                    case "io.debezium.time.Timestamp" -> "TIMESTAMP";
                    case "io.debezium.time.MicroTimestamp" -> "TIMESTAMP(6)";
                    case "io.debezium.time.NanoTimestamp" -> "TIMESTAMP(9)";
                    case "io.debezium.time.ZonedTimestamp" -> "TIMESTAMP_LTZ";
                    case "org.apache.kafka.connect.data.Decimal" -> "DECIMAL(38,18)";
                    default -> "STRING";
                };
            }

            if (typeNode.has("items")) {
                return "ARRAY<" + resolveFlinkType(typeNode.get("items")) + ">";
            }

            if (typeNode.has("values")) {
                return "MAP<STRING, " + resolveFlinkType(typeNode.get("values")) + ">";
            }

            if (typeNode.has("fields")) {
                return "STRING";
            }
        }

        return "STRING";
    }
}
