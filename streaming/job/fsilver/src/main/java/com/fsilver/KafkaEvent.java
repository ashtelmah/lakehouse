package com.fsilver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;

public class KafkaEvent {

    public enum Operation {
        SNAPSHOT,   // r
        INSERT,     // c
        UPDATE,     // u
        DELETE      // d
    }

    private final String table;                 // normalized table name
    private final Operation op;                 // c/u/d/r
    private final Map<String, String> before;   // PK + old values
    private final Map<String, String> after;    // new values
    private final String pkKey;                 // composite PK key for keyBy

    private static final ObjectMapper mapper = new ObjectMapper();

    public KafkaEvent(String table,
                      Operation op,
                      Map<String, String> before,
                      Map<String, String> after,
                      String pkKey) {
        this.table = table;
        this.op = op;
        this.before = before;
        this.after = after;
        this.pkKey = pkKey;
    }

    public String getTable() {
        return table;
    }

    public Operation getOp() {
        return op;
    }

    public Map<String, String> getBefore() {
        return before;
    }

    public Map<String, String> getAfter() {
        return after;
    }

    public String getPkKey() {
        return pkKey;
    }

    // ============================================================
    // PARSE JSON → KafkaEvent
    // ============================================================
    public static KafkaEvent parse(String jsonString) throws Exception {

        JsonNode json = mapper.readTree(jsonString);

        // -----------------------------
        // TABLE NAME (normalized)
        // -----------------------------
        String table;
        if (json.has("table")) {
            table = normalize(json.get("table").asText());
        } else if (json.has("source") && json.get("source").has("table")) {
            table = normalize(json.get("source").get("table").asText());
        } else {
            table = "unknown";
        }

        // -----------------------------
        // OPERATION
        // -----------------------------
        String op = json.has("op") ? json.get("op").asText() : "?";

        Operation operation = switch (op) {
            case "r" -> Operation.SNAPSHOT;
            case "c" -> Operation.INSERT;
            case "u" -> Operation.UPDATE;
            case "d" -> Operation.DELETE;
            default -> Operation.SNAPSHOT;
        };

        // -----------------------------
        // BEFORE / AFTER (lowerCase keys)
        // -----------------------------
        Map<String, String> beforeMap = jsonNodeToMap(json.get("before"));
        Map<String, String> afterMap  = jsonNodeToMap(json.get("after"));

        // -----------------------------
        // PK KEY
        // -----------------------------
        String pkKey = buildPkKey(table, beforeMap, afterMap);

        return new KafkaEvent(table, operation, beforeMap, afterMap, pkKey);
    }

    // ============================================================
    // BUILD PK KEY
    // ============================================================
    private static String buildPkKey(String table,
                                     Map<String, String> before,
                                     Map<String, String> after) {

        try {
            List<String> pkCols = IcebergCatalogFactory.loadPkColumns("silver", table);

            Map<String, String> src = (before != null && !before.isEmpty())
                    ? before
                    : after;

            if (src == null || pkCols == null || pkCols.isEmpty()) {
                return table;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < pkCols.size(); i++) {
                String col = pkCols.get(i).toLowerCase();
                String val = src.get(col);
                if (i > 0) sb.append('|');
                sb.append(val == null ? "null" : val);
            }
            return sb.toString();
        } catch (Exception e) {
            return table;
        }
    }

    // ============================================================
    // JSON → Map<String,String> (lowerCase keys)
    // ============================================================
    private static Map<String, String> jsonNodeToMap(JsonNode node) {
        if (node == null || node.isNull()) return null;

        Map<String, String> map = new HashMap<>();
        Iterator<String> fieldNames = node.fieldNames();

        while (fieldNames.hasNext()) {
            String field = fieldNames.next();
            JsonNode value = node.get(field);

            if (value == null || value.isNull()) {
                map.put(field.toLowerCase(), null);
            } else {
                map.put(field.toLowerCase(), value.asText());
            }
        }

        return map;
    }

    private static String normalize(String name) {
        return name.trim().toLowerCase();
    }

    @Override
    public String toString() {
        return "KafkaEvent{" +
                "table='" + table + '\'' +
                ", op=" + op +
                ", pkKey='" + pkKey + '\'' +
                '}';
    }
}
