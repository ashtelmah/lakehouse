package com.sync;

public class BufferSchema {

    private int lastSchemaId = -1;
    private String lastSchema = null;

    public boolean isInitialized() {
        return lastSchema != null;
    }

    public boolean isNewSchema(int schemaId) {
        return schemaId != lastSchemaId;
    }

    public String getLastSchema() {
        return lastSchema;
    }

    public int getLastSchemaId() {
        return lastSchemaId;
    }

    public void update(int schemaId, String schema) {
        this.lastSchemaId = schemaId;
        this.lastSchema = schema;
    }

    public void printState() {
        System.out.println("=== BufferSchema ===");
        System.out.println("lastSchemaId = " + lastSchemaId);
        System.out.println("lastSchema = " + (lastSchema != null ? lastSchema : "null"));
        System.out.println("====================");
    }
}
