package com.sync;

import java.util.Map;

public class SchemaEvent {

    public String eventId;
    public long timestamp;
    public String source;
    public String mode;

    public String topic;
    public String table;
    public String database;
    public String namespace;

    public long schemaVersion;
    public int schemaId;
    public long lsn;
    public long txId;

    public String eventType;

    public Map<String, String> fields;
    public String column;
    public String newColumnName;
    public String type;
    public String oldType;
    public Boolean nullable;
    public Boolean oldNullable;
    public String defaultValue;
    public String oldDefaultValue;

    public String primaryKey;

    public String fullSchema;

    public String branch;
    public String mergeStrategy;
}
