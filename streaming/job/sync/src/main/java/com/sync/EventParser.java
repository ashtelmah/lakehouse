package com.sync;

import com.fasterxml.jackson.databind.ObjectMapper;

public class EventParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static SchemaEvent parse(String json) {
        try {
            return MAPPER.readValue(json, SchemaEvent.class);
        } catch (Exception e) {
            System.out.println(">>> EventParser ERROR: " + e.getMessage());
            return null;
        }
    }
}
