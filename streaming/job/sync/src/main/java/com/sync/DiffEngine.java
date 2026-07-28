package com.sync;

import java.util.*;

public class DiffEngine {

    public static class DiffResult {
        public final List<String> added = new ArrayList<>();
        public final List<String> removed = new ArrayList<>();
        public final List<String> typeChanged = new ArrayList<>();
        public final List<String> nullableChanged = new ArrayList<>();
        public final List<String> logicalTypeChanged = new ArrayList<>();
        public final List<String> defaultChanged = new ArrayList<>();

        public boolean hasChanges() {
            return !added.isEmpty() || !removed.isEmpty() ||
                    !typeChanged.isEmpty() || !nullableChanged.isEmpty() ||
                    !logicalTypeChanged.isEmpty() || !defaultChanged.isEmpty();
        }
    }

    /**
     * Compare Iceberg schema (old) with Avro schema (new)
     *
     * @param icebergCols map: `colname` -> type
     * @param avroCols    map: `colname` -> type
     */
    public DiffResult diff(Map<String, String> icebergCols, Map<String, String> avroCols) {

        DiffResult result = new DiffResult();

        // -----------------------------
        // ADDED FIELDS
        // -----------------------------
        for (String col : avroCols.keySet()) {
            if (!icebergCols.containsKey(col)) {
                // remove backticks for ALTER TABLE
                result.added.add(col.replace("`", ""));
            }
        }

        // -----------------------------
        // REMOVED FIELDS (not used now)
        // -----------------------------
        for (String col : icebergCols.keySet()) {
            if (!avroCols.containsKey(col)) {
                result.removed.add(col.replace("`", ""));
            }
        }

        // -----------------------------
        // TYPE CHANGES (optional)
        // -----------------------------
        for (String col : avroCols.keySet()) {
            if (!icebergCols.containsKey(col)) continue;

            String oldType = icebergCols.get(col);
            String newType = avroCols.get(col);

            if (!oldType.equalsIgnoreCase(newType)) {
                result.typeChanged.add(col.replace("`", ""));
            }
        }

        return result;
    }
}
