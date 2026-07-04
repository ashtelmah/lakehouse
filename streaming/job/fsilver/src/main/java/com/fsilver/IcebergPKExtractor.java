package com.fsilver;


import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class IcebergPKExtractor {

    private static final String NAMESPACE = "silver";

    /**
     * Extract PK columns for a given Iceberg table.
     * Priority:
     *   1) cdc.pk property (set by SyncJob)
     *   2) Iceberg identifier fields
     */
    public static List<String> extractPK(Catalog catalog, String tableName) {

        String normalized = tableName.toLowerCase();
        TableIdentifier id = TableIdentifier.of(NAMESPACE, normalized);

        try {
            Table table = catalog.loadTable(id);

            // ------------------------------------------------------------
            // 1. CDC PK (highest priority)
            // ------------------------------------------------------------
            String pkProp = table.properties().get("cdc.pk");
            if (pkProp != null && !pkProp.isBlank()) {

                List<String> pkList = Arrays.stream(pkProp.split(","))
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .filter(s -> !s.isEmpty())
                        .toList();

                return pkList;
            }

            // ------------------------------------------------------------
            // 2. Iceberg identifier fields
            // ------------------------------------------------------------
            List<String> idFields = new ArrayList<>(table.schema().identifierFieldNames());
            if (!idFields.isEmpty()) {
                idFields.replaceAll(String::toLowerCase);
                return List.copyOf(idFields);
            }

            throw new RuntimeException("No PK defined for table " + id);

        } catch (Exception e) {
            throw new RuntimeException("Cannot load Iceberg table: " + id, e);
        }
    }
}
