package com.sync;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.LinkedHashMap;
import java.util.Map;

public class AlterExecutorSave {

    final TableEnvironment tEnv;
    private static final String CATALOG = "iceberg_catalog";
    private static final String DB = "silver";

    public AlterExecutorSave() {

        Configuration conf = new Configuration();
        conf.setString("execution.runtime-mode", "batch");

        conf.setString("s3.endpoint", System.getenv("S3_ENDPOINT"));
        conf.setString("s3.access-key", System.getenv("S3_ACCESS_KEY"));
        conf.setString("s3.secret-key", System.getenv("S3_SECRET_KEY"));
        conf.setString("s3.path-style-access", System.getenv("S3_PATH_STYLE"));
        conf.setString("s3.region", System.getenv("S3_REGION"));
        conf.setString("s3.connection.ssl.enabled", "false");

        this.tEnv = TableEnvironmentImpl.create(conf);

        tEnv.executeSql(
                "CREATE CATALOG IF NOT EXISTS " + CATALOG + " WITH ("
                        + " 'type'='iceberg',"
                        + " 'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',"
                        + " 'uri'='http://nessie:19120/api/v2',"
                        + " 'ref'='main',"
                        + " 'warehouse'='s3://warehouse/',"
                        + " 'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
                        + " 's3.endpoint'='" + System.getenv("S3_ENDPOINT") + "',"
                        + " 's3.access-key'='" + System.getenv("S3_ACCESS_KEY") + "',"
                        + " 's3.secret-key'='" + System.getenv("S3_SECRET_KEY") + "',"
                        + " 's3.path-style-access'='" + System.getenv("S3_PATH_STYLE") + "',"
                        + " 's3.region'='" + System.getenv("S3_REGION") + "'"
                        + ")"
        );

        tEnv.executeSql("USE CATALOG " + CATALOG);
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + DB);
        tEnv.executeSql("USE " + DB);
    }

    private String normalizeColumnName(String name) {
        return name.replace("`", "").trim().toLowerCase();
    }

    private String normalizeType(String type) {
        return type.trim().toUpperCase();
    }

    private String fullTable(String table) {
        return "`" + table.toLowerCase() + "`";
    }

    // ---------------------------------------------------------
    // CREATE TABLE IF NOT EXISTS
    // ---------------------------------------------------------
    public void createTableIfMissing(String table, String schemaJson) throws Exception {

        Map<String, String> cols = new LinkedHashMap<>();

        // Normalize all columns from Avro schema
        AvroUtils.extractColumnTypes(schemaJson).forEach((k, v) -> {
            String cleanName = normalizeColumnName(k);
            String cleanType = normalizeType(v);
            cols.put(cleanName, cleanType);
        });

        // Force Debezium service columns to STRING
        cols.put("__$operation", "STRING");
        cols.put("__$update_mask", "STRING");
        cols.put("__$end_lsn", "STRING");
        cols.put("__$start_lsn", "STRING");
        cols.put("__$seqval", "STRING");
        cols.put("__$command_id", "STRING");

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ")
          .append(fullTable(table))
          .append(" (\n");

        int i = 0;
        for (Map.Entry<String, String> e : cols.entrySet()) {
            sb.append("  `").append(e.getKey()).append("` ").append(e.getValue());
            if (i < cols.size() - 1) sb.append(",\n");
            i++;
        }

        sb.append(",\n  PRIMARY KEY (gid) NOT ENFORCED\n");
        sb.append(") WITH ('format-version'='2')");

        String sql = sb.toString();

        System.out.println(">>> EXEC CREATE:");
        System.out.println(sql);

        tEnv.executeSql(sql);
    }

    // ---------------------------------------------------------
    // READ ICEBERG TABLE SCHEMA
    // ---------------------------------------------------------
    public Map<String, String> getTableSchema(String table) {

        String tbl = "`" + table.toLowerCase() + "`";

        TableResult result = tEnv.executeSql("DESCRIBE " + tbl);

        Map<String, String> map = new LinkedHashMap<>();

        try (CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                Row row = it.next();

                Object f0 = row.getField(0);
                Object f1 = row.getField(1);

                if (f0 == null || f1 == null) continue;

                String col = normalizeColumnName(f0.toString());
                String type = normalizeType(f1.toString());

                map.put("`" + col + "`", type);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read Iceberg schema for table " + table, e);
        }

        return map;
    }

    // ---------------------------------------------------------
    // APPLY ALTER TABLE
    // ---------------------------------------------------------
    public void applyAlter(String table, DiffEngine.DiffResult diff) throws Exception {

        String tbl = "`" + DB + "`.`" + table.toLowerCase() + "`";

        for (String col : diff.added) {

            String clean = normalizeColumnName(col);

            String sql = "ALTER TABLE " + tbl + " ADD `" + clean + "` STRING";

            System.out.println(">>> EXEC: " + sql);
            tEnv.executeSql(sql);
        }

        if (!diff.typeChanged.isEmpty()) {
            System.out.println(">>> TYPE CHANGES SKIPPED: " + diff.typeChanged);
        }

        if (!diff.nullableChanged.isEmpty()) {
            System.out.println(">>> NULLABLE CHANGES SKIPPED: " + diff.nullableChanged);
        }

        if (!diff.removed.isEmpty()) {
            System.out.println(">>> REMOVED COLUMNS SKIPPED: " + diff.removed);
        }
    }
}
