package com.fsilver;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergCatalogFactory {

    private static Catalog cachedCatalog;

    public static Catalog loadCatalog() {

        if (cachedCatalog != null) {
            return cachedCatalog;
        }

        Map<String, String> props = new HashMap<>();

        props.put("type", "nessie");
        props.put("uri", System.getenv().getOrDefault("NESSIE_URI", "http://nessie:19120/api/v2"));
        props.put("ref", System.getenv().getOrDefault("NESSIE_REF", "main"));

        props.put("warehouse", "s3://warehouse/");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        props.put("s3.endpoint", System.getenv().getOrDefault("S3_ENDPOINT", "http://minio:9000"));
        props.put("s3.access-key-id", System.getenv().getOrDefault("S3_ACCESS_KEY", "admin"));
        props.put("s3.secret-access-key", System.getenv().getOrDefault("S3_SECRET_KEY", "admin12345"));
        props.put("s3.region", System.getenv().getOrDefault("S3_REGION", "us-east-1"));
        props.put("s3.path-style-access", System.getenv().getOrDefault("S3_PATH_STYLE", "true"));

        Configuration hadoopConf = new Configuration();

        CatalogLoader loader = CatalogLoader.custom(
                "silver",
                props,
                hadoopConf,
                "org.apache.iceberg.nessie.NessieCatalog"
        );

        cachedCatalog = loader.loadCatalog();
        return cachedCatalog;
    }

    public static List<String> listTables(String namespace) {
        Catalog catalog = loadCatalog();
        return catalog.listTables(org.apache.iceberg.catalog.Namespace.of(namespace))
                .stream()
                .map(TableIdentifier::name)
                .collect(Collectors.toList());
    }

    public static List<String> loadPkColumns(String namespace, String tableName) {
        Catalog catalog = loadCatalog();
        Table t = catalog.loadTable(TableIdentifier.of(namespace, tableName));
        return t.schema().identifierFieldNames()
                .stream()
                .map(String::toLowerCase)
                .toList();
    }
}
