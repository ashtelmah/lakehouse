package com.fsilver;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;

import java.util.HashMap;
import java.util.Map;

public class IcebergSinkFactory {

    private static CatalogLoader buildCatalogLoader(Map<String, String> props) {
        Configuration hadoopConf = new Configuration();

        return CatalogLoader.custom(
                "silver",
                props,
                hadoopConf,
                "org.apache.iceberg.nessie.NessieCatalog"
        );
    }

    private static Map<String, String> baseProps() {
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

        return props;
    }

    public static DataStreamSink<RowData> createSnapshotSink(DataStream<RowData> stream,
                                                             String tableName) {

        Map<String, String> props = baseProps();
        props.put("write.target-file-size-bytes", "134217728"); // 128MB

        CatalogLoader catalogLoader = buildCatalogLoader(props);

        TableIdentifier id = TableIdentifier.of("silver", tableName.toLowerCase());
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, id);

        // Debug перед комітом
        DataStream<RowData> debugged = stream.map(r -> {
            System.out.println("ICEBERG SNAPSHOT WRITE: " + tableName);
            return r;
        });

        return IcebergSink.forRowData(debugged)
                .tableLoader(tableLoader)
                .setAll(props)
                .append();
    }

    public static DataStreamSink<RowData> createCdcSink(DataStream<RowData> stream,
                                                        String tableName) {

        Map<String, String> props = baseProps();
        props.put("write.target-file-size-bytes", "67108864"); // 64MB

        CatalogLoader catalogLoader = buildCatalogLoader(props);

        TableIdentifier id = TableIdentifier.of("silver", tableName.toLowerCase());
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, id);

        // Debug перед комітом
        DataStream<RowData> debugged = stream.map(r -> {
            System.out.println("ICEBERG CDC WRITE: " + tableName);
            return r;
        });

        return IcebergSink.forRowData(debugged)
                .tableLoader(tableLoader)
                .setAll(props)
                .append();
    }
}
