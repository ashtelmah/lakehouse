CREATE CATALOG iceberg_catalog WITH (
  'type'='iceberg',
  'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
  'uri'='http://nessie:19120/api/v1',
  'ref'='main',
  'warehouse'='s3://warehouse/',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000',
  's3.path-style-access'='true'
);

DROP TEMPORARY TABLE IF EXISTS kafka_debezium_src;

CREATE TEMPORARY TABLE kafka_debezium_src (
    payload BYTES,

    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
    topic STRING METADATA FROM 'topic' VIRTUAL,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL
)
WITH (
    'connector' = 'kafka',
    'topic-pattern' = '_\\.DataPlatform\\.dbo\\..*',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink_router_products_3',
    'value.format' = 'raw',
    'scan.startup.mode' = 'earliest-offset'
);

SET 'execution.runtime-mode'='streaming';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

INSERT INTO iceberg_catalog.`raw`.`kafka_all_src_iceberg`
SELECT
    payload,
    CAST(kafka_ts AS TIMESTAMP(6)) AS kafka_ts,
    topic,
    kafka_partition,
    kafka_offset,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS ingest_ts
FROM kafka_debezium_src;

