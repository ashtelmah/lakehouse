CREATE CATALOG iceberg WITH (
  'type'='iceberg',
  'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
  'uri'='http://nessie:19120/api/v1',
  'ref'='main',
  'warehouse'='s3a://warehouse',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000',
  's3.access-key-id'='admin',
  's3.secret-access-key'='admin12345',
  's3.path-style-access'='true'
);

USE CATALOG iceberg;

CREATE NAMESPACE IF NOT EXISTS raw;
CREATE NAMESPACE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS raw.events (
  event_id BIGINT,
  event_type STRING,
  event_data STRING,
  event_ts TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'iceberg',
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'location' = 's3a://warehouse/raw/events/',
  'write.target-file-size-bytes' = '134217728',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '5'
);

CREATE TABLE IF NOT EXISTS bronze.users (
  user_id BIGINT,
  name STRING,
  email STRING,
  updated_at TIMESTAMP(3)
) WITH (
  'connector' = 'iceberg',
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'location' = 's3a://warehouse/bronze/users/',
  'upsert.enabled' = 'true',
  'equality-field.columns' = 'user_id',
  'write.target-file-size-bytes' = '134217728',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '5'
);
