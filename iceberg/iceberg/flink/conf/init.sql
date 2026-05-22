-- ============================================================
-- 1. Create Iceberg Catalog backed by Nessie
--    This catalog will be used by Flink for all Iceberg tables.
-- ============================================================
CREATE CATALOG iceberg WITH (
  'type'='iceberg',
  'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
  'uri'='http://nessie:19120/api/v1',
  'ref'='main',
  'warehouse'='s3://warehouse/',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',

  -- MinIO S3 configuration
  's3.endpoint'='http://minio:9000',
  's3.access-key-id'='admin',
  's3.secret-access-key'='admin12345',
  's3.path-style-access'='true'
);

-- Switch to the newly created catalog
USE CATALOG iceberg;

-- ============================================================
-- 2. NOTE: Iceberg namespaces CANNOT be created in Flink.
--    They must already exist in Nessie (created via Trino
--    or via REST API in init-runner.sh).
-- ============================================================

-- ============================================================
-- 3. Example: Create a table inside an existing namespace
--    (namespace must already exist in Nessie)
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.events (
  event_id BIGINT,
  event_type STRING,
  event_data STRING,
  event_ts TIMESTAMP(3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'iceberg',
  'write.format.default' = 'parquet'
);

-- ============================================================
-- 4. Example: Upsert-enabled table in the bronze namespace
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze.users (
  user_id BIGINT,
  name STRING,
  email STRING,
  updated_at TIMESTAMP(3)
) WITH (
  'connector' = 'iceberg',
  'write.format.default' = 'parquet',
  'upsert.enabled' = 'true',
  'equality-field.columns' = 'user_id'
);
