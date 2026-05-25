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

USE CATALOG iceberg_catalog;

CREATE DATABASE IF NOT EXISTS `raw`;
CREATE DATABASE IF NOT EXISTS `bronze`;
CREATE DATABASE IF NOT EXISTS `silver`;
CREATE DATABASE IF NOT EXISTS `gold`;

CREATE TABLE IF NOT EXISTS `raw`.`kafka_all_src_iceberg` (
  payload STRING,
  kafka_ts TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS `bronze`.`kafka_all_src_clean` (
  payload STRING,
  kafka_ts TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS `silver`.`kafka_all_src_enriched` (
  payload STRING,
  kafka_ts TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS `gold`.`kafka_all_src_final` (
  payload STRING,
  kafka_ts TIMESTAMP(3)
);
