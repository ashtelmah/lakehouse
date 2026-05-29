USE CATALOG iceberg_catalog;
SET 'execution.runtime-mode' = 'streaming';

INSERT INTO iceberg_catalog.`raw`.`kafka_all_src_iceberg`
SELECT
  `value` AS payload,
  CAST(kafka_ts AS TIMESTAMP(3)) AS kafka_ts
FROM default_catalog.default_database.kafka_debezium_src;
