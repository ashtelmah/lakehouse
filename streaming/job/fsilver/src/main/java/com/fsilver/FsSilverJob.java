package com.fsilver;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;


import java.util.List;

public class FsSilverJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

// Checkpointing tuned for Iceberg
env.enableCheckpointing(120_000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60_000);
env.getCheckpointConfig().setCheckpointTimeout(180_000);
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

env.getCheckpointConfig().setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
);

// опціонально, якщо під час backlog checkpoint-и гальмують ingestion
env.getCheckpointConfig().setCheckpointIntervalDuringBacklog(300_000);

        final String kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP");
        final String kafkaTopic     = System.getenv("KAFKA_TOPIC");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(kafkaTopic)
                .setGroupId("silver-cdc-earliest")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // Minimal debug
        System.out.println("### KafkaSource configured:");
        System.out.println("bootstrap = " + kafkaBootstrap);
        System.out.println("topic     = " + kafkaTopic);
        System.out.println("group.id  = silver-cdc-earliest");
        System.out.println("offsets   = earliest");

        DataStream<String> jsonStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-cdc-source"
        );

        // Parse JSON → KafkaEvent (без debug)
        DataStream<KafkaEvent> events = jsonStream
                .map(KafkaEvent::parse)
                .filter(e -> e != null)
                .name("event-parse");

        List<String> tables = IcebergCatalogFactory.listTables("silver");

        for (String tbl : tables) {

            String tableName = tbl.toLowerCase();
            String base = "silver-" + tableName;

            DataStream<KafkaEvent> filtered =
                    events.filter(e -> e.getTable().equalsIgnoreCase(tableName))
                          .name(base + "-filter");

            DataStream<KafkaEvent> snapshotStream =
                    filtered.filter(e ->
                            e.getOp() == KafkaEvent.Operation.SNAPSHOT ||
                            e.getOp() == KafkaEvent.Operation.INSERT)
                            .name(base + "-snapshot-filter");

            DataStream<KafkaEvent> cdcStream =
                    filtered.filter(e ->
                            e.getOp() == KafkaEvent.Operation.UPDATE ||
                            e.getOp() == KafkaEvent.Operation.DELETE)
                            .name(base + "-cdc-filter");

            DataStream<RowData> snapshotUpserted =
                    snapshotStream
                            .keyBy(KafkaEvent::getPkKey)
                            .process(new SilverUpsert(tableName))
                            .name(base + "-snapshot-upsert");

            DataStream<RowData> cdcUpserted =
                    cdcStream
                            .keyBy(KafkaEvent::getPkKey)
                            .process(new SilverUpsert(tableName))
                            .name(base + "-cdc-upsert");

            IcebergSinkFactory.createSnapshotSink(snapshotUpserted, tableName)
                    .name(base + "-snapshot-sink")
                    .uid(base + "-snapshot-sink");

            IcebergSinkFactory.createCdcSink(cdcUpserted, tableName)
                    .name(base + "-cdc-sink")
                    .uid(base + "-cdc-sink");
        }

        env.execute("FsSilver – Silver UPSERT Job (minimal debug)");
    }
}
