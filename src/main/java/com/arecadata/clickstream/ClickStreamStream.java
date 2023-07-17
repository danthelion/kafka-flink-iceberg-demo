package com.arecadata.clickstream;

import java.util.HashMap;
import java.util.Map;

import com.arecadata.clickstream.clicks.ClickDeserializationSchema;
import com.arecadata.clickstream.clicks.Click;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class ClickStreamStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickStreamStream.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        Configuration hadoopConf = new Configuration();

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("uri", parameters.get("uri", "http://rest:8181"));
        catalogProperties.put("io-impl", parameters.get("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"));
        catalogProperties.put("warehouse", parameters.get("warehouse", "s3://warehouse/wh/"));
        catalogProperties.put("s3.endpoint", parameters.get("s3-endpoint", "http://minio:9000"));

        CatalogLoader catalogLoader = CatalogLoader.custom(
                "demo",
                catalogProperties,
                hadoopConf,
                parameters.get("catalog-impl", "org.apache.iceberg.rest.RESTCatalog"));

        Schema schema = new Schema(
                Types.NestedField.optional(1, "timestamp", Types.StringType.get()),
                Types.NestedField.optional(2, "event", Types.StringType.get()),
                Types.NestedField.optional(3, "user_id", Types.StringType.get()),
                Types.NestedField.optional(4, "site_id", Types.StringType.get()),
                Types.NestedField.optional(5, "url", Types.StringType.get()),
                Types.NestedField.optional(6, "on_site_seconds", Types.IntegerType.get()),
                Types.NestedField.optional(7, "viewed_percent", Types.IntegerType.get())
        );

        Catalog catalog = catalogLoader.loadCatalog();

        TableIdentifier outputTable = TableIdentifier.of(
                "test",
                "clickstream");

        if (!catalog.tableExists(outputTable)) {
            catalog.createTable(outputTable, schema, PartitionSpec.unpartitioned());
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(Integer.parseInt(parameters.get("checkpoint", "10000")));

        KafkaSource<Click> source = KafkaSource.<Click>builder()
                .setBootstrapServers("broker:29092")
                .setTopics("clickstream")
                .setGroupId("clickstream-flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ClickDeserializationSchema())
                .build();

        DataStreamSource<Click> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Row> streamRow = stream.map(Click::toRow);

        FlinkSink.forRow(streamRow, FlinkSchemaUtil.toSchema(schema))
                .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable))
                .toBranch(parameters.get("branch", "main"))
                .distributionMode(DistributionMode.HASH)
                .writeParallelism(2)
                .append();

        env.execute();
    }
}
