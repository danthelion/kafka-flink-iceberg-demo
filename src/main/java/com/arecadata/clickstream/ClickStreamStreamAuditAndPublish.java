package com.arecadata.clickstream;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;

public class ClickStreamStreamAuditAndPublish {
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

        TableIdentifier outputTable = TableIdentifier.of(
                "test",
                "clickstream");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(Integer.parseInt(parameters.get("checkpoint", "10000")));

        // read from ci branch that is being written to by ClickStreamStream.java
        String branch = parameters.get("branch", "ci");

        DataStream<RowData> source = FlinkSource.forRowData()
                .env(env)
                .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable))
                .branch(branch)
                .streaming(true)
                .build();

        // run tests
        source.print();

        // if all good then publish to master
        // how to fast forward?
        // how to exit?
        // https://stackoverflow.com/a/75961706/1797204

        env.execute();
    }
}
