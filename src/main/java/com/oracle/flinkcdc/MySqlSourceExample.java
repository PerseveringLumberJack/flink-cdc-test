package com.oracle.flinkcdc;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class MySqlSourceExample {


    public static void main(String[] args) throws Exception {

        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("2.server")
                .port(3306)
                .databaseList("cds")
                .tableList("cds.adjust_cost")
                .username("leomaster")
                .password("leomastermysql")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // enable checkpoint
        env.enableCheckpointing(3000);

        DataStreamSink<String> sql_source = env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1)
                .print().setParallelism(1);// use parallelism 1 for sink to keep message ordering


        env.execute("Print MySQL Snapshot + Binlog");

    }
}
