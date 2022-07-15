package com.oracle.flinkcdc;


import com.google.gson.Gson;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {

        Gson jsstr = new Gson();
        HashMap<String, Object> hs = new HashMap<>();

        String topic = sourceRecord.topic();
        String[] split = topic.split("[.]");
        String database = split[1];
        String table = split[2];
        hs.put("database",database);
        hs.put("table",table);
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //获取数据本身
        Struct struct = (Struct)sourceRecord.value();
        Struct after = struct.getStruct("after");

        if (after != null) {
            Schema schema = after.schema();
            HashMap<String, Object> afhs = new HashMap<>();
            for (Field field : schema.fields()) {
                afhs.put(field.name(), after.get(field.name()));
            }
            hs.put("data",afhs);
        }

        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        hs.put("type",type);

        collector.collect(jsstr.toJson(hs));
    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
