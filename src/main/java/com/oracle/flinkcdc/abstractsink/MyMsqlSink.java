package com.oracle.flinkcdc.abstractsink;

import com.oracle.flinkcdc.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Objects;

public abstract class MyMsqlSink<T> extends RichSinkFunction<T> {

    transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {

        connection = MySQLUtil.getConnection();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        PreparedStatement statement = null;
        try{
            doInvoke(value);
        }finally {
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(Objects.nonNull(connection)){
            connection.close();
        }
    }

    abstract void doInvoke(T t);


}
