package com.mvpzhao.examples.sql;

import com.mvpzhao.examples.pojo.UserPvEntity;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.Properties;

/**
 * @see https://blog.csdn.net/qq_20672231/article/details/84936716
 */
public class FlinkSqlWindowUserPv {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(8);

        TableConfig tc = new TableConfig();

        StreamTableEnvironment tableEnv = new StreamTableEnvironment(env, tc);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "consumer-grp-1");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("flink-sql-test-1", new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(myConsumer);

        DataStream<Tuple2<String, Long>> map = stream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(" ");
                return new Tuple2<String, Long>(split[0], Long.valueOf(split[1]) * 1000);
            }
        });

        map.print(); //打印流数据


        //注册为user表
        tableEnv.registerDataStream("Users", map, "userId,timestampin,proctime.proctime");

        //执行sql查询     滚动窗口 10秒    计算10秒窗口内用户点击次数
        Table sqlQuery = tableEnv.sqlQuery("SELECT "
                + "TUMBLE_END(proctime, INTERVAL '10' SECOND) as processtime, userId, count(*) as pvcount "
                + "FROM Users "
                + "GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND), userId");


        //Table 转化为 DataStream
        DataStream<Tuple3<Timestamp, String, Long>> appendStream = tableEnv.toAppendStream(sqlQuery, Types.TUPLE(Types.SQL_TIMESTAMP, Types.STRING, Types.LONG));

        appendStream.print();

        //sink to mysql
        appendStream.map(new MapFunction<Tuple3<Timestamp, String, Long>, UserPvEntity>() {
            @Override
            public UserPvEntity map(Tuple3<Timestamp, String, Long> value) throws Exception {
                return new UserPvEntity(Timestamp.valueOf(value.f0.toString()), value.f1, value.f2);
            }
        }).addSink(new SinkUserPvToMySQL2());

        env.execute("userPv from Kafka");
    }

}
