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

        Properties kafkaProperties = buildKafkaProperties();
        // kafka msg: lily t10 1565852278
        FlinkKafkaConsumer010<String> userKafkaConsumer = new FlinkKafkaConsumer010<String>("flink-sql-user", new SimpleStringSchema(), kafkaProperties);
        // kafka msg: t10 tag-10
        FlinkKafkaConsumer010<String> tagKafkaConsumer = new FlinkKafkaConsumer010<String>("flink-sql-page", new SimpleStringSchema(), kafkaProperties);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        StreamTableEnvironment tableEnv = new StreamTableEnvironment(env, new TableConfig());

        DataStream<String> userStream = env.addSource(userKafkaConsumer);
        DataStream<Tuple3<String, String, Long>> userMap = userStream.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String value) throws Exception {
                String[] split = value.split(" ");
                return new Tuple3<String, String, Long>(split[0], split[1], Long.valueOf(split[2]) * 1000);
            }
        });
        userMap.print(); //打印流数据

        DataStream<String> tagStream = env.addSource(tagKafkaConsumer);
        DataStream<Tuple2<String, String>> tagMap = tagStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(" ");
                return new Tuple2<String, String>(split[0], split[1]);
            }
        });
        tagMap.print(); //打印流数据

        //注册为user表
        tableEnv.registerDataStream("Users", userMap, "userId, tagId, timestampin, proctime.proctime");
        //注册为tag表
        tableEnv.registerDataStream("Tags", tagMap, "tagId, tagName, proctime.proctime");

        processUser(env, tableEnv, userMap);
        processTag(env, tableEnv, tagMap);

        env.execute("user-tag-pv from Kafka");
    }

    public static void processUser(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, DataStream<Tuple3<String, String, Long>> userMap) throws Exception {
        //执行sql查询     滚动窗口 20秒    计算20秒窗口内用户点击次数
        Table sqlQuery = tableEnv.sqlQuery("SELECT "
                + "TUMBLE_END(proctime, INTERVAL '20' SECOND) as processtime, userId, count(*) as pvCnt "
                + "FROM Users "
                + "GROUP BY TUMBLE(proctime, INTERVAL '20' SECOND), userId");

        //Table 转化为 DataStream
        DataStream<Tuple3<Timestamp, String, Long>> dataStream = tableEnv.toAppendStream(sqlQuery, Types.TUPLE(Types.SQL_TIMESTAMP, Types.STRING, Types.LONG));

        dataStream.print();

        //sink to mysql
        dataStream.map(new MapFunction<Tuple3<Timestamp, String, Long>, UserPvEntity>() {
            @Override
            public UserPvEntity map(Tuple3<Timestamp, String, Long> value) throws Exception {
                return new UserPvEntity(Timestamp.valueOf(value.f0.toString()), value.f1, value.f2);
            }
        }).addSink(new SinkUserPvToMySQL2());
    }

    public static void processTag(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, DataStream<Tuple2<String, String>> tagMap) throws Exception {
        Table sqlQuery = tableEnv.sqlQuery("SELECT "
                + "TUMBLE_END(t.proctime, INTERVAL '20' SECOND) as processtime, t.tagName, count(*) as tagCnt "
                + "FROM Users u JOIN Tags t "
                + "ON u.tagId = t.tagId "
                + "GROUP BY TUMBLE(t.proctime, INTERVAL '20' SECOND), t.tagName");

        //Table 转化为 DataStream
        DataStream<Tuple3<Timestamp, String, Long>> dataStream = tableEnv.toAppendStream(sqlQuery, Types.TUPLE(Types.SQL_TIMESTAMP, Types.STRING, Types.LONG));

        dataStream.print();
    }

    public static Properties buildKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "consumer-grp-1");
        return properties;
    }

}
