package cn.flinkSql.train;

import cn.flinkSql.bean.Person;
import com.alibaba.fastjson.JSON;
import com.ibm.icu.impl.Row;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;


/**
 * @Author zhanghongyu
 * @Date 2022/6/27
 * @DESC 各种表对象的创建的办法
 */
public class FlinkSqlDemo_TableObjectCreate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table table_a(id int ..)" +
                "witg(...)");

        /*
         1-从一个已存在的表名创建table对象
         */
        Table table_a = tableEnv.from("table_a");// 不存在会报错


        /*
         2-从一个tableDescriptor（表描述器）来创建table对象
         */
        Table table_b = tableEnv.from(TableDescriptor
                .forConnector("kafka")
                .format("")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .build())
                .option("topic", "flink-demo")
                .build());

        /*
         3-从数据流来创建table对象
         */
        KafkaSource<String> kafka_source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.xxx.xxx:9092")
                .setTopics("flink-demo")
                .setGroupId("g3")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kfkStream = env.fromSource(kafka_source, WatermarkStrategy.noWatermarks(), "kfk");

        // 不指定schema。将流创建成表对象，表的schema值是默认的，一般不符合要求
        Table table_c = tableEnv.fromDataStream(kfkStream);

        // 为了获得更理想的表结构，可以将kfk流对象转换为javaBean
        SingleOutputStreamOperator<Person> javaBeanStream = kfkStream.map(json -> JSON.parseObject(json, Person.class));

        Table table_d = tableEnv.fromDataStream(javaBeanStream);
        // 手动指定 schema定义，来将一个javabean流转成table对象
        Table table_e = tableEnv.fromDataStream(javaBeanStream,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build());

        table_d.execute().print();


        /*
         4、从测试数据得到表对象
         */
        // 得到单字段表
        Table table_f = tableEnv.fromValues(1, 2, 3, 4, 5);
        table_f.printSchema();// 表结构
        table_f.execute().print();

        // 得到多字段表
        Table table_g = tableEnv.fromValues(
                DataTypes.ROW(// 定义字段属性
                  DataTypes.FIELD("id",DataTypes.INT()),
                        DataTypes.FIELD("name",DataTypes.STRING()),
                        DataTypes.FIELD("age",DataTypes.INT()),
                        DataTypes.FIELD("mail",DataTypes.STRING())
                ),
                Row.of(1,"zz",20,"ma"),
                Row.of(2,"zz",50,"ma"),
                Row.of(3,"zz",30,"ma"),
                Row.of(4,"zz",40,"ma")
        );
        table_g.printSchema();
        table_g.execute().print();
        env.execute();
    }


}



