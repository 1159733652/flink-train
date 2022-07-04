package cn.flinkSql.train;

import cn.flinkSql.bean.Person;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/27
 * @DESC sqltable 创建各种表对象
 * 带sql表名的表创建  各种方式
 */
public class FlinkSqlDemo_SqlTableCreate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String filePath = "data/sqldmo/a.txt";
        /*
         1-通过构建表描述去创建一个有名表（sql表）
         */
        tableEnv.createTable("table_a", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("csv")
                .option("path",filePath)
                .option("csv.ignore-parse-errors", "true")
                .option("csv.allow-comments", "true")
                .build()
        );


        //tableEnv.executeSql(" select * from table_a ").print();

        //tableEnv.executeSql("select gender,max(age) as max_age from table_a group by gender ").print();

        /*
        2-从一个datastream创建一个带名的视图
         */

        DataStreamSource<String> socketStream = env.socketTextStream("192.168.136.130", 9999);
        SingleOutputStreamOperator<Person> javaBeanStream = socketStream.map(s -> {
            String[] split = s.split(",");
            return new Person(Integer.parseInt(split[0]),
                    split[1],
                    Integer.parseInt(split[2]),
                    split[3]
            );
        });
//        tableEnv.createTemporaryView("table_view_a",javaBeanStream);
//
//        tableEnv.executeSql("select gender,max(age) as max_age from table_view_a group by gender ").print();


        /*
        3-从一个已存在的table对象，得到一个有名的视图
         */
        Table table_a = tableEnv.from("table_a");
        tableEnv.createTemporaryView("table_view",table_a);


        tableEnv.executeSql("select gender,max(age) as max_age from table_view group by gender ").print();
    }
}
