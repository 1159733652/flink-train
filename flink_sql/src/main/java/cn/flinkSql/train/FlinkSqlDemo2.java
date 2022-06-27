package cn.flinkSql.train;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author zhanghongyu
 * @Date 2022/6/27
 * @DESC table api
 */
public class FlinkSqlDemo2 {
    public static void main(String[] args) {
        // 混合表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);// 流式环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 直接（纯粹）表环境
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        // 建表
        Table table = tableEnv.from(TableDescriptor.forConnector("kafka")// 指定连接器
                .schema(Schema.newBuilder()// 指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json")// 相关参数
                .option("topic", "flink-sql-demo03")
                .option("properties.bootstrap.servers", "192.168.136.130:9092")
                .option("properties.group.id", "g2")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());

        // 查询
        //table.execute().print(); 类似select *
        Table table2 = table.groupBy($("gender"))
                .select($("age").avg().as("avg_age"));

        // 输出
        table2.execute().print();


    }
}
