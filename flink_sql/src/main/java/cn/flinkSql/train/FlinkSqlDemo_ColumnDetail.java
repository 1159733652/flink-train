package cn.flinkSql.train;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/28
 * @DESC 字段定义详解示例
 */
public class FlinkSqlDemo_ColumnDetail {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建表(数据源表）
        // 逻辑字段引用的物理字段必须存在
        // {"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tableEnv.executeSql("CREATE TABLE t_kafka_exercise   " +
                "   (  " +
                "   id int,      " +// 物理字段
                "   name string,    " +
                "   nick string,    " +
                "   age int,    " +
                "   guid as id ,    " +// 表达式字段（逻辑字段/计算字段）
                "   big_age as age + 10 ,    " +// 表达式字段（逻辑字段/计算字段）
                "   xxxoffset bigint metadata from 'offset' ,    " +// 元数据字段
                "   ts TIMESTAMP_LTZ(3) metadata from 'timestamp' ,    " +// 元数据字段
                "   gender string  " +
                /* "   PRIMARY KEY(id,name) NOT ENFORCED,  " +*/// 主键约束
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'kafka',  " +
                "       'topic' = 'flink-sql-exercise-demo2',      " +
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        " +
                "       'properties.group.id' = 'g5',        " +
                "       'scan.startup.mode' = 'earliest-offset',    " +
                "       'format' = 'json',   " +
                "       'json.fail-on-missing-field' = 'false',   " +
                "       'json.ignore-parse-errors' = 'true'    " +
                "   )   "
        );


        // 打印表结构
        tableEnv.executeSql("desc t_kafka_exercise").print();

        // 查询数据
        tableEnv.executeSql("select * from t_kafka_exercise").print();
    }
}
