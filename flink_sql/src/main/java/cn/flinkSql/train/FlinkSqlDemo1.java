package cn.flinkSql.train;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/24
 * @DESC flink sql 学习
 */
public class FlinkSqlDemo1 {
    public static void main(String[] args) {
        // 创建table 环境入口()
        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();// 流计算模式
        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

        // 创建原表
        // 把kafka中一个topic数据映射成一张flink sql表
        // json ": {"id":1,"name":"zs","age":20,"gender":"male"}
        // json ": {"id":2,"name":"bb","age":30,"gender":"female"}
        // json ": {"id":3,"name":"dd","age":40,"gender":"female"}
        // json ": {"id":4,"name":"cc","age":50,"gender":"male"}
        tableEnv.executeSql("CREATE TABLE t_kafka   " +
                "   (  " +
                "   id int,      " +
                "   name string,    " +
                "   age int,    " +
                "   gender string  " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'kafka',  "+
                "       'topic' = 'flink-sql-demo01',      "+
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        "+
                "       'properties.group.id' = 'g1',        "+
                "       'scan.startup.mode' = 'earliest-offset',    "+
                "       'format' = 'json',   " +
                "       'json.fail-on-missing-field' = 'false',   " +
                "       'json.ignore-parse-errors' = 'true'    " +
                "   )   "
        );


        tableEnv.executeSql("select gender,avg(age) as avg_age from t_kafka group by gender ").print();


    }
}
