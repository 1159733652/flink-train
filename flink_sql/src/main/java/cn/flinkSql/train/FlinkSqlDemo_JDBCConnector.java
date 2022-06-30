package cn.flinkSql.train;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/30
 * @DESC        mysql作为source
 */
public class FlinkSqlDemo_JDBCConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 建表来映射mysql中的表、
        tableEnv.executeSql("CREATE TABLE flink_mysql_stu   " +
                        "   (  " +
                        "   id int primary key not enforced,      " +
                        "   name string,      " +
                        "   age int,      " +
                        "   gender string    " +
                        "   )  " +
                        "   WITH  (          " +
                        "       'connector' = 'jdbc',  " +
                        "       'url' = 'jdbc:mysql://192.168.136.130:3306/flink',      " +
                        "       'table-name' = 'stu',        " +
                        "       'username' = 'hadoop',        " +
                        "       'password' = '123456'   " +
                        "   )   "
        );

        // 有界表  读完就结束，不会持续读
        tableEnv.executeSql("select * from flink_mysql_stu ").print();
    }
}
