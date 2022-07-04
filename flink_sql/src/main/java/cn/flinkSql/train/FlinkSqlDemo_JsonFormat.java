package cn.flinkSql.train;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/28
 * @DESC jsonformat 详解
 */
public class FlinkSqlDemo_JsonFormat {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // {"id":30,"name":{"nick":"tiedan3","formal":"doit edu3},"age":18,"gender":"male"}
        // 建表(数据源表）
        tableEnv.executeSql("CREATE TABLE t_json   " +
                "   (  " +
                "   id int,      " +
                "   name map<string,string>,    " +
                "   bigid as id * 10,      " +
                "   age int,    " +
                "   gender string  " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'filesystem',  " +
                "       'path' = 'data/json/qiantao/',      " +
                "       'format' = 'json'   " +
                "   )   "
        );

        tableEnv.executeSql("desc t_json").print();

        tableEnv.executeSql("select * from t_json").print();

        // 查询每个人的昵称和id
        // map类型的取数
        tableEnv.executeSql("select id,name['nick'] as nick from t_json").print();
    }
}
