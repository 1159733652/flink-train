package cn.flinkSql.train;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/28
 * @DESC    案例联系
 *  kafka中有如下数据：
 *      {"id":1,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
 *      {"id":2,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
 *      {"id":3,"name":"zs","nick":"tiezhu","age":18,"gender":"male"}
 *      {"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
 *      现在需要用flink-sql来对上述数据进行查询统计：
 *          截至到当前，每个昵称都有多少个用户？
 *          截至到当前，每个性别，年龄最大值？
 *
 *
 *
 *  高级嵌套
 *  {"id":1,"name":{"formal":"zs","nick":"tiedan"},"age":18,"gender":"male"}
 */
public class FlinkSqlDemo_Exercise {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建表(数据源表）
        tableEnv.executeSql("CREATE TABLE t_kafka_exercise   " +
                "   (  " +
                "   id int,      " +
                "   name string,    " +
                "   nick string,    " +
                "   age int,    " +
                "   gender string  " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'kafka',  "+
                "       'topic' = 'flink-sql-exercise-demo2',      "+
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        "+
                "       'properties.group.id' = 'g5',        "+
                "       'scan.startup.mode' = 'earliest-offset',    "+
                "       'format' = 'json',   " +
                "       'json.fail-on-missing-field' = 'false',   " +
                "       'json.ignore-parse-errors' = 'true'    " +
                "   )   "
        );

        // 建表（输出目标表）
        // 用kafka作为sink表的时候，如果注册表的查询语句有更新/updata操作，需要将连接器改为upsert-kafka
        // kafka 连接器，不能接受update/修正模式的数据，只能接受insert模式的数据
        // 并且，sink表的字段中必须指明哪个字段为主键，意思为对哪个字段所在行做更新操作   nick string primary key not enforced
        // 必须指定key.format 和 value.format
        tableEnv.executeSql("CREATE TABLE t_kafka_nicekcnt   " +
                "   (  " +
                "   nick string,    " +
                "   user_cnt bigint,    " +
                "   PRIMARY KEY (nick) NOT ENFORCED    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'upsert-kafka',  "+
                "       'topic' = 'flink-sql-nickcnt-demo2',      "+
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        "+
                //"       'table.exec.sink.not-null-enforcer' = 'drop',        "+
                "       'key.format' = 'json',   " +
                "       'value.format' = 'json'   " +
                "   )   "
        );



        // 查询并打印
        // 截至到当前，每个昵称都有多少个用户？
            tableEnv.executeSql("insert into t_kafka_nicekcnt " +
                "select nick,count(distinct id) as user_cnt from t_kafka_exercise group by nick");

        // 截至到当前，每个性别，年龄最大值？

        // 输出
        //tableEnv.executeSql("select * from t_kafka_nicekcnt").print();
    }
}
