package cn.flinkSql.train;


import cn.flinkSql.bean.UpsertBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/30
 * @DESC
 */
public class FlinkSqlDemo_UpsertKafkaConnector {
    public static void main(String[] args) throws Exception {
//        Configuration configuration = new Configuration();
//        configuration.setInteger("rest.port",8081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        /**
         * 数据格式
         *      1,male
         *      2,female
         *      3,male
         *      4,female
         */
        DataStreamSource<String> source1 = env.socketTextStream("192.168.136.130", 9999);
        SingleOutputStreamOperator<UpsertBean> bean1 = source1.map(s -> {
            String[] arr = s.split(",");
            return new UpsertBean(Integer.parseInt(arr[0]), arr[1]);
        });

        // 流转表
        tableEnv.createTemporaryView("bean1",bean1);

        // 建表
        tableEnv.executeSql("CREATE TABLE t_upsert_kafka   " +
                "   (  " +
                "   gender string primary key not enforced,      " +// 物理字段
                "   cnt bigint    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'upsert-kafka',  " +
                "       'topic' = 'flink-sql-upkafka',      " +
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        " +
                "       'properties.group.id' = 'testGroup',        " +
//                "       'scan.startup.mode' = 'earliest-offset',    " +
                "       'key.format' = 'csv',   " +
                "       'value.format' = 'csv'   " +
                "   )   "
        );

        // sql 查询
        tableEnv.executeSql("insert into t_upsert_kafka select gender,count(id) as cnt from bean1 group by gender");

        tableEnv.executeSql("select * from t_upsert_kafka").print();

        env.execute();

    }
}
