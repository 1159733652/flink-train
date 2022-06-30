package cn.flinkSql.train;

import cn.flinkSql.bean.UpsertBean;
import cn.flinkSql.bean.UpsertBean2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/30
 * @DESC
 */
public class FlinkSqlDemo_UpsertKafkaConnector2 {
    public static void main(String[] args) throws Exception {

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

        /**
         * 数据格式
         *      1,zs
         *      2,ls
         *      3,ww
         *      4,lh
         */
        DataStreamSource<String> source2 = env.socketTextStream("192.168.136.130", 9998);

        SingleOutputStreamOperator<UpsertBean> bean1 = source1.map(s -> {
            String[] arr = s.split(",");
            return new UpsertBean(Integer.parseInt(arr[0]), arr[1]);
        });


        SingleOutputStreamOperator<UpsertBean2> bean2 = source2.map(s -> {
            String[] arr = s.split(",");
            return new UpsertBean2(Integer.parseInt(arr[0]), arr[1]);
        });

        // 流转表
        tableEnv.createTemporaryView("bean1",bean1);
        tableEnv.createTemporaryView("bean2",bean2);

        // 建表
        tableEnv.executeSql("CREATE TABLE t_upsert_kafka2   " +
                        "   (  " +
                        "   id int primary key not enforced,      " +// 物理字段
                        "   gender string,      " +// 物理字段
                        "   name string    " +
                        "   )  " +
                        "   WITH  (          " +
                        "       'connector' = 'upsert-kafka',  " +
                        "       'topic' = 'flink-sql-upkafka3',      " +
                        "       'properties.bootstrap.servers' = '192.168.136.130:9092',        " +
                        "       'properties.group.id' = 'testGroup',        " +
//                "       'scan.startup.mode' = 'earliest-offset',    " +
                        "       'key.format' = 'csv',   " +
                        "       'value.format' = 'csv'   " +
                        "   )   "
        );

        // sql 查询
        tableEnv.executeSql("insert into t_upsert_kafka2" +
                " select bean1.id,bean1.gender,bean2.name from bean1 left join bean2 on bean1.id = bean2.id");

        tableEnv.executeSql("select * from t_upsert_kafka2").print();

        env.execute();

    }
}
