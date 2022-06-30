package cn.flinkSql.train;

import cn.flinkSql.bean.UpsertBean;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/30
 * @DESC
 */
public class FlinkSqlDemo_FileSystemConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/workspace/flink_workspace/flikn_course_1.14.4/data/sink");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 建表来映射mysql中的表、
        tableEnv.executeSql("CREATE TABLE flink_file   " +
                "   (  " +
                "   userid string,      " +
                "   order_amount double,      " +
                "   dt string,      " +
                "   `hour` string      " +
                "   )  partitioned by (dt,`hour`)" +
                "   WITH  (          " +
                "       'connector' = 'filesystem',  " +
                "       'path' = 'file:///D:/workspace/flink_workspace/flikn_course_1.14.4/data/filtable',      " +
                "       'format' = 'json',        " +
                "       'sink.partition-commit.delay' = '1 h',        " +
                "       'sink.partition-commit.policy.kind' = 'success-file',   " +
                "       'sink.rolling-policy.file-size' = '8M',   " +
                "       'sink.rolling-policy.rollover-interval' = '30 min',   " +
                "       'sink.rolling-policy.check-interval' = '10 second'   " +
                "   )   "
        );


        /**
         *  数据
         *      u01,88.8,2022-02-02,10
         *      u02,88.9,2022-02-02,10
         *      u03,99,8,2022-02-02,11
         *      u03,99.9,2022-0202,11
         */
        SingleOutputStreamOperator<Tuple4<String, Double, String, String>> stream = env.socketTextStream("192.168.136.130", 9999)
                .map(s -> {
                    String[] split = s.split(",");
                    return Tuple4.of(split[0], Double.parseDouble(split[1]), split[2], split[3]);
                }).returns(new TypeHint<Tuple4<String, Double, String, String>>() {
                });

        tableEnv.createTemporaryView("orders",stream);

        tableEnv.executeSql("insert into flink_file select * from orders");

    }
}
