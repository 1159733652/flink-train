package cn.flinkSql.train;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/7/5
 * @DESC           Interval join
 *          场景：广告曝光流和广告观看流的join
 */
public class FlinkSqlDemo_IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setLong("table.exec.state.ttl",60*60*1000);

        /**
         *  测试数据
         *  1,a,1000
         * 2,b,2000
         * 3,c,2500
         * c,f,3000
         * 5,e,12000
         */
        DataStreamSource<String> source1 = env.socketTextStream("192.168.136.130", 9998);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss1 = source1.map(s -> {
            String[] split = s.split(",");
            return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {
        });

        /**
         *  测试数据
         *  1,bj,1000
         * 2,sh,2000
         * 3,sz,2600
         * 5,yn,12000
         */
        DataStreamSource<String> source2 = env.socketTextStream("192.168.136.130", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss2 = source2.map(s -> {
            String[] split = s.split(",");
            return Tuple3.of(split[0], split[1], Long.parseLong(split[2]));
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {
        });


        // 创建俩个表
        tableEnv.createTemporaryView("t_left", ss1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
                .watermark("rt","rt - interval '0' second ")
                .build());
        tableEnv.createTemporaryView("t_right", ss2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2,3)")
                .watermark("rt","rt - interval '0' second ")
                .build());


        // interval join
        tableEnv.executeSql("   select a.f0,a.f1,a.f2,b.f0,b.f1    " +
                " from t_left a " +
                " join t_right b   " +
                "  on a.f0 = b.f0 " +
                "  and a.rt between b.rt - interval '2' second and b.rt ").print();
    }
}
