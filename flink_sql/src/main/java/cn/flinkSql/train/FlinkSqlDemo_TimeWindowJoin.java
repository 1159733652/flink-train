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
 * @DESC
 */
public class FlinkSqlDemo_TimeWindowJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // 各类窗口示例
        // inner
        /*tableEnv.executeSql("   select "         +
                "   a.f0,a.f1,a.f2,b.f0,b.f1 "        +
                "   from    " +
                "   (   " +
                "   select * from table(tumble(table t_left,descriptor(rt),interval '10' second))  " +
                "   ) a " +
                "   join    " +
                "   (   " +
                "   select * from table(tumble(table t_right,descriptor(rt),interval '10' second))  " +
                "   ) b    " +
                "   on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0  ").print();*/


        // left / rightt / full outer
        /*tableEnv.executeSql("   select "         +
                "   a.f0,a.f1,a.f2,b.f0,b.f1 "        +
                "   from    " +
                "   (   " +
                "   select * from table(tumble(table t_left,descriptor(rt),interval '10' second))  " +
                "   ) a " +
                "   full join    " +
                "   (   " +
                "   select * from table(tumble(table t_right,descriptor(rt),interval '10' second))  " +
                "   ) b    " +
                "   on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0  ").print();*/


        // semi in
/*        tableEnv.executeSql("   select "         +
                "   a.f0,a.f1,a.f2  "        +
                "   from    " +
                "   (   " +
                "   select * from table(tumble(table t_left,descriptor(rt),interval '10' second))  " +
                "   ) a " +
                "   where f0 in (   " +
                "   select f0 from   " +
                "   (   " +
                "   select * from table(tumble(table t_right,descriptor(rt),interval '10' second))  " +
                "   ) b  " +
                "   where  " +
                "   a.window_start = b.window_start and a.window_end = b.window_end )   ").print();*/


        // semi  no in
        tableEnv.executeSql("   select "         +
                "   a.f0,a.f1,a.f2  "        +
                "   from    " +
                "   (   " +
                "   select * from table(tumble(table t_left,descriptor(rt),interval '10' second))  " +
                "   ) a " +
                "   where f0 not in (   " +
                "   select f0 from   " +
                "   (   " +
                "   select * from table(tumble(table t_right,descriptor(rt),interval '10' second))  " +
                "   ) b  " +
                "   where  " +
                "   a.window_start = b.window_start and a.window_end = b.window_end )   ").print();
    }
}
