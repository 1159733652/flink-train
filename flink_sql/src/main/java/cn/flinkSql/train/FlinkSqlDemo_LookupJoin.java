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
 * @DESC        look up join  为了提高性能，会有缓存
 *              默认不开启，需要手动开启lookup.cache.max-rows=(none)
 *              ttl=> lookup.cache.ttl = (none)
 */
public class FlinkSqlDemo_LookupJoin {
    public static void main(String[] args) throws Exception {
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
        SingleOutputStreamOperator<Tuple2<Integer, String>> ss1 = source1.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(Integer.parseInt(split[0]), split[1]);
        }).returns(new TypeHint<Tuple2<Integer, String>>() {
        });


        // 创建俩个表
        // 主表,需要声明处理时间属性字段
        tableEnv.createTemporaryView("a", ss1, Schema.newBuilder()
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .columnByExpression("pt","proctime()")
                .build());

        // 建表来映射mysql中的表、（lookup伪表）
        tableEnv.executeSql("CREATE TABLE b   " +
                "   (  " +
                "   id int primary key not enforced,      " +
                "   name string,      " +
                "   gender string    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'jdbc',  " +
                "       'url' = 'jdbc:mysql://192.168.136.130:3306/flink',      " +
                "       'table-name' = 'stu2',        " +
                "       'username' = 'root',        " +
                "       'password' = '123456'   " +
                "   )   "
        );


        // look up join 查询
        tableEnv.executeSql("select a.*,c.*  from a " +
                "   join b for system_time as of a.pt  as c " +
                "   on a.f0 = c.id ").print();

        env.execute();

    }
}
