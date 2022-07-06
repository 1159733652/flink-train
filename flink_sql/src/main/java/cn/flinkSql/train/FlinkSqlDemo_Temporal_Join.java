package cn.flinkSql.train;

import cn.flinkSql.bean.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/7/5
 * @DESC 时态join 代码示例
 */
public class FlinkSqlDemo_Temporal_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /**
         *  测试数据
         *  订单 币种  金额  时间戳
         *  1,a,100，16743836400
         * 2,b,200
         * 3,c,250
         * c,f,300
         * 5,e,120
         */
        DataStreamSource<String> source1 = env.socketTextStream("192.168.136.130", 9998);

        SingleOutputStreamOperator<Order> ss1 = source1.map(s -> {
            String[] arr = s.split(",");
            return new Order(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3]));
        });


        // 创建俩个表
        // 主表,需要声明处理时间属性字段
        tableEnv.createTemporaryView("orders", ss1, Schema.newBuilder()
                .column("orderId", DataTypes.INT())
                .column("currency", DataTypes.STRING())
                .column("price", DataTypes.DOUBLE())
                .column("orderTime", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(orderTime,3)")
                .watermark("rt", "rt")
                .build());

        // 创建temporal 时态表
        tableEnv.executeSql("CREATE TABLE currency_rate   " +
                "   (  " +
                "   currency string,      " +
                "   rate double,      " +
                "   rt timestamp_ltz(3) metadata from 'op_ts',      " +
                "   watermark for rt as rt - interval '0' second,      " +
                "   PRIMARY KEY(currency) NOT ENFORCED    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'mysql-cdc',  " +
                "       'hostname' = '192.168.136.130',      " +
                "       'port' = '3306',        " +
                "       'username' = 'root',        " +
                "       'password' = '123456',   " +
                "       'database-name' = 'flink',   " +
                "       'table-name' = 'currency_rate'   " +
                "   )   "
        );

        tableEnv.executeSql("select * from currency_rate").print();

        // temporal 查询  永远查最新版
        tableEnv.executeSql("select " +
                "   orders.orderId,    " +
                "   orders.currency,   " +
                "   orders.price,  " +
                "   orders.orderTime,  " +
                "   currency_rate.rate    " +
                "   from orders " +
                "   left join currency_rate for system_time as of orders.rt  " +
                "   on orders.currency = currency_rate.currency ").print();
        env.execute();

    }
}
