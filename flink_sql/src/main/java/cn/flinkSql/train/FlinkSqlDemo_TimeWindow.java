package cn.flinkSql.train;

import cn.flinkSql.bean.Bid;
import cn.flinkSql.bean.Person;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author zhanghongyu
 * @Date 2022/7/4
 * @DESC
 */
public class FlinkSqlDemo_TimeWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 得到数据流
        DataStreamSource<String> source1 = env.socketTextStream("192.168.136.130", 9999);

        // 数据格式
        // 数据流转换
        // |          bidtime | price | item | supplier_id |

        /**
         *   2020-04-15 08:05 | 4.00  | C    | supplier1   |
         * 2020-04-15 08:07:00.000,2.00,A,supplier1
         * 2020-04-15 08:09:00.000,5.00,D,supplier2
         * 2020-04-15 08:11:00.000,3.00,B,supplier2
         * 2020-04-15 08:13:00.000,1.00,E,supplier1
         * 2020-04-15 08:17:00.000,6.00,F,supplier2
         */
        SingleOutputStreamOperator<Bid> source2 = source1.map(s -> {
            String[] split = s.split(",");
            return new Bid(
                    split[0],
                    Double.parseDouble(split[1]),
                    split[2],
                    split[3]
            );
        });

        // 把流变成表
        tableEnv.createTemporaryView("t_bid", source2, Schema.newBuilder()
                .column("bidtime", DataTypes.STRING())
                .column("price", DataTypes.DOUBLE())
                .column("item", DataTypes.STRING())
                .column("supplier_id", DataTypes.STRING())
                .columnByExpression("rt", $("bidtime").toTimestamp())
                .watermark("rt", "rt - interval '1' second")
                .build());// 注册了sql表名，后续可以用sql语句查询

        // 查询:测试数据是否可以正常得到
        //tableEnv.executeSql("select bidtime,price,item,supplier_id,current_watermark(rt) as wm from t_bid").print();

        // 查询：每分钟，计算最近5分钟的交易总额、滑动窗口
//        tableEnv.executeSql("select " +
//                "window_start,window_end,sum(price) as price_amt    "   +
//                "from table (   " +
//                "hop(table t_bid,descriptor(rt),interval '1' minutes,interval '5' minutes    )   " +// 参数：指定表名，指定时间，滑动步长，窗口长度
//                ")  " +
//                "group by window_start,window_end").print();


        // 查询 ;每俩分钟，计算俩分钟的交易总额  滚动窗口
//        tableEnv.executeSql("select " +
//                "window_start,window_end,sum(price) as price_amt    "   +
//                "from table (   " +
//                "tumble(table t_bid,descriptor(rt),interval '2' minutes    )   " +// 参数：指定表名，指定时间，窗口长度
//                ")  " +
//                "group by window_start,window_end").print();


        // 查询 ;每俩分钟，计算总交易总额  累加窗口
        // 超过步长重新计算
        /*tableEnv.executeSql("select " +
                "window_start,window_end,sum(price) as price_amt    "   +
                "from table (   " +
                "cumulate(table t_bid,descriptor(rt),interval '2' minutes, interval '24' hour   )   " +// 参数：指定表名，指定时间，步长，最大长度
                ")  " +
                "group by window_start,window_end").print();*/

        // 每十分钟计算一次，最近十分钟内交易总额最大的三个供应商及其交易参数
        /**
         *          * 2020-04-15 08:07:00.000,2.00,A,supplier1
         *          * 2020-04-15 08:09:00.000,5.00,D,supplier2
         *          * 2020-04-15 08:11:00.000,3.00,B,supplier2
         *          * 2020-04-15 08:13:00.000,1.00,E,supplier1
         *          * 2020-04-15 08:17:00.000,6.00,F,supplier2
         *          * 2020-04-15 08:19:00.000,2.00,G,supplier3
         *          * 2020-04-15 08:21:00.000,5.00,H,supplier1
         *          * 2020-04-15 08:23:00.000,3.00,E,supplier3
         *          * 2020-04-15 08:25:00.000,1.00,Q,supplier3
         *          * 2020-04-15 08:27:00.000,6.00,R,supplier2
         */
        tableEnv.executeSql(
                "select " +
                        "  *  " +
                        "   from  (  " +
                        "   select " +
                        "* over(partition by window_start,window_end order by price_amt desc) as rn " +
                        "from (" +
                        "select " +
                        "window_start," +
                        "window_end," +
                        "sum(price) as price_amt," +
                        "supplier_id," +
                        "couunt(supplier_id) as big_cnt    " +
                        "from table (   " +
                        "tumblr(table t_bid,descriptor(rt),interval '10' minutes   )   " +// 参数：指定表名，指定时间，步长
                        ")  " +
                        "group by window_start,window_end,supplier_id" +
                        ")" +
                        ")" +
                        "where rn <= 2").print();

    }
}
