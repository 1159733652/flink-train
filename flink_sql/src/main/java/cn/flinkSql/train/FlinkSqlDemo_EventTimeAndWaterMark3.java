package cn.flinkSql.train;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Author zhanghongyu
 * @Date 2022/6/29
 * @DESC 表 = > 流
 *
 *
 *      原表的watermark不会丢失
 */
public class FlinkSqlDemo_EventTimeAndWaterMark3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //  建表
        tableEnv.executeSql("CREATE TABLE flink_sql_event   " +
                "   (  " +
                "   guid int,      " +// 物理字段
                "   eventId string,    " +
                "   eventTime timestamp(3),    " +
                /*"   ts as to_timestamp_ltz(eventTime, 3)   " +*/  // 如果事件时间是一个长整型，需要这样转换，并将watermark中的事件事件转换为逻辑表达式
                "   pageId string,    " +
                "   pt as proctime(),    " +// 实时处理时间
                "   watermark for eventTime as   eventTime - interval '1' second  " +// 用watermark for xx 来将一个已定义字段声明成eventTime属性及指定watermark策略
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'kafka',  " +
                "       'topic' = 'flink-sql-exercise-doevent',      " +
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        " +
                "       'properties.group.id' = 'g1',        " +
                "       'scan.startup.mode' = 'earliest-offset',    " +
                "       'format' = 'json',   " +
                "       'json.fail-on-missing-field' = 'false',   " +
                "       'json.ignore-parse-errors' = 'true'    " +
                "   )   "
        );


        // 查询数据
        //tableEnv.executeSql("select guid,eventId,eventTime,pageId,pt,CURRENT_WATERMARK(eventTime) as wm from flink_sql_event").print();


        DataStream<Row> source = tableEnv.toDataStream(tableEnv.from("flink_sql_event"));

        source.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value + "=>" + ctx.timerService().currentWatermark());
            }
        }).print();

        env.execute();

//        // 打印表结构
//        tableEnv.executeSql("desc flink_sql_event").print();
//

    }
}
