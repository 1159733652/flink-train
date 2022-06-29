package cn.flinkSql.train;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/29
 * @DESC    事件窗口和水位线
 *
 *      只有 TMIESTAMP 或 TMIESTAMP_LTZ 类型的字段可以被声明为rowtime（事件时间属性）
 */
public class FlinkSqlDemo_EventTimeAndWaterMark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        // 打印表结构
        tableEnv.executeSql("desc flink_sql_event").print();

        // 查询数据
        tableEnv.executeSql("select guid,eventId,eventTime,pageId,pt,CURRENT_WATERMARK(eventTime) as wm from flink_sql_event").print();

    }
}
