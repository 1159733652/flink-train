package cn.flinkSql.train;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/29
 * @DESC    kafka连接器细节
 */
public class FlinkSqlDemo_KafkaConnectorDetail {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //  建表
        /**
         *  对应的kafka数据，
         *      key：{"k1":100,"k2":200}
         *      vlaue：{"guid":1,"eventId":"e01","eventTime":"1655017433000","pageId":"p001"}
         *      headers：
         *          h1 zzz
         *          h2 ttt
         */
        tableEnv.executeSql("CREATE TABLE table_kafka_connector   " +
                "   (  " +
                "   guid int,      " +// 物理字段
                "   eventId string,    " +
                "   eventTime bigint,    " +
                "   pageId string,    " +
                "   k1 int,    " +
                "   k2 int,    " +
                "   rec_ts timestamp(3) metadata from 'timestamp',    " +
                "   `offset` bigint metadata,    " +
                "   headers map<string,bytes> metadata,    " +
                "   pt as proctime(),    " +// 实时处理时间
                "   rt as to_timestamp_ltz(eventTime,3),    " +
                "   watermark for rt as   rt - interval '0.001' second  " +// 用watermark for xx 来将一个已定义字段声明成eventTime属性及指定watermark策略
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'kafka',  " +
                "       'topic' = 'flink-sql-kafka',      " +
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        " +
                "       'properties.group.id' = 'testGroup',        " +
                "       'scan.startup.mode' = 'earliest-offset',    " +
                "       'key.format' = 'json',   " +
                "       'key.fields' = 'k1;k2',   " +
                //"       'key.fileds-prefix' = '',   " +
                "       'value.format' = 'json',   " +
                "       'value.fields-include' = 'EXCEPT_KEY',   " +
                "       'value.json.fail-on-missing-field' = 'false',   " +
                "       'key.json.ignore-parse-errors' = 'true'    " +
                "   )   "
        );


        tableEnv.executeSql("select guid,eventId,eventTime,pageId,k1,k2,`offset`,rt,rec_ts,cast(headers['h1'] as string) as h1,cast(headers['h2'] as string) as h2 from table_kafka_connector").print();
    }
}
