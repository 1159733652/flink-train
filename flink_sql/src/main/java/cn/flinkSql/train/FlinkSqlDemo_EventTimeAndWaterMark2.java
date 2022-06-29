package cn.flinkSql.train;

import cn.flinkSql.bean.Event;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @Author zhanghongyu
 * @Date 2022/6/29
 * @DESC 表与流之间waterMark传承
 *
 *      流 =》 表的过程中如何传承  事件时间和watermark
 * <p>
 * <p>
 * bigint类型不能直接转成watermark  否则会报错
 * CURRENT_WATERMARK() must be called with a single rowtime attribute argument, but 'BIGINT NOT NULL' cannot be a time attribute.
 */
public class FlinkSqlDemo_EventTimeAndWaterMark2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //  建表
        //  数据 {"guid":1,"eventId":"e01","eventTime":"1655017433000","pageId":"p001"}
        //{"guid":2,"eventId":"e02","eventTime":"1655017434000","pageId":"p002"}
        //{"guid":3,"eventId":"e03","eventTime":"1655017435000","pageId":"p003"}
        DataStreamSource<String> source1 = env.socketTextStream("192.168.136.130", 9999);

        SingleOutputStreamOperator<Event> source2 = source1.map(s -> JSON.parseObject(s, Event.class)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.eventTime;
                    }
                }));

        // 观察流上的watermark
//        source2.process(new ProcessFunction<Event, String>() {
//            @Override
//            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
//                long wm = ctx.timerService().currentWatermark();
//                out.collect(value + " => " + wm);
//            }
//        }).print();


        // 流转表
        // 直接把流转成表会丢失watermark
        //tableEnv.createTemporaryView("t_event",source2);


        //  查询
        // 查询数据
        // 此时watermark水位线报错
        //tableEnv.executeSql("select guid,eventId,eventTime,pageId,CURRENT_WATERMARK(eventTime) as wm from t_event").print();


        // 测试验证watermrk的丢失
        // 查看下流转成表后是否还存在水位线
        // 将当前得到的表再转换为流，输出水位线看是否存在
//        Table t_event = tableEnv.from("t_event");
//        DataStream<Row> source3 = tableEnv.toDataStream(t_event);
//        source3.process(new ProcessFunction<Row, String>() {
//            @Override
//            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
//                out.collect(value + "=>" + ctx.timerService().currentWatermark());
//            }
//        }).print();


        // 可以在  流 转 表 时，显式声明watermark策略
        tableEnv.createTemporaryView("t_event", source2, Schema.newBuilder()
                .column("guid", DataTypes.INT())
                .column("eventId", DataTypes.STRING())
                .column("eventTime", DataTypes.BIGINT())
                .column("pageId", DataTypes.STRING())
                //.columnByExpression("ts","to_timestamp_ltz(eventTime,3)")// 重新利用一个bigint转成timestamp，作为事件时间属性
                .columnByMetadata("ts", DataTypes.TIMESTAMP(3), "rowtime") // 利用底层流连接器暴漏的rowtime元数据（代表的就是底层流中每条数据的eventTime），声明成字段时间属性


                //.watermark("ts", "rt - interval '1' second ")// 相当于重新生成表的watermark策略，与之前流中的水位线没有关系
                .watermark("ts", "source_watermark()")// 声明watermark策略直接引用底层流的watermark
                .build());


        // 查询
        tableEnv.executeSql("select guid,eventId,eventTime,pageId,ts,CURRENT_WATERMARK(ts) as wm from t_event").print();
        env.execute();
    }
}
