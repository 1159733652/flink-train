package cn.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author zhanghongyu
 * @Date 2022/6/24
 * @DESC flink入门程序案例，
 *          使用socket流式读取数据
 *          nc -lk 9999  开启一个socket服务
 *          统计数据流中出现的单词及其个数
 *          是不是没收到一条新数据就收到一个结果
 *          历史以来到这一瞬间全局单词个数
 *          本地运行默认并行度为电脑cpu的逻辑核数，默认并行度可以人为修改
 *          启动前先要开启socket服务，防止挂了
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建编程入口环境
        // 流的处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 设置并行度

        // 通过source算子，把socket数据源为一个dataStream,测试用
        DataStreamSource<String> source = env.socketTextStream("192.168.136.130", 9999);

        // 通过算子对数据进行各种转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 切单词
                String[] split = s.split("\\s+");
                for (String word : split) {
                    // 返回每一对单词
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 分组
        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        // 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyed.sum(1);

        // 通过sink算子，将结果输出
        resultStream.print();

        // 触发程序的提交运行
        env.execute();
    }
}
