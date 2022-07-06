package cn.flinkSql.train;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @Author zhanghongyu
 * @Date 2022/7/6
 * @DESC 自定义聚合函数
 */
public class FlinkSqlDemo_CustomAggregateFunction {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 创建表
        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("uid", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                Row.of(1,"male",80),
                Row.of(2,"male",90),
                Row.of(3,"female",100)
        );

        tableEnv.createTemporaryView("t",table);

        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("myavg",MyAvg.class);

        // 注册后就可以在sql中使用
        tableEnv.executeSql("select gender,myavg(score) as avg_score from t group by gender").print();
    }


    // 累加器类型
    public static class MyAccumulator{
        public int count;
        public double sum;
    }

    public static class MyAvg extends AggregateFunction<Double,MyAccumulator> {
        public String eval(String str) {
            return str.toUpperCase();
        }

        /**
         * 获取累加器的值
         * @param myAccumulator
         * @return
         */
        @Override
        public Double getValue(MyAccumulator myAccumulator) {
            return myAccumulator.sum / myAccumulator.count;
        }

        /**
         * 创建累加器
         * @return
         */
        @Override
        public MyAccumulator createAccumulator() {
            MyAccumulator myAccumulator = new MyAccumulator();
            myAccumulator.count = 0;
            myAccumulator.sum = 0;
            return myAccumulator;
        }


        /**
         * 进来输入数据后，如何更新累加器
         * @param accumulator
         * @param score
         */
        public void accumulate(MyAccumulator accumulator,Double score) {
            accumulator.count = accumulator.count+1;
            accumulator.sum = accumulator.sum +score;
        }
    }
}


