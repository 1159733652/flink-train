package cn.flinkSql.train;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author zhanghongyu
 * @Date 2022/7/6
 * @DESC
 */
public class FlinkSqlDemo_TableAggregateFunction {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 创建表
        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("gender", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                ),
                Row.of(1,"male",80),
                Row.of(2,"male",90),
                Row.of(3,"male",100),
                Row.of(4,"female",82),
                Row.of(5,"female",92),
                Row.of(6,"female",98)
        );

        tableEnv.createTemporaryView("t",table);


        // 用一个聚合函数直接求出每种性别中分数最高的俩个成绩
        table.groupBy($("gender"))
                .flatAggregate(call(MyTop2.class,$("score")))
                .select($("gender"),$("score_top"),$("rank_num"))
                .execute().print();
    }


    public static class MyAccu{
        public double firstV;
        public double secondV;
    }

    @FunctionHint(output = @DataTypeHint("ROW<score_top DOUBLE,rank_num INT>"))
    public static class MyTop2 extends TableAggregateFunction<Row,MyAccu>{

        @Override
        public MyAccu createAccumulator() {
            MyAccu myAccu = new MyAccu();
            myAccu.firstV = Double.MIN_VALUE;
            myAccu.secondV = Double.MIN_VALUE;
            return myAccu;
        }


        /**
         * 累加更新逻辑
         * @param acc 累加器
         * @param score 传入的分数
         */
        public void accumulate(MyAccu acc, Double score) {
            if (score > acc.firstV) {
                acc.secondV = acc.firstV;
                acc.firstV = score;
            } else if (score > acc.secondV) {
                acc.secondV = score;
            }
        }

        public void merge(MyAccu acc, Iterable<MyAccu> it) {
            for (MyAccu otherAcc : it) {
                accumulate(acc, otherAcc.firstV);
                accumulate(acc, otherAcc.secondV);
            }
        }

        /**
         * 输出结果：可以输出多行和多列
         * @param acc
         * @param out
         */
        public void emitValue(MyAccu acc, Collector<Row> out) {
            // emit the value and rank
            if (acc.firstV != Double.MIN_VALUE) {
                out.collect(Row.of(acc.firstV,1));
            }
            if (acc.secondV != Double.MIN_VALUE) {
                out.collect(Row.of(acc.secondV,1));
            }
        }

    }
}
