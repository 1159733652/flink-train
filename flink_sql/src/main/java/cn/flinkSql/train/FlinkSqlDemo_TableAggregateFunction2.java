package cn.flinkSql.train;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author zhanghongyu
 * @Date 2022/7/6
 * @DESC
 */
public class FlinkSqlDemo_TableAggregateFunction2 {
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
                .flatAggregate(call(MyTop2.class,row($("id"),$("gender"),$("score"))))
                .select($("id"),$("gender"),$("score"))
                .execute().print();
    }


    public static class MyAccu{
        public @DataTypeHint("ROW<id INT,rgender STRING,score DOUBLE>") Row first_row;
        public @DataTypeHint("ROW<id INT,rgender STRING,score DOUBLE>") Row second_row;
    }

    @FunctionHint(input = @DataTypeHint("ROW<id INT,gender STRING,score DOUBLE>") ,output = @DataTypeHint("ROW<id INT,rgender STRING,score DOUBLE>"))
    public static class MyTop2 extends TableAggregateFunction<Row,MyAccu>{

        @Override
        public MyAccu createAccumulator() {
            MyAccu myAccu = new MyAccu();
            myAccu.first_row = null;
            myAccu.second_row = null;
            return myAccu;
        }


        /**
         * 累加更新逻辑
         * @param acc 累加器
         * @param row 传入的一行数据
         */
        public void accumulate(MyAccu acc, Row row) {
            double score = (double) row.getField(2);
            if (null == acc.first_row || score > (double) acc.first_row.getField(2)) {
                acc.second_row = acc.first_row;
                acc.first_row = row;
            } else if (null == acc.second_row  || score > (double) acc.second_row.getField(2)) {
                acc.second_row = row;
            }
        }

        public void merge(MyAccu acc, Iterable<MyAccu> it) {
            for (MyAccu otherAcc : it) {
                accumulate(acc, otherAcc.first_row);
                accumulate(acc, otherAcc.second_row);
            }
        }

        /**
         * 输出结果：可以输出多行和多列
         * @param acc
         * @param out
         */
        public void emitValue(MyAccu acc, Collector<Row> out) {
            if (null != acc.first_row) {
                out.collect(acc.first_row);
            }
            if (null !=acc.second_row) {
                out.collect(acc.second_row);
            }
        }

    }
}
