package cn.flinkSql.train;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.util.Locale;

/**
 * @Author zhanghongyu
 * @Date 2022/7/6
 * @DESC    自定义标量函数
 *
 *              字符串变大写案例
 */
public class FlinkSqlDemo_CustomScalarFunction {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 创建表
        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                Row.of("aaa"),
                Row.of("bbb"),
                Row.of("ccc")
        );

        tableEnv.createTemporaryView("t",table);

        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("myupper",MyUpper.class);

        // 注册后就可以在sql中使用
        tableEnv.executeSql("select myupper(name) as Up_name from t ").print();
    }



    public static class MyUpper extends ScalarFunction{
        public String eval(String str) {
            return str.toUpperCase();
        }
    }
}


