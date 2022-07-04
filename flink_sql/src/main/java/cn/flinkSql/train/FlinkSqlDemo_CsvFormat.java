package cn.flinkSql.train;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/29
 * @DESC    csv格式代码示例
 */
public class FlinkSqlDemo_CsvFormat {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建表(数据源表）
        tableEnv.executeSql("CREATE TABLE t_csv   " +
                "   (  " +
                "   id int,      " +
                "   name string,    " +
                "   age string    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'filesystem',  " +
                "       'path' = 'data/csv/',      " +
                "       'format' = 'csv',   " +
                "       'csv.ignore-parse-errors' = 'true',   " +// 是否忽略解析错误 true为开启
                "       'csv.disable-quote-character' = 'false',   " +
                "       'csv.allow-comments' = 'true',   " +// 是否允许忽略注释
                "       'csv.null-literal' = 'AA',   " +// 将特定字符转换为空值
                "       'csv.quote-character' = '|'   " +
                "   )   "
        );

        tableEnv.executeSql("desc t_csv").print();

        tableEnv.executeSql("select * from t_csv").print();

    }
}
