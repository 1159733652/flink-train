package cn.flinkSql.train;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/7/1
 * @DESC mysql cdc 连接器测试使用
 *      cdc连接器主要是用作与源用，而不是目标表
 *      不适合做实时数仓，代价过大，适用于实时数据更新
 *
 */
public class FlinkSqlDemo_MsqlcdcConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建表
        tableEnv.executeSql("CREATE TABLE flink_scorce   " +
                "   (  " +
                "   id int,      " +// 物理字段
                "   name string,      " +
                "   gender string,      " +
                "   score double,      " +
                "   PRIMARY KEY(id) NOT ENFORCED    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'mysql-cdc',  " +
                "       'hostname' = '192.168.136.130',      " +
                "       'port' = '3306',        " +
                "       'username' = 'root',        " +
                "       'password' = '123456',   " +
                "       'database-name' = 'flink',   " +
                "       'table-name' = 'scorce'   " +
                "   )   "
        );

        // 查询
        //tableEnv.executeSql("select * from flink_scorce").print();

        // 查询  性别划分求平均分
        /**
         *      job停掉再启动是不会从保存点checkpoint点恢复，需要手动选择保存点、
         *      自动从保存点恢复是在job没有停掉的时候
         */
        //tableEnv.executeSql("select gender,avg(score) as avg_score from flink_scorce group by gender ").print();


        // 建一个目标表，用来存放查询结果：每种性别中，总分最高的俩个人
        // 建表来映射mysql中的表、
        tableEnv.executeSql("CREATE TABLE flink_rank   " +
                "   (  " +
                "   name string,      " +
                "   gender string,      " +
                "   score_amt double,   " +
                "   rn bigint,    " +
                "   PRIMARY KEY(gender,rn) NOT ENFORCED    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'jdbc',  " +
                "       'url' = 'jdbc:mysql://192.168.136.130:3306/flink',      " +
                "       'table-name' = 'score_rank',        " +
                "       'username' = 'root',        " +
                "       'password' = '123456'   " +
                "   )   "
        );

        // 根据性别姓名，查询最大最小值
        tableEnv.executeSql("insert into flink_rank " +
                "select gender,name,score_amt,rn " +
                "from (" +
                "select gender,name,score_amt,row_number() over(partition by gender order by score_amt desc) as rn " +
                "from (" +
                "select gender,name,sum(score) as score_amt from flink_scorce group by gender,name" +
                ") o1" +
                ") o2 " +
                "where rn <= 2");

    }
}
