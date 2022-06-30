package cn.flinkSql.train;

import cn.flinkSql.bean.UpsertBean;
import cn.flinkSql.bean.UpsertBean2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author zhanghongyu
 * @Date 2022/6/30
 * @DESC  mysql作为sink
 */
public class FlinkSqlDemo_JDBCConnector2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 建表来映射mysql中的表、
        tableEnv.executeSql("CREATE TABLE flink_mysql_stu2   " +
                "   (  " +
                "   id int primary key not enforced,      " +
                "   name string,      " +
                //"   age int,      " +
                "   gender string    " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'jdbc',  " +
                "       'url' = 'jdbc:mysql://192.168.136.130:3306/flink',      " +
                "       'table-name' = 'stu2',        " +
                "       'username' = 'hadoop',        " +
                "       'password' = '123456'   " +
                "   )   "
        );


        /**
         * 数据格式
         *      1,male
         *      2,female
         *      3,male
         *      4,female
         */
        DataStreamSource<String> source1 = env.socketTextStream("192.168.136.130", 9998);

        /**
         * 数据格式
         *      1,zs
         *      2,ls
         *      3,ww
         *      4,lh
         */
        DataStreamSource<String> source2 = env.socketTextStream("192.168.136.130", 9999);

        SingleOutputStreamOperator<UpsertBean> bean1 = source1.map(s -> {
            String[] arr = s.split(",");
            return new UpsertBean(Integer.parseInt(arr[0]), arr[1]);
        });


        SingleOutputStreamOperator<UpsertBean2> bean2 = source2.map(s -> {
            String[] arr = s.split(",");
            return new UpsertBean2(Integer.parseInt(arr[0]), arr[1]);
        });

        // 流转表
        tableEnv.createTemporaryView("bean1",bean1);
        tableEnv.createTemporaryView("bean2",bean2);


        tableEnv.executeSql("insert into flink_mysql_stu2 select bean1.id,bean1.gender,bean2.name from bean1 left join bean2 on bean1.id = bean2.id ").print();
    }
}
