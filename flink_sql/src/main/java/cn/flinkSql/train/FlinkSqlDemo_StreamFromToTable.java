package cn.flinkSql.train;

import cn.flinkSql.bean.Person;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author zhanghongyu
 * @Date 2022/7/4
 * @DESC
 */
public class FlinkSqlDemo_StreamFromToTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 得到数据流
        DataStreamSource<String> source1 = env.socketTextStream("192.168.136.130", 9999);

        // 数据格式
        // 数据流转换
        // 1,zs,23,male
        SingleOutputStreamOperator<Person> source2 = source1.map(s -> {
            String[] split = s.split(",");
            return new Person(
                    Integer.parseInt(split[0]),
                    split[1],
                    Integer.parseInt(split[2]),
                    split[3]
            );
        });

        // 把流变成表
        tableEnv.createTemporaryView("abc",source2);// 注册了sql表名，后续可以用sql语句查询

        //Table table = tableEnv.fromDataStream(source2);// 得到表对象，后续可以用table api 查询

        // 查询：  每一种性别中年龄最大的3个人信息
        String sql1 = "select  " +
                "id,    " +
                "name,  " +
                "age,   " +
                "tgender,   " +
                "rn " +
                "from   " +
                "(  " +
                "select " +
                "id,  " +
                "name,    " +
                "age,   " +
                "gender" +
                "row_number() over(partition by gender order by age desc ) as rn    " +
                "from abc   " +
                ") o    " +
                "where rn <= 3";

        /**
         *  topn的查询结果，创建为视图，继续查询
         *  方式一
         */
       /* Table tmp = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("tmp",tmp);
        tableEnv.executeSql("select * from tmp ");*/
        //tableEnv.executeSql(sql1);


        /**
         *  topn的查询结果，创建为视图，继续查询
         *  方式二
         */
        String sql2 = "create temporary view topn_view as " +
                "select  " +
                "id,    " +
                "name,  " +
                "age,   " +
                "tgender,   " +
                "rn " +
                "from   " +
                "(  " +
                "select " +
                "id,  " +
                "name,    " +
                "age,   " +
                "gender" +
                "row_number() over(partition by gender order by age desc ) as rn    " +
                "from abc   " +
                ") o    " +
                "where rn <= 3";

        tableEnv.executeSql(sql2);
        tableEnv.executeSql("create temporary view topn_odd as select * from topn_view where age % 2 = 1 ");


        // 创建kafka映射表
        // 建表
        tableEnv.executeSql("CREATE TABLE t_upsert_kafka2   " +
                        "   (  " +
                        "   id int ,      " +// 物理字段
                        "   age int,      " +// 物理字段
                        "   gender string,      " +// 物理字段
                        "   name string,    " +
                        "   rn int,    " +
                        "   primary key(rn,gender) not enforced    " +
                        "   )  " +
                        "   WITH  (          " +
                        "       'connector' = 'upsert-kafka',  " +
                        "       'topic' = 'flink-sql-kafka-topn',      " +
                        "       'properties.bootstrap.servers' = '192.168.136.130:9092',        " +
                        "       'key.format' = 'csv',   " +
                        "       'value.format' = 'csv'   " +
                        "   )   "
        );

        tableEnv.executeSql("insert into t_upsert_kafka2 select * from topn_odd ");

        tableEnv.executeSql("select gender,max(age) as max_age from t_upsert_kafka2 group by gender").print();

       /* // 讲上述查询结果变成流
        // 有delete/update操作，需要将toDataStream换为toChangelogStream
        DataStream<Row> dataStream = tableEnv.toChangelogStream(tableEnv.from("topn_odd"));

        // 打印流
        dataStream.print();


        // 将流变为表
        tableEnv.createTemporaryView("t_do",dataStream);

        // 用sql查询: 每个性别的年龄最大值
        tableEnv.executeSql("select gender,max(age) from t_do group gender ").print();*/

        env.execute();
    }
}
