package cn.flinkSql.train;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author zhanghongyu
 * @Date 2022/6/28
 * @DESC    catalog 详细
 */
public class FlinkSqlDemo2_CatalogDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 环境创建之初，底层会自动初始化一个元数据空间实现对象，default_catalog => GenericInMemoryCatalog
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // hive配置文件路径
        String hiveConfigPath = "conf/hiveconf";
        // 创建一个hive元数据空间实现对象
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", hiveConfigPath);
        // 将hive元数据空间注册到环境中
        tableEnv.registerCatalog("hivecatalog",hiveCatalog);


        tableEnv.executeSql("CREATE TABLE t_kafka   " +
                "   (  " +
                "   id int,      " +
                "   name string,    " +
                "   age int,    " +
                "   gender string  " +
                "   )  " +
                "   WITH  (          " +
                "       'connector' = 'kafka',  "+
                "       'topic' = 'flink-sql-demo01',      "+
                "       'properties.bootstrap.servers' = '192.168.136.130:9092',        "+
                "       'properties.group.id' = 'g1',        "+
                "       'scan.startup.mode' = 'earliest-offset',    "+
                "       'format' = 'json',   " +
                "       'json.fail-on-missing-field' = 'false',   " +
                "       'js on.ignore-parse-errors' = 'true'    " +
                "   )   "
        );


        //tableEnv.executeSql("select * from t_kafka").print();

        //tableEnv.listCatalogs();


        // 查看默认的元数据空间
        tableEnv.executeSql("show catalogs").print();
        tableEnv.executeSql("use catalog default_catalog");
        tableEnv.executeSql("show databases").print();
        tableEnv.executeSql("use default_database");
        tableEnv.executeSql("show tables").print();

        System.out.println("===========================================");

        // 查看hive的元数据空间
        tableEnv.executeSql("use catalog hivecatalog");
        tableEnv.executeSql("show databases").print();
        tableEnv.executeSql("use `default`");  //默认default，不需要use
        tableEnv.executeSql("show tables").print();
    }
}
