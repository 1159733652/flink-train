package cn.flinkSql.bean;

import org.apache.kafka.common.protocol.types.Field;

/**
 * @Author zhanghongyu
 * @Date 2022/6/30
 * @DESC
 */
public class UpsertBean2 {
    int id;
    String name;

    public UpsertBean2() {
    }

    public UpsertBean2(int id, String name) {
        this.id = id;
        this.name = name;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
