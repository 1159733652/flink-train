package cn.flinkSql.bean;

/**
 * @Author zhanghongyu
 * @Date 2022/6/30
 * @DESC
 */
public class UpsertBean {
    public int id;
    public String gender;

    public UpsertBean() {
    }

    public UpsertBean(int id, String gender) {
        this.id = id;
        this.gender = gender;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }
}
