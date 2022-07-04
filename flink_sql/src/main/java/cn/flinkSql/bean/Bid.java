package cn.flinkSql.bean;

/**
 * @Author zhanghongyu
 * @Date 2022/7/4
 * @DESC
 */
public class Bid {
    String bidtime;
    double price;
    String item;
    String supplier_id;

    public Bid() {
    }

    public Bid(String bidtime, double price, String item, String supplier_id) {
        this.bidtime = bidtime;
        this.price = price;
        this.item = item;
        this.supplier_id = supplier_id;
    }

    public String getBidtime() {
        return bidtime;
    }

    public void setBidtime(String bidtime) {
        this.bidtime = bidtime;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public String getSupplier_id() {
        return supplier_id;
    }

    public void setSupplier_id(String supplier_id) {
        this.supplier_id = supplier_id;
    }

    @Override
    public String toString() {
        return "Bid{" +
                "bidtime='" + bidtime + '\'' +
                ", price=" + price +
                ", item='" + item + '\'' +
                ", supplier_id='" + supplier_id + '\'' +
                '}';
    }
}
