package cn.flinkSql.bean;

/**
 * @Author zhanghongyu
 * @Date 2022/7/5
 * @DESC    订单表
 */
public class Order {
    // 订单 币种  金额  时间戳
    int orderId;
    String currency;
    double price;
    long orderTime;

    public Order() {
    }

    public Order(int orderId, String currency, double price, long orderTime) {
        this.orderId = orderId;
        this.currency = currency;
        this.price = price;
        this.orderTime = orderTime;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(long orderTime) {
        this.orderTime = orderTime;
    }
}
