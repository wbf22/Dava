package org.dava.external;

import org.dava.external.annotations.PrimaryKey;
import org.dava.external.annotations.Table;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Table
public class Order {
    @PrimaryKey
    private String orderId;
    private String description;
    private BigDecimal total;
    private BigDecimal discount;
    private OffsetDateTime time;

    public Order(String orderId, String description, BigDecimal total, BigDecimal discount, OffsetDateTime time) {
        this.orderId = orderId;
        this.description = description;
        this.total = total;
        this.discount = discount;
        this.time = time;
    }

    /*
        Getter Setter
     */
    public String getOrderId() {
        return orderId;
    }

    public String getDescription() {
        return description;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public BigDecimal getDiscount() {
        return discount;
    }

    public OffsetDateTime getTime() {
        return time;
    }
}
