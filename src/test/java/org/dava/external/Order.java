package org.dava.external;

import org.dava.external.annotations.PrimaryKey;
import org.dava.external.annotations.Table;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Table
public class Order {
    @PrimaryKey
    private String orderId;
    private BigDecimal total;
    private OffsetDateTime time;

    public Order(String orderId, BigDecimal total, OffsetDateTime time) {
        this.orderId = orderId;
        this.total = total;
        this.time = time;
    }


    /*
            Getter Setter
         */
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }

    public OffsetDateTime getTime() {
        return time;
    }

    public void setTime(OffsetDateTime time) {
        this.time = time;
    }
}
