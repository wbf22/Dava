package org.dava.external;

import org.dava.external.annotations.Query;

import java.util.List;
import java.util.Map;

public class OrderRepository extends Repository<Order, String> {



    @Query(
        query = "select * from order_table"
    )
    public List<Order> getOrders(Map<String, String> params) {
        return query(params);
    }

}
