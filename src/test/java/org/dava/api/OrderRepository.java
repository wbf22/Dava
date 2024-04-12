package org.dava.api;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.dava.api.Repository;
import org.dava.api.annotations.Query;
import org.dava.core.database.objects.database.structure.Database;

public class OrderRepository extends Repository<Order, String> {



    public OrderRepository(Database database) {
        super(database);
    }


    

    @Query(
        query = "select * from order_table"
    )
    public List<Order> getOrders(Map<String, String> params) {
        return query(params);
    }

    public List<Order> getOrdersByTime(LocalDateTime localDateTime) {
        return findByColumn("time");
    } 

}
