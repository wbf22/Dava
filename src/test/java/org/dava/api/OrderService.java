package org.dava.api;

import java.util.List;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Mode;

public class OrderService {





    public List<Order> getOrder() {
        Database database = new Database("/db", List.of(Order.class), List.of(Mode.INDEX_ALL));
        OrderRepository repository = new OrderRepository(database);


        return repository.getOrders(null);
    }


}
