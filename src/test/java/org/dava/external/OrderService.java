package org.dava.external;

import java.util.List;

public class OrderService {



    public List<Order> getOrder() {
        OrderRepository repository = new OrderRepository();


        return repository.getOrders(null);
    }


}
