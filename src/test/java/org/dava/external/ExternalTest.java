package org.dava.external;

import org.junit.jupiter.api.Test;

public class ExternalTest {



    @Test
    void test_repository_query() {
        OrderRepository repo = new OrderRepository();
        repo.getOrders(null);
    }


}
