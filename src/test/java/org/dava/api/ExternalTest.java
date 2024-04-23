package org.dava.api;

import org.dava.core.database.service.structure.Database;
import org.junit.jupiter.api.Test;

public class ExternalTest {



    @Test
    void test_repository_query() {
        OrderRepository repo = new OrderRepository(new Database(null, null, null));
        repo.getOrders(null);
    }


}
