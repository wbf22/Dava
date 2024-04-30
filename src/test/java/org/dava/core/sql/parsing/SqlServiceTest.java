package org.dava.core.sql.parsing;

import java.util.List;

import org.dava.api.Order;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Mode;
import org.dava.core.sql.Select;
import org.junit.jupiter.api.Test;

public class SqlServiceTest {


    @Test
    void select_parse_complex() {
        String query = """
            Select o.id, p.name 
            from order o
            join o.productId = p.id
            where o.msrp > 7.00
            Order by o.msrp
            limit 10
            offset 10;
        """;

        Database db = new Database("db", List.of(Order.class), List.of(Mode.INDEX_ALL), 0);


        Select s = SqlService.parse(query, db.getTableByName("Order"));
    }
    
}
