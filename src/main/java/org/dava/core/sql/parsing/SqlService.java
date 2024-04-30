package org.dava.core.sql.parsing;

import java.util.List;
import java.util.stream.IntStream;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;
import org.dava.core.sql.As;
import org.dava.core.sql.From;
import org.dava.core.sql.Join;
import org.dava.core.sql.Select;

public class SqlService {

    // TODO if a 'limit' statement is included, make sure the value is less than Integer.MAX_VALUE.
    // Also if a limit is ever applied in another part of Dava other than custom queries we'll also want
    // to check the value there.

    public static Select parse(String query, Table<?> table) {
        

        query = query.replace("\n", "").toLowerCase();

        // remove select word
        query = query.substring(7);


        String asStrings = query.split("from")[0];
        List<As> ases = List.of(asStrings.split(",")).stream()
            .map(As::parse)
            .toList();


        String queryTail = query.split("from")[1];

        String fromAndJoins = queryTail.split("where")[0];
        
        String[] joinSplit = fromAndJoins.split("join");

        String fromString = joinSplit[0];
        From from = From.parse(fromString);

        List<Join> joins = IntStream.range(1, joinSplit.length)
            .mapToObj( i -> Join.parse(joinSplit[i]) )
            .toList();
            

        String q = """
            Select o.id, p.name 
            from order o
            join product on o.productId = p.id
            where o.msrp > 7.00
            AND (p.name = 'test' OR p.name = 'test2')
            Order by o.msrp
            limit 10
            offset 10;
        """;


        String where = queryTail.split("where")[1];



        
        return new Select();

    }



}
