package org.dava.core.sql.parsing;

import java.util.List;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;
import org.dava.core.sql.Select;

public class SqlService {

    // TODO if a 'limit' statement is included, make sure the value is less than Integer.MAX_VALUE.
    // Also if a limit is ever applied in another part of Dava other than custom queries we'll also want
    // to check the value there.

    public static Select parse(String query, Table<?> table) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parse'");
    }



}
