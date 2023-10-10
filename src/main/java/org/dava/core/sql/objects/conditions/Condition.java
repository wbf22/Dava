package org.dava.core.sql.objects.conditions;


import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;

import java.util.List;

public interface Condition {

    boolean filter(Row row);

    List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset);

    Long getCount(Database database, String from);

}
