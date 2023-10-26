package org.dava.core.sql.objects.conditions;


import org.dava.common.Functions;
import org.dava.common.ListUtil;
import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.service.BaseOperationService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface Condition {

    boolean filter(Row row);

    List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset);

    Long getCount(Database database, String from);


    default List<Row> getRowsLimited(
            Database database,
            List<Condition> parentFilters,
            String from,
            Long limit,
            Long offset,
            BiFunction<Long, Long, List<Row>> functionToGetRows
    ) {
        limit = (limit == null)? Long.MAX_VALUE : limit;
        offset = (offset == null)? 0 : offset;

        List<Row> rows = new ArrayList<>();
        long startRow = offset;
        long rowsPerIteration = (long) (limit * (1.1 + .2 * parentFilters.size()));
        boolean done = false;
        while (!done) {
            List<Row> retrieved = functionToGetRows.apply(startRow, startRow + rowsPerIteration);
            int size = retrieved.size();

            retrieved = retrieved.stream()
                    .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)))
                    .toList();
            startRow += size;
            rows.addAll(retrieved);
            done = (rows.size() >= limit) || size < rowsPerIteration;
        }
        return (rows.size() > limit)? ListUtil.limit(rows, limit) : rows;
    }

}
