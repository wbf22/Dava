package org.dava.core.sql.objects.conditions;


import org.dava.common.ListUtil;
import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public interface Condition {

    boolean filter(Row row);

    List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset);

    Long getCountEstimate(Database database, String from); // could return real count. but types some like dates give estimate


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
        long startRow = 0;
        // get rows based off limit + plus some. More if there are lots of parent filters. Then divide by number of partitions
        long rowsPerIteration = (long) (limit * (1.1 + .2 * parentFilters.size())) / database.getTableByName(from).getPartitions().size();
        rowsPerIteration = (rowsPerIteration == 0)? 1 : rowsPerIteration;

        boolean done = false;
        while (!done) {
            List<Row> retrieved = functionToGetRows.apply(startRow, startRow + rowsPerIteration);
            int size = retrieved.size();

            retrieved = retrieved.stream()
                    .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)))
                    .toList();
            startRow += size;
            rows.addAll(retrieved);
            done = (rows.size() >= limit + offset) || size < rowsPerIteration;
        }

        return limit(rows, limit, offset);
    }

    default List<Row> limit(List<Row> rows, long limit, long offset) {
        if (rows.size() > limit + offset) {
            return rows.subList(Math.toIntExact(offset), Math.toIntExact(limit));
        }
        else {
            if (offset > rows.size())
                return new ArrayList<>();
            return rows.subList(Math.toIntExact(offset), rows.size());
        }
    }

}
