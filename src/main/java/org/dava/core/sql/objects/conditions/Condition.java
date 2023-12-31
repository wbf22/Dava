package org.dava.core.sql.objects.conditions;


import org.dava.core.database.objects.database.structure.Column;
import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.service.BaseOperationService;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public interface Condition {

    boolean filter(Row row);

    List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Long limit, Long offset);

    Long getCountEstimate(Table<?> table); // could return real count. but types some like dates give estimate




    default List<Row> retrieve(
        Table<?> table,
        List<Condition> parentFilters,
        String columnName,
        Supplier<Stream<Row>> getAllRows,
        BiFunction<Long, Long, List<Row>> getRangeRows,
        Long limit,
        Long offset
    ) {
        List<Row> rows;
        boolean allRows = limit == null && offset == null;
        if (allRows) {
            rows = getAllRows.get()
                .filter(row -> {
                    if (parentFilters.isEmpty())
                        return true;
                    else
                        return parentFilters.parallelStream().allMatch(condition -> condition.filter(row));
                })
                .toList();
        }
        else {
            rows = getRowsLimited(
                table,
                parentFilters,
                limit,
                offset,
                getRangeRows
            );
        }

        return rows;
    }


    default Stream<Row> getRows(
        Table<?> table,
        String columnName,
        Object value,
        List<Condition> parentFilters
    ) {
        return table.getPartitions().parallelStream()
            .flatMap(partition ->
                BaseOperationService.getRowsFromTable(table, columnName, value.toString(), 0, null).stream()
                    .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)))
            );
    }

    default List<Row> getRowsLimited(
            Table<?> table,
            List<Condition> parentFilters,
            Long limit,
            Long offset,
            BiFunction<Long, Long, List<Row>> functionToGetRows
    ) {
        offset = (offset == null)? 0 : offset;
        Long endRow;
        long rowsPerIteration = 0L;
        if (limit != null) {
            // get rows based off limit + plus some. More if there are lots of parent filters. Then divide by number of partitions
            rowsPerIteration = (long) (limit * (1.1 + .2 * parentFilters.size())) / table.getPartitions().size();
            rowsPerIteration = (rowsPerIteration == 0)? 1 : rowsPerIteration;
            endRow = rowsPerIteration;
        }
        else {
            endRow = null;
        }

        List<Row> rows = new ArrayList<>();


        long startRow = 0;
        boolean done = false;
        while (!done) {
            List<Row> retrieved = functionToGetRows.apply(startRow, endRow);

            retrieved = retrieved.stream()
                    .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)))
                    .toList();

            startRow += retrieved.size();
            endRow = (endRow == null)? null : startRow + rowsPerIteration;
            rows.addAll(retrieved);
            if (limit == null) {
                done = true;
            }
            else {
                done = (rows.size() >= limit + offset) || retrieved.size() < rowsPerIteration;
            }
        }

        return limit(rows, limit, offset);
    }

    default List<Row> limit(List<Row> rows, Long limit, long offset) {
        if (limit != null) {
            if (rows.size() > limit + offset) {
                return rows.subList(Math.toIntExact(offset), Math.toIntExact(limit));
            }
        }

        if (offset > rows.size())
            return new ArrayList<>();

        return rows.subList(Math.toIntExact(offset), rows.size());
    }

}
