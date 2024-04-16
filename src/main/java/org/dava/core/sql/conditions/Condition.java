package org.dava.core.sql.conditions;


import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.objects.exception.ExceptionType;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.structure.Column;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public interface Condition {

    boolean filter(Row row);

    List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Integer limit, Long offset);

    /**
     * Used for evaluating AND and OR conditions more efficiently. 
     * @param table
     * @return
     */
    Long getCountEstimate(Table<?> table); // could return real count. but types some like dates give estimate



    /**
     * Gets rows using a special efficient method using the 'getRangeRows' lambda 
     * if a limit and offset are provided, otherwise using 'getAllRows' method.
     * (The 'getAllRows' method probably ought to use an index or something efficient
     * if possible)
     * 
     * @param table
     * @param parentFilters
     * @param columnName
     * @param getAllRows
     * @param getRangeRows
     * @param limit
     * @param offset
     * @return
     */
    default List<Row> retrieve(
        Table<?> table,
        List<Condition> parentFilters,
        String columnName,
        Supplier<Stream<Row>> getAllRows,
        BiFunction<Long, Long, List<Row>> getRangeRows,
        Integer limit,
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
            rows = getLimitedRowsEfficiently(
                table,
                parentFilters,
                limit,
                offset,
                getRangeRows
            );
        }

        return rows;
    }



    /**
     * This method helps retrieve rows more efficiently
     * 
     * <p> The function provided actually gets the rows given a start and end index, but this
     * method manages it to be more efficient.
     * 
     * <p> The way this works, is that typcially when you have a long list of filters in a query,
     * you'll actually need to retrieve more rows than the original range provided. This method adjusts
     * the amount of rows to retrieve based on the number of parent filters. This helps avoid doing multiple
     * table reads while also balancing reading too much.
     * 
     * <p> It uses this equation to determine the number of rows to retrieve initially:
     * <p> #rowToRetrieve = limit * (1.1 + .2 * #parentFilters) / #ofPartitions
     * 
     * @param table
     * @param parentFilters
     * @param limit
     * @param offset
     * @param functionToGetRows
     * @return
     */
    default List<Row> getLimitedRowsEfficiently(
            Table<?> table,
            List<Condition> parentFilters,
            Integer limit,
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


    /**
     * This method limits the size of the returned rows given the long limit and offset values.
     * 
     * It's mostly here to reduce complexity to methods above, but it handles converting these
     * limit and offset values to ints for the subList method.
     * 
     * Intesrtingly, java Lists can't have a size larger than an Integer so maybe we don't really need
     * limit to be a Long. However, in very large tables an offset could be a long. I'm not sure anyone
     * would have more records than max int, but you never know. But they won't be able to retrieve more
     * than max int records at a time. 
     * 
     * 
     * @param rows
     * @param limit
     * @param offset
     * @return
     */
    default List<Row> limit(List<Row> rows, Integer limit, long offset) {
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
