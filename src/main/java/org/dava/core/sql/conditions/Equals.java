package org.dava.core.sql.conditions;

import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.structure.*;

import java.util.List;

import static org.dava.core.database.service.BaseOperationService.getCountForIndexPath;

public class Equals implements Condition {

    private String column;
    private String value;

    public Equals(String column, String value) {
        this.column = column;
        this.value = value;
    }

    public static Equals in(String column, String value) {
        return new Equals(column, value);
    }


    @Override
    public boolean filter(Row row) {
        return row.getValue(column).equals(value);
    }

    @Override
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Integer limit, Long offset) {
        return retrieve(
            table,
            parentFilters,
            column,
            () -> table.getPartitions().parallelStream()
                .flatMap(partition ->
                    BaseOperationService.getRowsFromTable(table, column, value, 0, null).stream()
                        .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)))
                ),
            (startRow, rowsPerIteration) -> BaseOperationService.getRowsFromTable(
                table,
                column,
                value,
                startRow,
                startRow + rowsPerIteration
            ),
            limit,
            offset
        );
    }

    @Override
    public Long getCountEstimate(Table<?> table) {
        Column<?> columnObj = table.getColumn(column);
        if (columnObj.isIndexed()) {
            return table.getPartitions().parallelStream()
                .map(partition ->
                         getCountForIndexPath(
                             Index.buildIndexPath(
                                 table,
                                 partition,
                                 column,
                                 value
                             )
                         )
                )
                .reduce(Long::sum)
                .orElse(null);
        }
        else {
            return null;
        }
    }
}
