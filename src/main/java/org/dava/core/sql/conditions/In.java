package org.dava.core.sql.conditions;

import java.util.List;
import java.util.Set;

import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;

public class In implements Condition {

    private Set<String> values;
    private String columnName;

    public In(Set<String> values, String columnName) {
        this.values = values;
        this.columnName = columnName;
    }

    @Override
    public boolean filter(Row row) {
        return values.contains(row.getValue(columnName));
    }

    @Override
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Integer limit, Long offset) {
        return retrieve(
            table,
            parentFilters,
            columnName,
            () -> BaseOperationService.getRowsWithValueInCollection(
                table,
                columnName,
                values,
                0L,
                null,
                true
            ).stream(),
            (startRow, rowsPerIteration) -> BaseOperationService.getRowsWithValueInCollection(
                table,
                columnName,
                values,
                startRow,
                startRow + rowsPerIteration,
                false
            ),
            limit,
            offset
        );
    }

    @Override
    public Long getCountEstimate(Table<?> table) {
        /* 
            Punting on this one. It's unlikely this would be the most limiting condition.
        */ 
        return Long.MAX_VALUE;
    }


}
