package org.dava.core.sql.conditions;

import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;

import java.util.List;

public class All implements Condition {
    @Override
    public boolean filter(Row row) {
        return true;
    }

    @Override
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Long limit, Long offset) {
        return getRowsLimited(
            table,
            parentFilters,
            limit,
            offset,
            (startRow, endRow) -> BaseOperationService.getRowsFromTable(
                table,
                startRow,
                endRow
            )
        );
    }

    @Override
    public Long getCountEstimate(Table<?> table) {
        return null;
    }
}
