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
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Integer limit, Long offset) {
        return getLimitedRowsEfficiently(
            table,
            parentFilters,
            limit,
            offset,
            (startRow, endRow) -> BaseOperationService.getRowsFromTableWithoutIndices(
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
