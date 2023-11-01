package org.dava.core.sql.objects.conditions;

import org.dava.common.ListUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.service.BaseOperationService;

import java.util.ArrayList;
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
    public List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset) {

        List<Row> rows;
        if (limit == null && offset == null) {
            Table<?> table = database.getTableByName(from);
            rows = database.getTableByName(from).getPartitions().stream()
                    .flatMap(partition -> {
                        String indexPath = Index.buildIndexPath(database.getRootDirectory(), from, partition, table.getColumn(column), table.getColumnLeaves().get(partition + column), value);
                        return BaseOperationService.getAllRowsFromIndex(indexPath, partition, database, from).stream()
                                .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)));
                    })
                    .toList();
        }
        else {
            rows = getRowsLimited(
                    database,
                    parentFilters,
                    from,
                    limit,
                    offset,
                    (startRow, rowsPerIteration) -> BaseOperationService.getRowsFromIndex(
                            database,
                            from,
                            column,
                            value,
                            startRow,
                            startRow + rowsPerIteration
                    )
            );
        }

        return rows;
    }

    @Override
    public Long getCount(Database database, String from) {
        Table<?> table = database.getTableByName(from);
        return table.getPartitions().stream()
            .map(partition ->
                getCountForIndexPath(
                    Index.buildIndexPath(database.getRootDirectory(), from, partition, table.getColumn(column), table.getColumnLeaves().get(partition + column), value)
                )
            )
            .reduce(Long::sum)
            .orElse(null);

    }
}
