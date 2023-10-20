package org.dava.core.sql.objects.conditions;

import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.service.BaseOperationService;

import java.util.List;

public class After <T extends Date<?>> implements Condition {

    private String column;
    private T date;
    private boolean descending;


    public After(String column, T date, boolean descending) {
        this.column = column;
        this.date = date;
        this.descending = descending;
    }

    @Override
    public boolean filter(Row row) {
        String value = row.getValue(column).toString();

        Date<?> rowDate = Date.of(value, date.getType());

        return rowDate.isAfter(date);
    }

    @Override
    public List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset) {
        List<Row> rows;
        if (limit == null && offset == null) {
            rows = BaseOperationService.getAllComparingDate(
                    database,
                    from,
                    column,
                    (yearFolder, dateYear) -> yearFolder > dateYear,
                    Date::isAfter,
                    date
            );
            rows = rows.stream()
                    .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)))
                    .toList();
        }
        else {
            rows = getRowsLimited(
                database,
                parentFilters,
                from,
                limit,
                offset,
                (startRow, rowsPerIteration) -> BaseOperationService.getRowsComparingDate(
                    database,
                    from,
                    column,
                    date,
                    (folderYear, dateYear) -> folderYear >= dateYear,
                    Date::isAfter,
                    startRow,
                    startRow + rowsPerIteration,
                    true
                )
            );
        }

        return rows;
    }

    @Override
    public Long getCount(Database database, String from) {
        return null;
    }
}
