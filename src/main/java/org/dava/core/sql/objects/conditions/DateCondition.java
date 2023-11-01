package org.dava.core.sql.objects.conditions;

import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

import static org.dava.core.database.service.BaseOperationService.getCountForIndexPath;

public abstract class DateCondition<T extends Date<?>> implements Condition {
    protected String columnName;
    protected T date;
    protected BiPredicate<Integer, Integer> compareYearsFolderToYearDateYearAllRows;
    protected BiPredicate<Integer, Integer> compareYearsFolderToYearDateYear;
    protected BiPredicate<Date<?>, Date<?>> compareDates;
    protected boolean descending;


    @Override
    public List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset) {
        List<Row> rows;
        boolean allRows = limit == null && offset == null;
        Column<?> column = database.getTableByName(from).getColumn(columnName);
        if (allRows || !column.isIndexed()) {
            rows = BaseOperationService.getAllComparingDate(
                database,
                from,
                columnName,
                compareYearsFolderToYearDateYearAllRows,
                compareDates,
                date,
                descending
            );
            rows = rows.stream()
                .filter(row -> parentFilters.parallelStream().allMatch(condition -> condition.filter(row)))
                .toList();

            if (!allRows && limit != null)
                return limit(rows, limit, offset);

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
                    columnName,
                    date,
                    compareYearsFolderToYearDateYear,
                    compareDates,
                    startRow,
                    startRow + rowsPerIteration,
                    descending
                )
            );
        }

        return rows;
    }


    @Override
    public Long getCountEstimate(Database database, String from) {
        Table<?> table = database.getTableByName(from);
        Column<?> columnObj = table.getColumn(columnName);
        if (columnObj.isIndexed()) {
            List<Integer> yearfolders = BaseOperationService.getYearDateFolders(
                database,
                from,
                columnName,
                date,
                compareYearsFolderToYearDateYear,
                descending,
                table
            );

            String path = Index.buildColumnPath(database.getRootDirectory(), from, table.getRandomPartition(), columnName);
            path += "/" + yearfolders.get(yearfolders.size() - 1);
            File[] subDirs = FileUtil.listFiles(path);
            path = subDirs[0].getPath();
            File[] subDateIndices = FileUtil.listFiles(path);
            long count = getCountForIndexPath(
                subDateIndices[0].getPath()
            );

            return count * yearfolders.size() * subDirs.length * subDateIndices.length;
        }
        return null;
    }
}
