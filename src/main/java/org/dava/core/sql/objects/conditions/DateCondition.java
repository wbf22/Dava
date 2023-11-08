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
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Long limit, Long offset) {
        return retrieve(
            table,
            parentFilters,
            columnName,
            () -> BaseOperationService.getAllComparingDate(
                table,
                columnName,
                compareYearsFolderToYearDateYearAllRows,
                compareDates,
                date,
                descending
            ),
            (startRow, rowsPerIteration) -> BaseOperationService.getRowsComparingDate(
                table,
                columnName,
                date,
                compareYearsFolderToYearDateYear,
                compareDates,
                startRow,
                startRow + rowsPerIteration,
                descending
            ),
            limit,
            offset
        );
    }


    @Override
    public Long getCountEstimate(Table<?> table) {
        Column<?> columnObj = table.getColumn(columnName);
        if (columnObj.isIndexed()) {
            List<Integer> yearfolders = BaseOperationService.getYearDateFolders(
                table,
                columnName,
                date,
                compareYearsFolderToYearDateYear,
                descending
            );

            String path = Index.buildColumnPath(table.getDatabaseRoot(), table.getTableName(), table.getRandomPartition(), columnName);
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
