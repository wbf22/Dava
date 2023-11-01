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

public class Before<T extends Date<?>> extends DateCondition<T> {



    public Before(String columnName, T date, boolean descending) {
        this.columnName = columnName;
        this.date = date;
        this.descending = descending;
        this.compareYearsFolderToYearDateYearAllRows = (folderYear, dateYear) -> folderYear < dateYear;
        this.compareYearsFolderToYearDateYear = (folderYear, dateYear) -> folderYear <= dateYear;
        this.compareDates = Date::isBefore;
    }

    @Override
    public boolean filter(Row row) {
        String value = row.getValue(columnName).toString();

        Date<?> rowDate = Date.of(value, date.getType());

        return rowDate.isAfter(date);
    }

}
