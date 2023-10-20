package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.dates.Date;

import java.time.LocalDate;

public class Index {



    public static String buildIndexPath(String databaseRoot, String tableName, String partition, Column<?> column, String value) {
        return buildIndexRootPath(databaseRoot, tableName, partition, column, value) + "/" + value + ".index";
    }

    public static String buildIndexPath(String indexRootPath, String value) {
        return indexRootPath + "/" + value + ".index";
    }

    public static String buildIndexRootPath(String databaseRoot, String tableName, String partition, Column<?> column, Object value) {
        if ( value instanceof Date<?> || Date.isDateSupportedDateType(column.getType()) ) {
            Date<?> date = (value instanceof Date<?> dateFromValue)? dateFromValue : Date.of(value.toString(), column.getType());
            return buildIndexYearFolderForDate(databaseRoot, tableName, partition, column.getName(), date.getYear().toString());
        }
        return buildColumnPath(databaseRoot, tableName, partition, column.getName());
    }

    public static String buildIndexYearFolderForDate(String databaseRoot, String tableName, String partition, String columnName, String year) {
        return buildColumnPath(databaseRoot, tableName, partition, columnName) + "/" + year;
    }

    public static String buildIndexPathForDate(String databaseRoot, String tableName, String partition, String columnName, LocalDate localDate) {
        return buildColumnPath(databaseRoot, tableName, partition, columnName) + "/" + localDate.getYear() + "/" + localDate + ".index";
    }

    public static String buildColumnPath(String databaseRoot, String tableName, String partition, String columnName) {
        return databaseRoot + "/" + tableName + "/indecis_" + partition + "/" + columnName;
    }



}
