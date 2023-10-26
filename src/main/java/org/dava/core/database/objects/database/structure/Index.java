package org.dava.core.database.objects.database.structure;

import org.dava.common.TypeUtil;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

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
        else if ( TypeUtil.isNumericClass(column.getType()) ) {
            return findIndexPathForNumber(
                databaseRoot,
                tableName,
                partition,
                column.getName(),
                new BigDecimal( value.toString() )
            );
        }
        return buildColumnPath(databaseRoot, tableName, partition, column.getName());
    }

    public static String buildIndexYearFolderForDate(String databaseRoot, String tableName, String partition, String columnName, String year) {
        return buildColumnPath(databaseRoot, tableName, partition, columnName) + "/" + year;
    }

    public static String buildIndexPathForDate(String databaseRoot, String tableName, String partition, String columnName, LocalDate localDate) {
        return buildColumnPath(databaseRoot, tableName, partition, columnName) + "/" + localDate.getYear() + "/" + localDate + ".index";
    }

    public static String findIndexPathForNumber(String databaseRoot, String tableName, String partition, String columnName, BigDecimal value) {
        File columnDirectory = new File(buildColumnPath(databaseRoot, tableName, partition, columnName));
        List<File> numberFolders = FileUtil.getSubFolders(
            columnDirectory.getPath()
        );

        File destinationFolder = columnDirectory;
        while (!numberFolders.isEmpty()) {
            BigDecimal folderBd = new BigDecimal(numberFolders.get(0).getName().substring(1));

            boolean lessThan = value.compareTo(folderBd) < 0;
            if (lessThan) {
                destinationFolder = numberFolders.stream()
                    .filter(file -> file.getName().charAt(0) == '-')
                    .findFirst()
                    .orElse(columnDirectory);
            }
            else {
                destinationFolder = numberFolders.stream()
                    .filter(file -> file.getName().charAt(0) == '+')
                    .findFirst()
                    .orElse(columnDirectory);
            }


            numberFolders = FileUtil.getSubFolders(
                destinationFolder.getPath()
            );
        }

        return destinationFolder.getPath();
    }

    public static String buildColumnPath(String databaseRoot, String tableName, String partition, String columnName) {
        return databaseRoot + "/" + tableName + "/indecis_" + partition + "/" + columnName;
    }

}
