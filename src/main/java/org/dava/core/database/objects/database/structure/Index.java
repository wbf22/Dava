package org.dava.core.database.objects.database.structure;

import org.dava.common.TypeUtil;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

public class Index {



    public static String buildIndexPath(String databaseRoot, String tableName, String partition, Column<?> column, List<File> columnLeaves, String value) {
        return buildIndexRootPath(databaseRoot, tableName, partition, column, columnLeaves, value) + "/" + value + ".index";
    }

    public static String buildIndexPath(String indexRootPath, String value) {
        return indexRootPath + "/" + value + ".index";
    }

    public static String buildIndexRootPath(String databaseRoot, String tableName, String partition, Column<?> column, List<File> columnLeaves, Object value) {
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
                columnLeaves,
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

    public static String findIndexPathForNumber(String databaseRoot, String tableName, String partition, String columnName, List<File> columnLeaves, BigDecimal value) {

        String columnPath = buildColumnPath(databaseRoot, tableName, partition, columnName);
        if (columnLeaves.isEmpty())
            return columnPath;

        List<String> leaves = columnLeaves.stream()
            .map(File::getPath)
            .toList();

        StringBuilder correctPath = null;
        boolean done = false;
        while(!done) {
            String path = leaves.get(0);

            StringBuilder pathSoFar = new StringBuilder(columnPath);
            for (String folder : path.split("/")) {
                pathSoFar.append("/").append(folder);
                BigDecimal folderBd = new BigDecimal(folder.substring(1));

                boolean rightFolderSoFar = ( value.compareTo(folderBd) < 0 && folder.contains("-") )
                    || ( value.compareTo(folderBd) < 0 && folder.contains("+") );

                if (!rightFolderSoFar) {
                    leaves = leaves.stream()
                        .filter(leaf -> leaf.contains(pathSoFar.toString()))
                        .toList();
                    done = false;
                    break;
                }
                else {
                    done = true;
                    correctPath = pathSoFar;
                }
            }
        }

        return correctPath.toString();
    }

    public static String buildColumnPath(String databaseRoot, String tableName, String partition, String columnName) {
        return databaseRoot + "/" + tableName + "/META_" + partition + "/" + columnName;
    }

}
