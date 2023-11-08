package org.dava.core.database.objects.database.structure;

import org.dava.common.HashUtil;
import org.dava.common.TypeUtil;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.dava.common.Checks.safeCast;

public class Index {



    public static String buildIndexPath(Table<?> table, String partition, String columnName, Object value) {

        Column<?> column = table.getColumn(columnName);
        String folderPath = buildIndexRootPath(table.getDatabaseRoot(), table, partition, column, value);

        return buildIndexPath(folderPath, value, column);
    }

    public static String buildIndexPath(String folderPath, Object value, Column<?> column) {
        Object valueObj = prepareValueForIndexName(value, column);
        return indexPathBypass(folderPath, valueObj);
    }

    public static String indexPathBypass(String folderPath, Object preparedValueForIndexName) {
        return folderPath + "/" + preparedValueForIndexName + ".index";
    }

    public static String buildIndexRootPath(String databaseRoot, Table<?> table, String partition, Column<?> column, Object value) {
        if ( value instanceof Date<?> || Date.isDateSupportedDateType(column.getType()) ) {
            Date<?> date = (value instanceof Date<?> dateFromValue)? dateFromValue : Date.ofOrLocalDateOnFailure(value.toString(), column.getType());
            return buildIndexYearFolderForDate(table, partition, column.getName(), date.getYear().toString());
        }
        else if ( TypeUtil.isNumericClass(column.getType()) ) {
            List<File> columnLeaves = table.getLeafList(partition, column.getName());

            return findIndexPathForNumber(
                databaseRoot,
                table.getTableName(),
                partition,
                column.getName(),
                columnLeaves,
                new BigDecimal( value.toString() )
            );
        }
        return buildColumnPath(databaseRoot, table.getTableName(), partition, column.getName());
    }

    public static String buildIndexYearFolderForDate(Table<?> table, String partition, String columnName, String year) {
        return buildColumnPath(table.getDatabaseRoot(), table.getTableName(), partition, columnName) + "/" + year;
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



    public static Object prepareValueForIndexName(Object value, Column<?> column) {

        // if it's a date get the local data version
        if (Date.isDateSupportedDateType(column.getType() )) {

            if ( Date.isDateSupportedDateType( value.getClass() ) )
                value = safeCast(value, Date.class).getDateWithoutTime();
            else
                value = Date.ofOrLocalDateOnFailure(
                    value.toString(),
                    column.getType()
                ).getDateWithoutTime().toString();
        }

        // limit file name less than 255 bytes for ext4 file system
        value = value.toString();
        byte[] bytes = value.toString().getBytes();
        if (bytes.length > 240) {
            value = HashUtil.hashToUUID(bytes);
        }

        return value;
    }

}
