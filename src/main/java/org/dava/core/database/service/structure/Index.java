package org.dava.core.database.service.structure;

import org.dava.core.common.HashUtil;
import org.dava.core.common.TypeUtil;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;

import static org.dava.core.common.Checks.safeCast;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        if ( Index.isNumericallyIndexed(column.getType()) ) {
            List<File> columnLeaves = table.getLeafList(partition, column.getName());
            BigDecimal valueBd = null;

            if ( value instanceof Date<?> || Date.isDateSupportedDateType(column.getType()) ) {
                Date<?> date = (value instanceof Date<?> dateFromValue)? dateFromValue : Date.ofOrLocalDateOnFailure(value.toString(), column.getType());
                valueBd = date.getMillisecondsSinceTheEpoch();
            }
            else {
                valueBd = new BigDecimal( value.toString() );
            }

            return findIndexPathForNumber(
                databaseRoot,
                table.getTableName(),
                partition,
                column.getName(),
                columnLeaves,
                valueBd
            );
        }
        return buildColumnPath(databaseRoot, table.getTableName(), partition, column.getName());
    }

    public static String buildIndexYearFolderForDate(Table<?> table, String partition, String columnName, String year) {
        return buildColumnPath(table.getDatabaseRoot(), table.getTableName(), partition, columnName) + "/" + year;
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
            path = path.replace(columnPath, "");
            if (path.length() > 0) {
                path = path.substring(1);
                StringBuilder pathSoFar = new StringBuilder(columnPath);
                for (String folder : path.split("/")) {
                    pathSoFar.append("/").append(folder);
                    BigDecimal folderBd = new BigDecimal(folder.substring(1));

                    boolean correctFolderSoFar = ( folderBd.compareTo(value) > 0 && folder.contains("-") )
                        || ( folderBd.compareTo(value) <= 0 && folder.contains("+") );

                    if (!correctFolderSoFar) {
                        leaves = leaves.stream()
                            .filter(leaf -> !leaf.contains(pathSoFar.toString()))
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
            else {
                return columnPath;
            }
        }

        return correctPath.toString();
    }

    public static String buildColumnPath(String databaseRoot, String tableName, String partition, String columnName) {
        return databaseRoot + "/" + tableName + "/META_" + partition + "/" + columnName;
    }

    public static String getParitionFromPath(String databaseRoot, String tableName, String path) {
        String pathStart = databaseRoot + "/" + tableName;
        return path.replace(pathStart, "").split("/")[1].replace("META_", "");
    }



    public static Object prepareValueForIndexName(Object value, Column<?> column) {

        // if it's a date get the milliseconds since the epoch version
        if (Date.isDateSupportedDateType(column.getType() )) {

            if ( Date.isDateSupportedDateType( value.getClass() ) )
                value = safeCast(value, Date.class).getMillisecondsSinceTheEpoch();
            else
                value = Date.ofOrLocalDateOnFailure(
                    value.toString(),
                    column.getType()
                ).getMillisecondsSinceTheEpoch();
        }

        // limit file name less than 255 bytes for ext4 file system
        String strValue = value.toString();
        if (strValue.length() > 50  || strValue.contains(";")) {
            value = HashUtil.hashToUUID(strValue.getBytes(StandardCharsets.UTF_8));
        }

        return value;
    }



    public static boolean isNumericallyIndexed(Class<?> type) {
        return TypeUtil.isDate(type) || TypeUtil.isNumericClass(type);
    }
}
