package org.dava.core.database.service;


import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

/**
 * Class for actually modifying database files.
 */
public class BaseOperationService {




    public static boolean insert(Row row, Database database, Table<?> table, boolean index) {
        // TODO collect data to perform rollback if necessary

        // serialize row
        String partition = table.getRandomPartition();
        byte[] bytes = Row.serialize(table, row.getColumnsToValues()).getBytes(StandardCharsets.UTF_8);

        // get random empty row or use last row in the table
        Long destinationRow = table.getEmptyRow(partition);
        destinationRow = (destinationRow == null || bytes.length > table.getRowLength(destinationRow, partition))?
            table.getSize(partition) : destinationRow;

        IndexRoute route = IndexRoute.of(partition, destinationRow, table);

        // add to table
        String path = table.getTablePath(route.getPartition()) + ".csv";
        try {
            FileUtil.writeBytes(
                path,
                route.getOffsetInTable(),
                bytes
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing row to: " + path,
                e
            );
        }

        // update table row lengths if needed
        table.addRowLengthIfNeeded(bytes.length, destinationRow, partition);

        // update table row count in empties
        Long newSize = table.getSize(route.getPartition()) + 1;
        table.setSize(route.getPartition(), newSize);

        if (index) {
            // add to index csv
            for (Map.Entry<String, String> columnValue : row.getColumnsToValues().entrySet()) {
                String indexPath = Index.buildIndexRootPath(
                    database.getRootDirectory(),
                    table.getTableName(),
                    route.getPartition(),
                    columnValue.getKey()
                );

                addToIndex(indexPath, columnValue.getValue(), destinationRow);
            }
        }


        return true;
    }

    public <T> boolean update(Row row, IndexRoute primaryKeyRoute, Table<T> table) {

//        BigInteger rowOffset = calculateStartOfRow(primaryKeyRoute.getRow(), table);
//        try {
//            FileUtil.writeFile(
//                table.getTablePath(primaryKeyRoute),
//                rowOffset.longValue(),
//                ""
//            );
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        return true;
    }



    /*
        Indices
     */

    public static List<Row> getAllRowsFromIndex(Database database, String tableName, String columnName, String value) {
        Table<?> table = database.getTableByName(tableName);

        return table.getPartitions().stream()
            .flatMap(partition -> {
                String path = Index.buildIndexPath(database.getRootDirectory(), tableName, partition, columnName, value);

                long[] lines = getLongs(
                    path,
                    true,
                    null,
                    null
                );
                long[] countRemoved = Arrays.copyOfRange(lines, 1, lines.length);

                return Arrays.stream(countRemoved).sequential()
                    .mapToObj(line -> getLineFromPartition(partition, table, line))
                    .map(line -> new Row(line, table));
            })
            .toList();
    }

    public static List<Row> getRowsFromIndex(
        Database database,
        String tableName,
        String columnName,
        String value,
        long startRow,
        long endRow
    ) {
        Table<?> table = database.getTableByName(tableName);

        return table.getPartitions().stream()
            .flatMap(partition -> {
                String path = Index.buildIndexPath(database.getRootDirectory(), tableName, partition, columnName, value);

                long[] lines = getLongs(
                        path,
                        false,
                        8 + startRow * 8,
                        (int) ((endRow - startRow) * 8)
                );


                return Arrays.stream(lines).sequential()
                    .mapToObj(line -> getLineFromPartition(partition, table, line))
                    .map(line -> new Row(line, table));
            })
            .toList();
    }

    private static long[] getLongs(String filePath, boolean allLines, Long startByte, Integer numBytes) {
        try {

            byte[] bytes = (allLines)? FileUtil.readBytes(filePath)
                : FileUtil.readBytes(filePath, startByte, numBytes);

            return TypeToByteUtil.byteArrayToLongArray(bytes);
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error reading file for path: " + filePath,
                e
            );
        }
    }

    public static String getLineFromPartition(String partition, Table<?> table, long indexRow) {

        IndexRoute entry = IndexRoute.of(partition, indexRow, table);
        try {
            return FileUtil.readFile(
                table.getTablePath(entry.getPartition()),
                entry.getOffsetInTable(),
                entry.getLengthInTable()
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error reading table csv file for table: " + table.getTableName(),
                e
            );
        }
    }

    public static Long getCountForIndexPath(String path) {
        try {
            return TypeToByteUtil.byteArrayToLong(
                FileUtil.readBytes(path, 0, 8)
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error getting index count in: " + path,
                e
            );
        }

    }

    public static void writeCountForIndexPath(String path, long count) {
        try {
            FileUtil.writeBytes(
                path,
                0,
                TypeToByteUtil.longToByteArray(
                    count
                )
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing index count in: " + path,
                e
            );
        }

    }


    public static void addToIndex(String folderPath, String value, long rowNumber) {
        try {
            // make index file if it doesn't exist
            if (!FileUtil.exists(folderPath)) {
                FileUtil.createDirectoriesIfNotExist(folderPath);
            }

            // increment index count
            String path = folderPath + "/" + value + ".index";
            Long count = 0L;
            if (FileUtil.exists(path)) {
                count = getCountForIndexPath(path);
            }
            count++;
            writeCountForIndexPath(path, count);

            // write new row in index
            FileUtil.writeBytesAppend(
                folderPath + "/" + value + ".index",
                TypeToByteUtil.longToByteArray(rowNumber)
            );



        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing row index to: " + folderPath,
                e
            );
        }



    }



    /*
        Table meta data
     */
    public static Long popLong(String filePath, long headerSize, Random random) {
        if (FileUtil.fileSize(filePath) - headerSize >= 8) {
            try {
                return TypeToByteUtil.byteArrayToLong(
                    FileUtil.popRandomBytes(filePath, 8, random)
                );
            } catch (IOException e) {
                throw new DavaException(
                    BASE_IO_ERROR,
                    "Error getting empty row for insert from table meta file: " + filePath,
                    e
                );
            }
        }
        else {
             return null;
        }

    }

    public static void writeLong(String filePath, long longToWrite) {
        try {
            FileUtil.writeBytesAppend(
                filePath,
                TypeToByteUtil.longToByteArray(longToWrite)
            );

        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing long to: " + filePath,
                e
            );
        }
    }




}
