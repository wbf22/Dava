package org.dava.core.database.service;


import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

/**
 * Class for actually modifying database files.
 */
public class BaseOperationService {




    public <T> boolean insert(Row row, Table<?> table, boolean index) {
        // TODO check constraints before this method

        // add to table
        String serialized = row.serialize(table);
        table.

        // add to index csv

        // increment index count

        return true;
    }

    public <T> boolean update(Row row, IndexEntry primaryKeyRoute, Table<T> table) {

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
    public static Index getIndex(Database database, String tableName, String partition, String columnName, String value) {
        String indexRootPath = Index.buildIndexRootPath(database.getRootDirectory(), tableName, partition, columnName, value);
        File indexRootFile = new File(
            indexRootPath
        );

        String headerPath = null;
        File[] files = indexRootFile.listFiles();
        if (files != null) {
            File headerFile = Arrays.stream(files).sequential()
                .filter(file -> file.getName().contains(".header"))
                .findAny()
                .orElse(null);
            headerPath = (headerFile != null)? indexRootPath + "/" + headerFile.getName() : null;
        }

        if (headerPath != null) {
            return Index.parseFromHeader(headerPath);
        }

        throw new DavaException(
            BASE_IO_ERROR,
            "Missing header file in index directory or error in read: " + indexRootPath,
            null
        );
    }

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

                return Arrays.stream(lines).sequential()
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
                Index index = BaseOperationService.getIndex(database, tableName, partition, columnName, value);

                long[] lines = getLongs(
                        path,
                        false,
                        startRow * index.getRowLengths(),
                        (int) ((endRow - startRow) * index.getRowLengths())
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

        IndexEntry entry = IndexEntry.of(partition, indexRow, table);
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
        File file = new File(path);

        File[] dirs = file.listFiles();
        if (dirs == null)
            return null;
        
        return Arrays.stream(dirs)
            .map(File::getName)
            .filter(name -> name.contains(".header"))
            .findAny()
            .map(name ->
                Long.parseLong(name.split(".header")[0])
            )
            .orElse(0L);
    }


    /*
        Table meta data
     */
    public static Long popLong(String filePath, long headerSize) {
        if (FileUtil.fileSize(filePath) - headerSize >= 8) {
            try {
                return TypeToByteUtil.byteArrayToLong(
                    FileUtil.popLastBytes(filePath, 8)
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






}
