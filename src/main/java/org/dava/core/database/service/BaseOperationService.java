package org.dava.core.database.service;


import org.dava.common.ArrayUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

import static org.dava.common.Checks.safeCast;
import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

/**
 * Class for actually modifying database files.
 */
public class BaseOperationService {




    public static boolean insert(Row row, Database database, Table<?> table, boolean index) {
        // TODO collect data to perform rollback if necessary

        // serialize row
        String partition = table.getRandomPartition();
        String rowString = Row.serialize(table, row.getColumnsToValues()) + "\n";
        byte[] bytes = rowString.getBytes(StandardCharsets.UTF_8);

        // get random empty row or use last row in the table
        Long destinationRow = table.getEmptyRow(partition);
        destinationRow = (destinationRow == null || bytes.length > table.getRowLength(destinationRow, partition))?
            table.getSize(partition) : destinationRow;

        IndexRoute route = IndexRoute.of(partition, destinationRow, table);
        if (bytes.length < route.getLengthInTable()) {
            rowString = rowString.substring(0, rowString.length()-1) + " ".repeat(route.getLengthInTable() - bytes.length) + "\n";
        }
        bytes = rowString.getBytes(StandardCharsets.UTF_8);

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
            for (Map.Entry<String, Object> columnValue : row.getColumnsToValues().entrySet()) {
                String folderPath = Index.buildIndexRootPath(
                    database.getRootDirectory(),
                    table.getTableName(),
                    route.getPartition(),
                    table.getColumn(columnValue.getKey()),
                    columnValue.getValue()
                );

                Object value = columnValue.getValue();
                if (Date.isDateSupportedDateType( table.getColumn(columnValue.getKey()).getType() )) {
                    value = Date.of(
                        value.toString(),
                        table.getColumn(columnValue.getKey()).getType()
                    ).getDateWithoutTime().toString();
                }
                addToIndex(folderPath, value, destinationRow);
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
    public static List<Row> getAllRowsFromIndex(String indexPath, String partition, Database database, String tableName) {
        Table<?> table = database.getTableByName(tableName);

        long[] lines = getLongs(
                indexPath,
                true,
                null,
                null
        );
        long[] countRemoved = Arrays.copyOfRange(lines, 1, lines.length);

        return Arrays.stream(countRemoved).sequential()
                .mapToObj(line -> getLineFromPartition(partition, table, line))
                .map(line -> new Row(line, table))
                .toList();
    }

    /**
     * Gets rows from the table from start to end row. If the table has multiple partitions
     * the rows be stored returned starting from the first partition to the last.
     *
     * @return a list of rows of size 0 - (endRow - startRow)
     */
    public static List<Row> getRowsFromIndex(Database database, String tableName, String columnName, String value, long startRow, long endRow) {
        Table<?> table = database.getTableByName(tableName);

        List<Row> rows = new ArrayList<>();
        for(int i = 0; i < table.getPartitions().size(); i++) {
            String partition = table.getPartitions().get(i);
            String path = Index.buildIndexPath(database.getRootDirectory(), tableName, partition, table.getColumn(columnName), value);

            long[] lines = getLongs(
                    path,
                    false,
                    8 + startRow * 8,
                    (int) ((endRow - startRow) * 8)
            );

            rows.addAll(
                Arrays.stream(lines).sequential()
                    .mapToObj(line -> getLineFromPartition(partition, table, line))
                    .map(line -> new Row(line, table))
                    .toList()
            );

            if (rows.size() >= endRow - startRow)
                return rows;

        }
        return rows;
    }

    public static List<Row> getAllAfterDate(Database database, String tableName, String columnName, Date<?> date) {
        int year = date.getYear();
        Table<?> table = database.getTableByName(tableName);
        Column<?> column = table.getColumn(columnName);

        return table.getPartitions().parallelStream()
            .flatMap(partition -> {
                String path = Index.buildColumnPath(database.getRootDirectory(), tableName, partition, columnName);
                File[] yearFolders = FileUtil.listFiles(path);

                List<Row> rows = new ArrayList<>();
                for (File yearFolder : yearFolders) {
                    int folderYear = Integer.parseInt(yearFolder.getName());
                    if (folderYear > year) {
                        rows.addAll(
                            Arrays.stream(FileUtil.listFiles(
                                Index.buildIndexRootPathForDate(database.getRootDirectory(), tableName, partition, columnName, Integer.toString(folderYear))
                            )).sequential()
                            .flatMap( indexFile -> getAllRowsFromIndex( indexFile.getPath(), partition, database, tableName ).stream() )
                            .toList()
                        );
                    }
                    else if (folderYear == year) {
                        rows.addAll(
                            Arrays.stream(FileUtil.listFiles(
                                Index.buildIndexRootPathForDate(database.getRootDirectory(), tableName, partition, columnName, Integer.toString(folderYear))
                            )).sequential()
                            .filter( indexFile -> date.isAfter( Date.of(indexFile.getName(), column.getType()) ) )
                            .flatMap( indexFile -> getAllRowsFromIndex( indexFile.getPath(), partition, database, tableName ).stream() )
                            .toList()
                        );
                    }
                }

                return rows.stream();
            })
            .toList();
    }

    /**
     *   |     |--OrderDate
     *   |     |    |--2019
     *   |     |    |    |--1/22/2019.index
     *   |     |    |    |--4/12/2019.index
     *   |     |    |    '--5/3/2019.index
     *   |     |    |--2020
     *   |     |    |    |--1/1/2020.index
     *   |     |    |    |--1/3/2020.index
     *   |     |    |    '--1/4/2020.index
     *
     *
     *   |     |--OrderDate
     *   |     |    |--2020
     *   |     |    |    |--1/2/2020.index
     *   |     |    |    |--1/5/2020.index
     *   |     |    |    '--1/6/2020.index
     *
     *   02/12/2020.index values:
     *     2020-2-12T07:5:57.703778
     *     2020-2-12T07:16:57.703778
     *     2020-2-12T07:21:57.703778
     *
     *
     *   sql after calls return unsorted unless called with order by
     */
    public static List<Row> getRowsAfterDate(Database database, String tableName, String columnName, Date<?> date, long startRow, long endRow, boolean descending) {

        Table<?> table = database.getTableByName(tableName);

        // all year folders within partitions, filtered by after date, sorted descending or ascending
        List<Integer> yearDates = table.getPartitions().stream()
            .flatMap(partition -> {
                String columnPath = Index.buildColumnPath(database.getRootDirectory(), tableName, partition, columnName);
                return Arrays.stream(FileUtil.listFiles(columnPath)).sequential()
                    .map(yearFolder -> Integer.parseInt(yearFolder.getName().split("\\.")[0]))
                    .filter(fYear -> fYear >= date.getYear())
                    .sorted(
                        (descending)? Comparator.reverseOrder() : Integer::compareTo
                    );
            })
            .sorted(
                (descending)? Comparator.reverseOrder() : Comparator.naturalOrder()
            )
            .toList();



        List<Row> rows = new ArrayList<>();
        Comparator<Row> comparator = Comparator.comparing(
                row -> safeCast(row.getValue(columnName), Date.class)
        );
        comparator = (descending)? comparator.reversed() : comparator;

        long count = 0L;
        int size = (int) (endRow - startRow);
        for (Integer year : yearDates) {
            // get date files for year
            List<LocalDate> indicesForLocalDates = table.getPartitions().stream()
                    .flatMap(partition -> {
                        String yearPath = Index.buildIndexRootPathForDate(database.getRootDirectory(), tableName, partition, columnName, year.toString());
                        return Arrays.stream(FileUtil.listFiles(yearPath)).sequential()
                                .map( indexFile -> LocalDate.parse(indexFile.getName().split("\\.")[0]) );
                    })
                    .sorted(
                            (descending)? Comparator.reverseOrder() : Comparator.naturalOrder()
                    )
                    .toList();

            // for each of the indexLocalDate in indicesForLocalDates, count up until you get to start row
            for (LocalDate indexLocalDate : indicesForLocalDates) {
                if (count <= startRow) {
                    count += table.getPartitions().stream()
                            .map(partition -> {
                                String path = Index.buildIndexPathForDate(database.getRootDirectory(), tableName, partition, columnName, indexLocalDate);
                                return getCountForIndexPath(path);
                            })
                            .reduce(Long::sum)
                            .orElse(0L);
                }


                if (count > startRow) {
                    List<Row> returned = table.getPartitions().parallelStream()
                            .flatMap(partition -> {
                                String path = Index.buildIndexPathForDate(database.getRootDirectory(), tableName, partition, columnName, indexLocalDate);
                                return getAllRowsFromIndex(path, partition, database, tableName).stream();
                            })
                            .toList();

                    rows.addAll(returned);

                    // once you have enough rows, sort the rows descending or ascending and return the sublist of the requested size
                    if (rows.size() > size) {
                        return rows.stream()
                                .sorted( comparator )
                                .toList()
                                .subList(0, size);
                    }
                }
            }
        }

        return rows.stream()
                .sorted( comparator )
                .toList();
    }


    /**
     * Gets longs from a file from startByte to numBytes or the end of the file (whichever comes first).
     * If allLines is true then startByte and numBytes are ignored and
     */
    private static long[] getLongs(String filePath, boolean allLines, Long startByte, Integer numBytes) {
        try {

            byte[] bytes;
            if (allLines) {
                bytes = FileUtil.readBytes(filePath);
            }
            else {
                long fileSize = FileUtil.fileSize(filePath);
                numBytes = Math.toIntExact(
                    (fileSize < numBytes + startByte) ? fileSize - startByte : numBytes
                );
                bytes = FileUtil.readBytes(filePath, startByte, numBytes);
            }
            if (bytes == null)
                return new long[0];

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
                table.getTablePath(entry.getPartition()) + ".csv",
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


    public static void addToIndex(String folderPath, Object value, long rowNumber) {
        try {
            // make index file if it doesn't exist
            if (!FileUtil.exists(folderPath)) {
                FileUtil.createDirectoriesIfNotExist(folderPath);
            }

            // increment index count
            String path = folderPath + "/" + value + ".index";
            long count = 0L;
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
