package org.dava.core.database.service;


import org.dava.common.ArrayUtil;
import org.dava.common.TypeUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.dava.common.Checks.safeCast;
import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;
import static org.dava.core.database.objects.exception.ExceptionType.INDEX_CREATION_ERROR;

/**
 * Class for actually modifying database files.
 */
public class BaseOperationService {

    private static int NUMERIC_PARTITION_SIZE = 1000000;



    public static boolean insert(Row row, Database database, Table<?> table, boolean index) {
        // TODO collect data to perform rollback if necessary

        // serialize row
        String partition = table.getRandomPartition();
        String rowString = Row.serialize(table, row.getColumnsToValues()) + "\n";
        byte[] bytes = rowString.getBytes(StandardCharsets.UTF_8);

        // get random empty row or use last row in the table
        IndexRoute route = table.getEmptyRow(partition);
        if (route == null || bytes.length > route.getLengthInTable()) {
            Long tableSize = FileUtil.fileSize(table.getTablePath(partition) + ".csv");
            route = new IndexRoute(
                partition,
                tableSize,
                bytes.length
            );
        }

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

        // update table row count in empties
        Long newSize = table.getSize(route.getPartition()) + 1;
        table.setSize(route.getPartition(), newSize);

        if (index) {
            // add to indices
            for (Map.Entry<String, Object> columnValue : row.getColumnsToValues().entrySet()) {
                String folderPath = Index.buildIndexRootPath(
                    database.getRootDirectory(),
                    table.getTableName(),
                    route.getPartition(),
                    table.getColumn(columnValue.getKey()),
                    columnValue.getValue()
                );

                Column<?> column = table.getColumn(columnValue.getKey());
                Object value = columnValue.getValue();
                if (Date.isDateSupportedDateType( column.getType() )) {
                    value = Date.of(
                        value.toString(),
                        table.getColumn(columnValue.getKey()).getType()
                    ).getDateWithoutTime().toString();
                    addToIndex(folderPath, value, route);
                }
                else if ( TypeUtil.isNumericClass(column.getType()) ) {
                    if (!FileUtil.exists(folderPath + "/" + value + ".index")) {
                        updateNumericCountFile(folderPath);
                    }
                    addToIndex(folderPath, value, route);
                    repartitionIfNeeded(folderPath);
                }
                else {
                    addToIndex(folderPath, value, route);
                }
            }
        }


        return true;
    }

    public static boolean update(Row row, IndexRoute primaryKeyRoute, Table<?> table) {
        // delete row and indices.

        // insert row again

        return true;
    }

    public static boolean delete(Row row, IndexRoute primaryKeyRoute, Table<?> table) {
        // delete row and indices.

        // insert row again

        return true;
    }



    /*
        Indices
     */

    public static void addToIndex(String folderPath, Object value, IndexRoute route) {
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
                route.getRouteAsBytes()
            );

        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing row index to: " + folderPath,
                e
            );
        }
    }

    public static List<Row> getAllRowsFromIndex(String indexPath, String partition, Database database, String tableName) {
        Table<?> table = database.getTableByName(tableName);

        List<IndexRoute> lines = getRoutes(
                indexPath,
                partition,
                8L,
                null
        );

        return getLinesFromPartition( partition, table, lines )
            .map(line -> new Row(line, table))
            .toList();
    }

    /**
     * Gets rows from the table from start to end row.
     *
     * @return a list of rows of size 0 to (endRow - startRow)
     */
    public static List<Row> getRowsFromIndex(Database database, String tableName, String columnName, String value, long startRow, long endRow) {
        Table<?> table = database.getTableByName(tableName);

        int partitionQuerySize = (int) (1.1 * (endRow - startRow)/table.getPartitions().size());
        int adjustedSize = (partitionQuerySize == 0)? 1 : partitionQuerySize;

        return table.getPartitions().parallelStream()
            .flatMap(partition -> {
                String path = Index.buildIndexPath(database.getRootDirectory(), tableName, partition, table.getColumn(columnName), value);

                List<IndexRoute> lines = getRoutes(
                    path,
                    partition,
                    8 + startRow * 10,
                    adjustedSize * 10
                );

                return getLinesFromPartition(partition, table, lines)
                    .map(line -> new Row(line, table));
            })
            .toList();
    }

    /**
     * Get's rows by date from both partitions
     */
    public static List<Row> getAllComparingDate(
            Database database,
            String tableName,
            String columnName,
            BiPredicate<Integer, Integer> compareYearsFolderYearDateYear,
            BiPredicate<Date<?>, Date<?>> compareDatesRowYearDateYear,
            Date<?> date
    ) {
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
                    if (compareYearsFolderYearDateYear.test(folderYear, year)) {
                        rows.addAll(
                            Arrays.stream(FileUtil.listFiles(
                                Index.buildIndexYearFolderForDate(database.getRootDirectory(), tableName, partition, columnName, Integer.toString(folderYear))
                            ))
                            .flatMap( indexFile -> getAllRowsFromIndex( indexFile.getPath(), partition, database, tableName ).stream() )
                            .toList()
                        );
                    }
                    else if (folderYear == year) {
                        rows.addAll(
                            Arrays.stream(FileUtil.listFiles(
                                Index.buildIndexYearFolderForDate(database.getRootDirectory(), tableName, partition, columnName, Integer.toString(folderYear))
                            ))
                            .flatMap( localDateIndexFile -> {
                                Date<?> indexLocateDate = Date.of(localDateIndexFile.getName().split("\\.")[0], column.getType());

                                if ( compareDatesRowYearDateYear.test(indexLocateDate, date) ) {
                                    return getAllRowsFromIndex( localDateIndexFile.getPath(), partition, database, tableName ).stream();
                                }
                                else if (indexLocateDate.equals(date.getDateWithoutTime())) {
                                    return getAllRowsFromIndex( localDateIndexFile.getPath(), partition, database, tableName ).stream()
                                            .filter( row -> {
                                                Date<?> rowDate = safeCast(row.getValue(columnName), Date.class);
                                                return compareDatesRowYearDateYear.test(rowDate, date);
                                            });
                                }
                                return new ArrayList<Row>().stream();
                            } )
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
    public static List<Row> getRowsComparingDate(
            Database database,
            String tableName,
            String columnName,
            Date<?> date,
            BiPredicate<Integer, Integer> compareYearsFolderYearDateYear,
            BiPredicate<Date<?>, Date<?>> compareDatesRowYearDateYear,
            long startRow,
            long endRow,
            boolean descending
    ) {

        Table<?> table = database.getTableByName(tableName);

        // all year folders within partitions, filtered by after date, sorted descending or ascending
        List<Integer> yearDatesFolders = table.getPartitions().stream()
            .flatMap(partition -> {
                String columnPath = Index.buildColumnPath(database.getRootDirectory(), tableName, partition, columnName);
                return Arrays.stream(FileUtil.listFiles(columnPath))
                    .map(yearFolder -> Integer.parseInt(yearFolder.getName().split("\\.")[0]))
                    .filter(fYear -> compareYearsFolderYearDateYear.test(fYear, date.getYear()))
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
        for (Integer year : yearDatesFolders) {
            // get date files for year
            List<LocalDate> indicesForLocalDates = table.getPartitions().stream()
                    .flatMap(partition -> {
                        String yearPath = Index.buildIndexYearFolderForDate(database.getRootDirectory(), tableName, partition, columnName, year.toString());
                        return Arrays.stream(FileUtil.listFiles(yearPath))
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
                                Date<?> indexLocateDate = Date.of(indexLocalDate.toString(), LocalDate.class);

                                if ( compareDatesRowYearDateYear.test(indexLocateDate, date) ) {
                                    String path = Index.buildIndexPathForDate(database.getRootDirectory(), tableName, partition, columnName, indexLocalDate);
                                    return getCountForIndexPath(path);
                                }
                                else if (indexLocalDate.equals(date.getDateWithoutTime())) {
                                    String path = Index.buildIndexPathForDate(database.getRootDirectory(), tableName, partition, columnName, indexLocalDate);
                                    return (long) getAllRowsFromIndex(path, partition, database, tableName).stream()
                                            .filter(row -> {
                                                Date<?> rowDate = safeCast(row.getValue(columnName), Date.class);
                                                return compareDatesRowYearDateYear.test(rowDate, date);
                                            })
                                            .toList()
                                            .size();
                                }
                                return 0L;
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

                    if (indexLocalDate.getYear() == year) {
                        returned = returned.stream()
                                .filter(row -> {
                                    Date<?> rowDate = safeCast(row.getValue(columnName), Date.class);
                                    return compareDatesRowYearDateYear.test(rowDate, date);
                                })
                                .toList();
                    }

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
     * Gets IndexRoutes from a file from startByte to numBytes or the end of the file (whichever comes first).
     * If allLines is true then startByte and numBytes are ignored
     */
    private static List<IndexRoute> getRoutes(String filePath, String partition, Long startByte, Integer numBytes) {
        try {

            byte[] bytes;
            long fileSize = FileUtil.fileSize(filePath);
            numBytes = Math.toIntExact(
                (numBytes == null || fileSize < numBytes + startByte) ? fileSize - startByte : numBytes
            );
            bytes = (startByte > fileSize)? null : FileUtil.readBytes(filePath, startByte, numBytes);
            if (bytes == null)
                return new ArrayList<>();

            return IndexRoute.parseBytes(
                bytes,
                partition
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error reading file for path: " + filePath,
                e
            );
        }
    }

    public static Stream<String> getLinesFromPartition(String partition, Table<?> table, List<IndexRoute> rows) {

        try {
            return FileUtil.readBytes(
                table.getTablePath(partition) + ".csv",
                rows.stream()
                    .map(IndexRoute::getOffsetInTable)
                    .toList(),
                rows.stream()
                    .map(IndexRoute::getLengthInTable)
                    .map(i -> (long) i)
                    .toList()
            )
            .stream()
            .map(bytes -> new String((byte[]) bytes, StandardCharsets.UTF_8) );
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

    private static void updateNumericCountFile(String folderPath) {
        String countFile = folderPath + "/c.count";
        try {
            FileUtil.createDirectoriesIfNotExist(folderPath);

            if (!FileUtil.exists(countFile)) {
                FileUtil.createFile(countFile, TypeToByteUtil.longToByteArray(0L) );
            }

            long count = getNumericCount(countFile);
            count++;
            FileUtil.writeBytes(countFile, 0, TypeToByteUtil.longToByteArray(count) );

        } catch (IOException e) {
            throw new DavaException(INDEX_CREATION_ERROR, "Error updating count for numeric index partition: " + countFile, e);
        }

    }

    private static void repartitionIfNeeded(String folderPath) {
        try {
            String countFile = folderPath + "/c.count";

            long count = getNumericCount(countFile);

            if (count > NUMERIC_PARTITION_SIZE) {
                File[] files = FileUtil.listFiles(folderPath);

                int step = files.length / 5;
                List<BigDecimal> samples = IntStream.range(0, 5)
                    .mapToObj(i ->
                          (files[i * step].getName().contains(".index")) ? new BigDecimal(files[i * step].getName().split("\\.")[0]) : BigDecimal.ZERO
                    )
                    .sorted(BigDecimal::compareTo)
                    .toList();
                BigDecimal median = samples.get(2);

                FileUtil.createDirectoriesIfNotExist(folderPath + "/-" + median);
                FileUtil.createDirectoriesIfNotExist(folderPath + "/+" + median);

                List<File> lessThan = new ArrayList<>();
                List<File> greaterThanOrEqual = new ArrayList<>();
                Arrays.stream(files).forEach( file -> {
                    String fileName = file.getName();
                    if (fileName.contains(".index")) {
                        if ( new BigDecimal(fileName.split("\\.")[0]).compareTo(median) < 0 ) {
                            lessThan.add(file);
                        }
                        else {
                            greaterThanOrEqual.add(file);
                        }
                    }
                });

                FileUtil.moveFilesToDirectory(lessThan, folderPath + "/-" + median);
                FileUtil.moveFilesToDirectory(greaterThanOrEqual, folderPath + "/+" + median);

                FileUtil.createFile(folderPath + "/-" + median + "/c.count", TypeToByteUtil.longToByteArray(lessThan.size()) );
                FileUtil.createFile(folderPath + "/+" + median + "/c.count", TypeToByteUtil.longToByteArray(greaterThanOrEqual.size()) );

                FileUtil.deleteFile(countFile);

            }
        } catch (IOException e) {
            throw new DavaException(INDEX_CREATION_ERROR, "Repartition of numerical index failed: " + folderPath, e);
        }
    }


    private static long getNumericCount(String countFile) throws IOException {
        byte[] countBytes = FileUtil.readBytes(countFile, 0, 8);
        return (countBytes == null)? 0L :
            TypeToByteUtil.byteArrayToLong(
                countBytes
            );
    }


    /*
        Table meta data
     */
    public static IndexRoute popEmpty(String filePath, long headerSize, Random random) {
        if (FileUtil.fileSize(filePath) - headerSize >= 8) {
            try {
                byte[] bytes = FileUtil.popRandomBytes(filePath, 8,10, random);
                return new IndexRoute(
                    null,
                    TypeToByteUtil.byteArrayToLong(
                        ArrayUtil.subRange(bytes, 0, 6)
                    ),
                    (int) TypeToByteUtil.byteArrayToLong(
                        ArrayUtil.subRange(bytes, 6, 10)
                    )
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
