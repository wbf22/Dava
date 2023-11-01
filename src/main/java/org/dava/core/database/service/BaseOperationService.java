package org.dava.core.database.service;


import org.dava.common.ArrayUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.*;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.dava.common.Checks.safeCast;
import static org.dava.core.database.objects.exception.ExceptionType.*;

/**
 * Class for actually modifying database files.
 */
public class BaseOperationService {

    public static int NUMERIC_PARTITION_SIZE = 1000000;




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

    public static void performOperation(IORunnable operation, String rollbackData, String rollbackPath) throws IOException {

        // write the rollback information (so it can be rolled back if this or later calls fail)
        FileUtil.writeBytesAppend(rollbackPath, rollbackData.getBytes());

        // run the write operation
        operation.run();
    }


    /*
        Indices
     */
    public static void addToIndex(String folderPath, Object value, List<IndexWritePackage> indexWritePackages, boolean isUnique) {
        try {

            // make index file if it doesn't exist
            // TODO make a little cache here to avoid this expensive operation
            if (!FileUtil.exists(folderPath)) {
                FileUtil.createDirectoriesIfNotExist(folderPath);
            }

            // increment index count and write new routes in index
            String path = folderPath + "/" + value + ".index";
            long count = 0L;
            if (FileUtil.exists(path)) {
                if (isUnique) {
                    throw new DavaException(UNIQUE_CONSTRAINT_VIOLATION, "Row already exists with unique value or key: " + value, null);
                }
                else {
                    count = getCountForIndexPath(path);
                }
            }

            List<WritePackage> writes = (List<WritePackage>) (List<?>) indexWritePackages;

            writes.add(
                0,
                new WritePackage(
                    0L,
                    TypeToByteUtil.longToByteArray(
                        count + indexWritePackages.size()
                    )
                )
            );


            // write indices to index (and also count)
            FileUtil.writeBytes(
                path,
                writes
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

        return getLinesUsingRoutes(partition, table, lines )
            .map(line -> new Row(line, table))
            .toList();
    }

    /**
     * Gets rows from the table from start to end row.
     *
     * @return a list of rows of size 0 to (endRow - startRow)
     */
    public static List<Row> getRowsFromTable(Database database, String tableName, String columnName, String value, long startRow, long endRow) {
        Table<?> table = database.getTableByName(tableName);

        Column<?> column = table.getColumn(columnName);
        if (column.isIndexed()) {
            return table.getPartitions().parallelStream()
                .flatMap(partition -> {
                    String path = Index.buildIndexPath(database.getRootDirectory(), tableName, partition, column, table.getColumnLeaves().get(partition + columnName), value);

                    List<IndexRoute> lines = getRoutes(
                        path,
                        partition,
                        8 + startRow * 10,
                        (int) (endRow * 10)
                    );

                    return getLinesUsingRoutes(partition, table, lines)
                        .map(line -> new Row(line, table));
                })
                .toList();
        }
        else {
            return table.getPartitions().parallelStream()
                .flatMap(partition ->
                    getAllLinesWithoutRoutes(
                        table,
                        partition,
                        row -> row.getValue(columnName).toString().equals(value),
                        startRow,
                        endRow
                    )
                )
                .toList();
        }



    }

    private static Stream<Row> getAllLinesWithoutRoutes(Table<?> table, String partition, Predicate<Row> filter, long startRow, Long endRow) {
        try {
            Integer end = (endRow == null)? Integer.MAX_VALUE : Math.toIntExact(endRow);
            byte[] bytes = FileUtil.readBytes(
                table.getTablePath(partition),
                0,
                null
            );

            if (bytes == null)
                throw new IOException("Eof reached early");

            List<String> tableRows = Arrays.stream(new String(
                bytes,
                StandardCharsets.UTF_8
            ).split("\n"))
            .toList();

            int index = 1; // +1 for header line of table
            List<Row> rows = new ArrayList<>();
            while ( rows.size() < end && index < tableRows.size() ) {
                Row row = new Row(tableRows.get(index), table);
                if (filter.test(row))
                    rows.add(row);
                index++;
            }

            return (rows.size() < end)? rows.stream() : rows.subList((int) startRow, end).stream();

        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error reading all lines from table: " + table.getTablePath(partition),
                e
            );
        }
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
            Date<?> date,
            boolean descending
    ) {
        int year = date.getYear();
        Table<?> table = database.getTableByName(tableName);
        Column<?> column = table.getColumn(columnName);

        Comparator<Row> comparator = Comparator.comparing(
            row -> safeCast(row.getValue(columnName), Date.class)
        );
        comparator = (descending)? comparator.reversed() : comparator;

        if (!column.isIndexed()) {
            return table.getPartitions().parallelStream()
                .flatMap(partition ->
                    getAllLinesWithoutRoutes(
                        table,
                        partition,
                        (Row row) -> compareDatesRowYearDateYear.test(
                            safeCast(row.getValue(columnName), Date.class),
                            date
                        ),
                        0,
                        null
                    )
                )
                .sorted(comparator)
                .toList();
        }

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
            .sorted(comparator)
            .toList();
    }

    // TODO consider making a separate service for dates, or maybe just chopping up these methods

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
        List<Integer> yearDatesFolders = getYearDateFolders(database, tableName, columnName, date, compareYearsFolderYearDateYear, descending, table);


        List<Row> rows = new ArrayList<>();
        Comparator<Row> comparator = Comparator.comparing(
                row -> safeCast(row.getValue(columnName), Date.class)
        );
        comparator = (descending)? comparator.reversed() : comparator;

        long count = 0L;
        int size = (int) (endRow - startRow);
        for (Integer year : yearDatesFolders) {
            // get date files for year
            List<LocalDate> indicesForLocalDates = table.getPartitions().parallelStream()
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
                    count += table.getPartitions().parallelStream()
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

    public static List<Integer> getYearDateFolders(Database database, String tableName, String columnName, Date<?> date, BiPredicate<Integer, Integer> compareYearsFolderYearDateYear, boolean descending, Table<?> table) {
        return table.getPartitions().parallelStream()
            .flatMap(partition -> {
                String columnPath = Index.buildColumnPath(database.getRootDirectory(), tableName, partition, columnName);
                return Arrays.stream(FileUtil.listFiles(columnPath))
                    .map(yearFolder -> Integer.parseInt(yearFolder.getName().split("\\.")[0]))
                    .filter(fYear -> compareYearsFolderYearDateYear.test(fYear, date.getYear()))
                    .sorted(
                        descending ? Comparator.reverseOrder() : Comparator.naturalOrder()
                    );
            })
            .sorted(
                descending ? Comparator.reverseOrder() : Comparator.naturalOrder()
            )
            .toList();
    }

    /**
     * Gets IndexRoutes from a file from startByte to numBytes or the end of the file (whichever comes first).
     * If allLines is true then startByte and numBytes are ignored
     */
    public static List<IndexRoute> getRoutes(String filePath, String partition, Long startByte, Integer numBytes) {
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

    public static Stream<String> getLinesUsingRoutes(String partition, Table<?> table, List<IndexRoute> rows) {

        try {
            return FileUtil.readBytes(
                table.getTablePath(partition),
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

    public static long getCountForIndexPath(String path) {
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

    public static void incrementNumericCountFile(String folderPath) {
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

    public static boolean repartitionIfNeeded(String folderPath, Map<String, Long> numericCounts) {
        try {
            String countFile = folderPath + "/c.count";

            long count;
            if (numericCounts.containsKey(countFile)) {
                count = numericCounts.get(countFile);
            }
            else {
                count = getNumericCount(countFile);
                numericCounts.put(countFile, count);
            }

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
                return true;
            }
        } catch (IOException e) {
            throw new DavaException(INDEX_CREATION_ERROR, "Repartition of numerical index failed: " + folderPath, e);
        }
        return false;
    }

    public static long getNumericCount(String countFile) throws IOException {
        byte[] countBytes = FileUtil.readBytes(countFile, 0, 8);
        return (countBytes == null)? 0L :
            TypeToByteUtil.byteArrayToLong(
                countBytes
            );
    }





    /*
        Table meta data
     */

    public static void popRoutes(String emptiesFile, List<Empty> emptiesPackages) throws IOException {

        if (emptiesPackages.isEmpty())
            return;

        List<Long> startBytes = emptiesPackages.stream()
            .map(Empty::getStartByte)
            .toList();

        FileUtil.popBytes(emptiesFile, 10, startBytes);
    }

    /**
     * get <= emptiesToGet routes and packages
     *
     * doesn't delete any routes, just retrieves
     */
    public static EmptiesPackage getEmpties(Integer emptiesToGet, String emptiesFile, Random random) throws IOException {
        // TODO don't check for empties every time to avoid this costly file access
        long fileSize = FileUtil.fileSize(emptiesFile);
        if ( fileSize - 10 <= 8) {
            return null;
        }

        Set<Long> startBytes = new HashSet<>();

        emptiesToGet = ( emptiesToGet == null || emptiesToGet > (fileSize -8) / 10 )? (int) (fileSize - 8) / 10 : emptiesToGet;

        IntStream.range(0, emptiesToGet)
            .forEach( i ->
                startBytes.add(
                    ( random.nextLong( 0, (fileSize -8) / 10 ) ) * 10 + 8
                )
            );

        List<Long> numBytes = IntStream.range(0, startBytes.size())
            .mapToObj( i -> 10L)
            .toList();

        List<Long> listStartBytes = new ArrayList<>(startBytes);

        EmptiesPackage emptiesPackages = new EmptiesPackage();

        // TODO assuming here that are startbytes are valid and will be returned.
        List<Object> bytesArrays = FileUtil.readBytes(emptiesFile, startBytes.stream().toList(), numBytes)
            .stream()
            .toList();

        IntStream.range(0, bytesArrays.size())
            .forEach( i -> {
                Object bytes = bytesArrays.get(i);
                if (bytes != null) {
                    byte[] casted = (byte[]) bytes;
                    IndexRoute route = new IndexRoute(
                        null,
                        TypeToByteUtil.byteArrayToLong(
                            ArrayUtil.subRange(casted, 0, 6)
                        ),
                        (int) TypeToByteUtil.byteArrayToLong(
                            ArrayUtil.subRange(casted, 6, 10)
                        )
                    );
                    emptiesPackages.addEmpty(
                        new Empty(
                            route,
                            listStartBytes.get(i)
                        )
                    );
                }
            });

        return emptiesPackages;
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
