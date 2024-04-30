package org.dava.core.database.service;


import org.dava.core.common.ArrayUtil;
import org.dava.core.common.Bundle;
import org.dava.core.common.TypeUtil;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.operations.common.EmptiesPackage;
import org.dava.core.database.service.operations.common.WritePackage;
import org.dava.core.database.service.operations.insert.IndexWritePackage;
import org.dava.core.database.service.structure.*;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.dava.core.common.Checks.safeCast;
import static org.dava.core.database.objects.exception.ExceptionType.*;

/**
 * Class for actually modifying database files.
 */
public class BaseOperationService {

    public static int NUMERIC_PARTITION_SIZE = 10;//1000000




    /**
     * Adds values to an index. Also handles numeric count file updates.
     * 
     * <p> Does not handle numeric repartitions. That's done in the Insert class as of 4/16/2024
     */
    public static void addToIndex(String folderPath, Object value, List<IndexWritePackage> indexWritePackages, boolean isUnique) {
        try {

            // make index file if it doesn't exist
            if (!FileUtil.exists(folderPath)) {
                FileUtil.createDirectoriesIfNotExist(folderPath);

                // update numeric or date index counts
                if (TypeUtil.isNumericClass(indexWritePackages.get(0).getColumnType())) {
                    updateNumericCountFile(folderPath, 1);
                }
                if (TypeUtil.isDate(indexWritePackages.get(0).getColumnType())) {
                    updateNumericCountFile(folderPath, 1);
                }


            }

            String path = folderPath + "/" + value + ".index";
            if (FileUtil.exists(path) && isUnique) {
                    throw new DavaException(UNIQUE_CONSTRAINT_VIOLATION, "Row already exists with unique value or key: " + value, null);
            }

            // write indices to index
            FileUtil.writeBytes(
                path,
                (List<WritePackage>) (List<?>) indexWritePackages
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing row index to: " + folderPath,
                e
            );
        }
    }


    /**
     * Gets rows from the table from start to end row.
     *
     * @return a list of rows of size 0 to (endRow - startRow)
     */
    public static List<Row> getRowsFromTable(Table<?> table, String columnName, String value, long startRow, Long endRow) {

        Column<?> column = table.getColumn(columnName);
        if (column.isIndexed()) {
            return table.getPartitions().parallelStream()
                .flatMap(partition -> {
                    String indexPath = Index.buildIndexPath(
                        table,
                        partition,
                        columnName,
                        value
                    );

                    return getRowsFromIndex(indexPath, table, partition, startRow, endRow);
                })
                .toList();
        }
        else {
            return table.getPartitions().parallelStream()
                .flatMap(partition ->
                    getRowsFromTablePartitionWithoutIndicies(
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

    /**
     * Get's rows in an index limited
     */
    private static Stream<Row> getRowsFromIndex(String indexPath, Table<?> table, String partition, long startRow, Long endRow) {
        List<Route> routes = getFileSizeAndRoutes(
            indexPath,
            partition,
            startRow * 10,
            (endRow == null)? null : (int) (endRow - startRow) * 10
        ).getSecond();

        List<String> lines = getLinesUsingRoutes(partition, table, routes);

        return IntStream.range(0, lines.size())
            .mapToObj(i -> new Row(lines.get(i), table, routes.get(i)));
    }


    /**
     * Get's rows that match the values in 'values'. Also is limited. Uses indices if possible
     */
    public static List<Row> getRowsWithValueInCollection(Table<?> table, String columnName, Set<String> values, Long startRow, Long endRow, boolean allRows) {
    

        Column<?> column = table.getColumn(columnName);
        if (column.isIndexed() && !allRows) {
            return table.getPartitions().parallelStream()
                .flatMap(partition -> {
                    List<String> indexPaths = values.stream()
                        .map(value ->
                            Index.buildIndexPath(
                                table,
                                partition,
                                columnName,
                                value
                            )
                        )
                        .toList();

                    return getRowsFromMultipleIndices(indexPaths, table, partition, startRow, endRow);
                })
                .toList();
        }
        else {
            return table.getPartitions().parallelStream()
                .flatMap(partition ->
                    getRowsFromTablePartitionWithoutIndicies(
                        table,
                        partition,
                        row -> values.contains(row.getValue(columnName).toString()),
                        startRow,
                        endRow
                    )
                )
                .toList();
        }

    }

    /**
     * Get's rows that match the values in 'values'. Uses indices if possible
     */
    private static Stream<Row> getRowsFromMultipleIndices(List<String> indexPaths, Table<?> table, String partition, long startRow, Long endRow) {

        List<Route> routes = indexPaths.parallelStream()
            .flatMap( indexPath -> {
                return getFileSizeAndRoutes(
                    indexPath,
                    partition,
                    startRow * 10,
                    (endRow == null)? null : (int) (endRow - startRow) * 10
                ).getSecond().stream();
            })
            .distinct() // no duplicates, see Route class for equals method
            .toList();

        List<String> lines = getLinesUsingRoutes(partition, table, routes);

        return IntStream.range(0, lines.size())
            .mapToObj(i -> new Row(lines.get(i), table, routes.get(i)));
    }

    /**
     * Get's rows in the table. limited. Doesn't use indices
     */
    public static List<Row> getRowsFromTableWithoutIndices(Table<?> table, long startRow, Long endRow) {

        return table.getPartitions().parallelStream()
            .flatMap(partition ->
                getRowsFromTablePartitionWithoutIndicies(
                    table,
                    partition,
                    row -> true,
                    startRow,
                    endRow
                )
            )
            .toList();
    }

    /**
     * Get's rows in the table by partition, using the start and end row values. Doesn't use indices
     */
    private static Stream<Row> getRowsFromTablePartitionWithoutIndicies(Table<?> table, String partition, Predicate<Row> filter, long startRow, Long endRow) {
        try {
            int end = (endRow == null)? Integer.MAX_VALUE : Math.toIntExact(endRow);
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
            long offset = tableRows.get(0).getBytes().length + 1;
            List<Row> rows = new ArrayList<>();
            while ( rows.size() < end && index < tableRows.size() ) {
                String rowString = tableRows.get(index);
                if (!rowString.equals( " ".repeat(rowString.length()) )) {
                    Row row = new Row(
                        rowString,
                        table,
                        new Route(
                            partition,
                            offset,
                            rowString.getBytes(StandardCharsets.UTF_8).length + 1
                        )
                    );

                    offset += rowString.getBytes(StandardCharsets.UTF_8).length + 1;

                    if (filter.test(row))
                        rows.add(row);
                }
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
     * Get's rows comparing with a numeric value. (Either greater than or less than). Uses indices if possible
     * 
     * Is also used to compare dates which are converted to Bigdecimal milliseconds since the epoch
     */
    public static List<Row> getRowsComparingNumeric(
        Table<?> table,
        String columnName,
        Comparator<BigDecimal> compareValues,
        Predicate<BigDecimal> filter,
        Function<String, BigDecimal> fileNameConverter,
        Long startRow,
        Long endRow,
        boolean descending,
        boolean getAllRows
    ) {

        Column<?> column = table.getColumn(columnName);


        Comparator<String> compareFileNames = (first, second) -> {
            return compareValues.compare(
                fileNameConverter.apply(first), 
                fileNameConverter.apply(second)
            );
        };
        compareFileNames = (descending)? compareFileNames.reversed() : compareFileNames;


        Comparator<Row> comparatorRows = Comparator.comparing(
            row -> {
                Object rowValue = row.getValue(columnName);
                if (rowValue instanceof BigDecimal bd) 
                    return bd;
                else {
                    Date<?> date = safeCast(rowValue, Date.class);
                    return date.getMillisecondsSinceTheEpoch();
                }
            }
        );
        comparatorRows = (descending)? comparatorRows.reversed() : comparatorRows;

        Predicate<Row> filterRows = (row) -> {
            BigDecimal value = null;
            Object rowValue = row.getValue(columnName);
            if (rowValue instanceof BigDecimal bd) 
                value = bd;
            else {
                Date<?> date = safeCast(rowValue, Date.class);
                value = date.getMillisecondsSinceTheEpoch();
            }
            return filter.test(value);
        };

        Long size = null;
        startRow = (startRow == null)? 0 : startRow;
        if (startRow != null && endRow != null) size = (endRow - startRow);

        if (column.isIndexed() && !getAllRows) {

            // get all the files in the root of the column folder for each table paritition (number folders or index files)
            Deque<String> nextFiles = new ArrayDeque<>();
            nextFiles.addAll(
                table.getPartitions().parallelStream()
                .flatMap(partition -> {
                    String columnPath = Index.buildColumnPath(table.getDatabaseRoot(), table.getTableName(), partition, columnName);
                    return Arrays.stream(FileUtil.listFiles(columnPath))
                        .map(File::getPath)
                        .filter(filePath -> {
                            if (filePath.contains(".count")) return false;
                            BigDecimal value = fileNameConverter.apply(filePath);
                            return filter.test(value);
                        });
                })
                .sorted(compareFileNames)
                .toList()
            );
            
            // walk through the files, not parsing rows less than the start row
            List<Row> rows = new ArrayList<>();
            boolean done = false;
            int count = 0;
            int extra = 0;
            while (!done) {
                
                String file = nextFiles.pop();
                File[] files = FileUtil.listFilesIfDirectory(file);
                if (files != null) {
                    // since equal value folders could have overlapping sub files we need to drill down through all of them to get the files in order
                    List<String> newFilesToExplore = Arrays.stream(files)
                        .map(File::getPath)
                        .filter(filePath -> {
                            if (filePath.contains(".count")) return false;
                            BigDecimal value = fileNameConverter.apply(filePath);
                            return filter.test(value);
                        })
                        .collect(Collectors.toList());

                    String next = nextFiles.peek();
                    if (next != null) {
                        while (compareFileNames.compare(file, next) == 0) {
                            newFilesToExplore.add(next);
                            nextFiles.pop();
                            next = nextFiles.peek();
    
    
                            File[] otherFiles = FileUtil.listFilesIfDirectory(next);
                            if (otherFiles == null) {
                                newFilesToExplore.add(next);
                            }
                            else {
                                newFilesToExplore.addAll(
                                    Arrays.stream(otherFiles)
                                        .map(File::getPath)
                                        .toList()   
                                );
                            }
                        }
                    }
                    

                    // sort and add back into deque
                    newFilesToExplore.sort(compareFileNames);
                    nextFiles.addAll(newFilesToExplore);
                }

                // if we've made it to the start row start parsing rows
                boolean isIndexFile = files == null;
                if (isIndexFile) {
                    if (count >= startRow) {
                        String partition = Index.getParitionFromPath(table.getDatabaseRoot(), table.getTableName(), file);
                        List<Row> newRows = getRowsFromIndex( file, table, partition, 0, null ).toList();
                        rows.addAll( newRows );
                        count += newRows.size();
                    }
                    else {
                        count += getCountForIndexPath(file);
                        if (count >= startRow) {
                            extra += (int) (count - startRow);
                            String partition = Index.getParitionFromPath(table.getDatabaseRoot(), table.getTableName(), file);
                            List<Row> newRows = getRowsFromIndex( file, table, partition, 0, null ).toList();
                            rows.addAll( newRows );
                        }
                    }
                }
                
                // check if we're done
                done = nextFiles.isEmpty() || (endRow != null && count >= endRow);
            }
                
            if (size == null) 
                size = rows.size() - startRow;
            size = (extra + size > rows.size())? rows.size() : extra + size;

            rows.sort(comparatorRows);
            return rows.subList(extra, size.intValue());
        }
        else {
            List<Row> rows = getRowsFromTableWithoutIndices(table, 0, null).stream()
                .filter(filterRows)
                .collect(Collectors.toList());
            
            if (size == null) {
                size = rows.size() - startRow;
            }
            rows.sort(comparatorRows);
            return rows.subList(startRow.intValue(), size.intValue());
        }
    }

    /**
     * Get's all rows in the table from a partition. Doesn't use indices
     */
    public static List<Row> getAllRowsInTablePartitionWithoutIndicies(Table<?> table, String partition) {
        return getRowsFromTablePartitionWithoutIndicies(
            table,
            partition,
            row -> true,
            0,
            null
        )
        .toList();
    }




    /**
     * Converts a file name to big decimal. Only used for numeric queries really. 
     * Folders that have a + in the name are null. (eg +10)
     * @param fileName
     * @return
     */
    public static BigDecimal convertFileNameToBigDecimalUpperNull(String fileName) {
        fileName = Path.of(fileName).getFileName().toString();

        fileName = fileName.replace(".index", "").replace("-", "");

        if (fileName.contains("+"))
            return null; // returning null hear to indicate this folder could be anything over it's value

        return new BigDecimal(fileName);
    }

    /**
     * Converts a file name to big decimal. Only used for numeric queries really. 
     * Folders that have a - in the name are null. (eg -10)
     * @param fileName
     * @return
     */
    public static BigDecimal convertFileNameToBigDecimalLowerNull(String fileName) {
        fileName = Path.of(fileName).getFileName().toString();

        fileName = fileName.replace(".index", "").replace("+", "");

        if (fileName.contains("-"))
            return null; // returning null hear to indicate this folder could be anything over it's value

        return new BigDecimal(fileName);
    }


    private static Stream<Long> getNumericIndexValuesInFolder(String folderPath, Long point,
            BiPredicate<Long, Long> compareValues, boolean descending) {
        return Arrays.stream(FileUtil.listFiles(folderPath))
            .filter(File::isFile)
            .map(file -> Long.parseLong(file.getName().split(".")[0]))
            .filter(fileLong -> compareValues.test(fileLong, point))
            .sorted(
                descending ? Comparator.reverseOrder() : Comparator.naturalOrder()
            );
    }

    public static List<Long> getNumericFolders(String columnPath, Long point, BiPredicate<Long, Long> compareValues, boolean descending) {
        return Arrays.stream(FileUtil.listFiles(columnPath))
            .filter(File::isDirectory)
            .map(folder -> Long.parseLong(folder.getName().substring(1)))
            .filter(folderLong -> compareValues.test(folderLong, point))
            .sorted(
                descending ? Comparator.reverseOrder() : Comparator.naturalOrder()
            )
            .toList();
    }

    /**
     * Gets IndexRoutes from a file from startByte to numBytes or the end of the file (whichever comes first).
     * If allLines is true then startByte and numBytes are ignored
     */
    public static Bundle<Long, List<Route>> getFileSizeAndRoutes(String filePath, String partition, Long startByte, Integer numBytes) {
        try {

            byte[] bytes;
            long fileSize = FileUtil.fileSize(filePath);
            if (fileSize == 0)
                return new Bundle<>(0L, new ArrayList<>());

            numBytes = Math.toIntExact(
                (numBytes == null || fileSize < numBytes + startByte) ? fileSize - startByte : numBytes
            );
            bytes = (startByte > fileSize)? null : FileUtil.readBytes(filePath, startByte, numBytes);
            if (bytes == null)
                return new Bundle<>(fileSize, new ArrayList<>());

            return new Bundle<>(
                fileSize,
                Route.parseBytes(
                    bytes,
                    partition
                )
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error reading file for path: " + filePath,
                e
            );
        }
    }

    public static List<String> getLinesUsingRoutes(String partition, Table<?> table, List<Route> rows) {

        try {
            return FileUtil.readBytes(
                table.getTablePath(partition),
                rows.stream()
                    .map(Route::getOffsetInTable)
                    .toList(),
                rows.stream()
                    .map(Route::getLengthInTable)
                    .map(i -> (long) i)
                    .toList()
            )
            .stream()
            .map(bytes -> new String((byte[]) bytes, StandardCharsets.UTF_8) )
            .toList();
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error reading table csv file for table: " + table.getTableName(),
                e
            );
        }

    }

    public static long getCountForIndexPath(String path) {
        return FileUtil.fileSize(path) / 10;
    }

    public static long getCountFromCountFile(String path) {
        try {
            return TypeToByteUtil.byteArrayToLong(
                FileUtil.readBytes(path)
            );
        } catch (IOException e) {
            throw new DavaException(INDEX_READ_ERROR, "Couldn't read count file for index: " + path, e);
        }
    }

    public static void updateNumericCountFile(String folderPath, long change) {
        String countFile = folderPath + "/c.count";
        try {
            FileUtil.createDirectoriesIfNotExist(folderPath);

            long count = TypeToByteUtil.byteArrayToLong(
                FileUtil.readBytes(countFile, 0, 8)
            );
            FileUtil.changeCount(countFile, 0, count + change, 8);

        } catch (IOException e) {
            throw new DavaException(INDEX_CREATION_ERROR, "Error updating count for numeric index partition: " + countFile, e);
        }

    }

    public static BigDecimal repartitionNumericIndex(String folderPath, BigDecimal defaultMedian) {
        try {
            String countFile = folderPath + "/c.count";
            File[] files = FileUtil.listFiles(folderPath);

            int step = files.length / 5;
            List<BigDecimal> samples = IntStream.range(0, 5)
                .mapToObj(i ->
                    (files[i * step].getName().contains(".index")) ? new BigDecimal(files[i * step].getName().split("\\.")[0]) : defaultMedian
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
                if ( !fileName.contains(".count") ) { // looking at .index files and their respective .empties files
                    BigDecimal numberValue = new BigDecimal(fileName.split("\\.")[0]);
                    if ( numberValue.compareTo(median) < 0 ) {
                        lessThan.add(file);
                    }
                    else {
                        greaterThanOrEqual.add(file);
                    }
                }
            });

            String newUpperPath = folderPath + "/+" + median;
            String newLowerPath = folderPath + "/-" + median;

            // make copies of files in new partition folders
            FileUtil.copyFilesToDirectory(lessThan, newLowerPath);
            FileUtil.copyFilesToDirectory(greaterThanOrEqual, newUpperPath);

            FileUtil.createFile(folderPath + "/-" + median + "/c.count", TypeToByteUtil.longToByteArray(lessThan.size()) );
            FileUtil.createFile(folderPath + "/+" + median + "/c.count", TypeToByteUtil.longToByteArray(greaterThanOrEqual.size()) );

            // delete original files
            FileUtil.deleteFile(countFile);
            for (File file : lessThan) {
                FileUtil.deleteFile(file);
            }
            for (File file : greaterThanOrEqual) {
                FileUtil.deleteFile(file);
            }

            return median;
        } catch (IOException e) {
            throw new DavaException(INDEX_CREATION_ERROR, "Repartition of numerical index failed: " + folderPath, e);
        }
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

    public static void popRoutes(String emptiesFile, List<Integer> startIndices) throws IOException {

        if (startIndices.isEmpty())
            return;

        List<Long> startBytes = startIndices.stream()
            .map(i -> 8 + i * 10L)
            .collect(Collectors.toList());

        FileUtil.popBytes(emptiesFile, 10, startBytes);
    }

    public static List<Route> getAllEmpties(String emptiesFile) throws IOException {
        // TODO don't check for empties every time to avoid this costly file access
        long fileSize = FileUtil.fileSize(emptiesFile);
        if ( fileSize - 10 <= 8) {
            return null;
        }

        EmptiesPackage emptiesPackages = new EmptiesPackage();

        byte[] bytes = FileUtil.readBytes(emptiesFile);
        return Route.parseBytes(
            ArrayUtil.subRange(bytes, 8, bytes.length),
            null
        );
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
