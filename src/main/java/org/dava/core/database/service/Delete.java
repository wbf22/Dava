package org.dava.core.database.service;

import org.dava.common.ArrayUtil;
import org.dava.common.Bundle;
import org.dava.common.TypeUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.Batch;
import org.dava.core.database.service.objects.WritePackage;
import org.dava.core.database.service.objects.delete.CountChange;
import org.dava.core.database.service.objects.delete.IndexDelete;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.dava.core.database.objects.exception.ExceptionType.*;

public class Delete {


    private final Database database;
    private final Table<?> table;

    public Delete(Database database, Table<?> table) {
        this.database = database;
        this.table = table;
    }


    public void delete(List<Row> rows) {
        Map<String, Batch> deleteBatchesByPartition = table.getPartitions().parallelStream()
            .map(partition -> {
                Batch batch = new Batch();
                batch.setRows(rows);

                // TODO, on light mode neither of these should be needed
                // determine old table size
                long tableSize = table.getSize(partition);
                batch.setOldTableSize(tableSize);

                // determine old table empties file size
                long emptiesSize = FileUtil.fileSize(table.emptiesFilePath(partition));
                batch.setOldEmptiesSize(emptiesSize);


                // determine all indices that point to the row and determine changes to indices (routes and file deletes)
                // determine changes to numeric count files
                Bundle< Map<String, CountChange>, Map<String, IndexDelete> > data = collectIndexData(rows, partition);
                batch.setNumericCountFileChanges(data.getFirst());
                batch.setIndexPathToInvalidRoutes(data.getSecond());

                return Map.entry(partition, batch);
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        execute(deleteBatchesByPartition);
    }

    private void execute(Map<String, Batch> deleteBatchesByPartition) {
        // log rollback
        table.getPartitions().parallelStream().forEach( partition -> {
            try {
                Batch batch = deleteBatchesByPartition.get(partition);
                String rollback = batch.makeRollbackString(table, partition);
                FileUtil.replaceFile(table.getRollbackPath(partition), rollback.getBytes() );
            } catch (IOException e) {
                throw new DavaException(ROLLBACK_ERROR, "Error writing to rollback log", e);
            }
        });

        // perform delete
        deleteBatchesByPartition.entrySet().parallelStream()
            .forEach( entry -> {
                if (table.getMode() == Mode.LIGHT)
                    performDeleteLightMode(entry.getValue(), entry.getKey());
                else
                    performDelete(entry.getValue(), entry.getKey());
            } );
    }

    private void performDelete(Batch batch, String partition) {
        // whitespace rows in table and add routes to empties
        List<WritePackage> overwritePackages = new ArrayList<>();
        List<Object> emptiesWrites = new ArrayList<>();
        batch.getRows().forEach( row -> {
            Route location = row.getLocationInTable();
            byte[] whitespaceBytes = Table.getWhitespaceBytes(location.getLengthInTable());
            overwritePackages.add(
                new WritePackage(location.getOffsetInTable(), whitespaceBytes)
            );

            emptiesWrites.add(
                location.getRouteAsBytes()
            );
        });

        try {
            FileUtil.writeBytes(
                table.getTablePath(partition),
                overwritePackages
            );

            FileUtil.writeBytesAppend(
                table.emptiesFilePath(partition),
                ArrayUtil.appendArrays(emptiesWrites, 10)
            );
        } catch (IOException e) {
            throw new DavaException(BASE_IO_ERROR, "Error deleting rows from table", e);
        }

        // update table size
        table.setSize(partition, batch.getOldTableSize() - batch.getRows().size());

        // delete indices
        batch.getIndexPathToInvalidRoutes().entrySet().parallelStream()
            .forEach( entry -> {
                try {
                    String indexPath = entry.getKey();
                    IndexDelete indexDelete = entry.getValue();

                    long newSize = FileUtil.popBytes(indexPath, 10, indexDelete.getIndicesToDelete());
                    if (newSize == 0)
                        FileUtil.deleteFile(indexPath); // this is important as during rollbacks table counts are updated by the number of primary key index files

                } catch (IOException e) {
                    throw new DavaException(BASE_IO_ERROR, "Error updating indices after delete", e);
                }
            });

        // update numeric count files
        batch.getNumericCountFileChanges().forEach( (countFile, countChange) -> {
            String folderPath = countFile.replace("/c.count", "");
            BaseOperationService.updateNumericCountFile(folderPath, countChange.getChange());
        });

    }


    private void performDeleteLightMode(Batch batch, String partition) {

        // get all rows, and filter by ones in the delete
        List<String> deleteRows = batch.getRows().stream()
            .map( row -> Row.serialize(table, row.getColumnsToValues()) )
            .toList();

        String rows = BaseOperationService.getAllRows(
            table,
            partition
        ).stream()
            .map(row -> Row.serialize(table, row.getColumnsToValues()))
            .filter( row ->
                 deleteRows.stream()
                    .map( deleteRow -> !row.equals(deleteRow) )
                    .reduce(Boolean::logicalAnd)
                    .orElse(true)
            )
            .collect(Collectors.joining("\n")) + "\n";

        // delete the old file and write all rows back
        String tablePath = table.getTablePath(partition);
        try {
            FileUtil.deleteFile(tablePath);

            table.initTableCsv(partition);

            long offset = FileUtil.fileSize(tablePath);
            FileUtil.writeBytes(tablePath, offset, rows.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new DavaException(BASE_IO_ERROR, "Failed trying to delete and recreate table", e);
        }
    }

    private Bundle<Map<String, CountChange>, Map<String, IndexDelete>> collectIndexData(List<Row> rows, String partition) {

        Map<String, Integer> numericIndexPathToIndicesLeft = new ConcurrentHashMap<>();
        Map<String, IndexDelete> indexPathToIndexDeletes = new ConcurrentHashMap<>();

        rows.forEach( row -> row.getColumnsToValues().entrySet().parallelStream() // TODO determine if this parallel stream helps
            .forEach( entry -> {

                Column<?> column = table.getColumn(entry.getKey());
                if (column.isIndexed()) {
                    String indexPath = Index.buildIndexPath(
                        table,
                        partition,
                        column.getName(),
                        entry.getValue()
                    );

                    Bundle<Long, List<Route>> bundle = BaseOperationService.getFileSizeAndRoutes(
                        indexPath,
                        partition,
                        0L,
                        null
                    );

                    // figure out which routes (ordered) should be deleted
                    List<Route> lines = bundle.getSecond();
                    List<Long> startBytesOfRoutesToDelete = LongStream.range(0, lines.size())
                        .filter(i -> lines.get((int)i).equals(row.getLocationInTable()))
                        .boxed()
                        .map(i -> i * 10)
                        .toList();

                    if (TypeUtil.isNumericClass(column.getType())) {

                        if (numericIndexPathToIndicesLeft.containsKey(indexPath)) {
                            Integer numLeft = numericIndexPathToIndicesLeft.get(indexPath);
                            numericIndexPathToIndicesLeft.put(indexPath, numLeft - startBytesOfRoutesToDelete.size());
                        }
                        else {
                            numericIndexPathToIndicesLeft.put(indexPath, lines.size() - startBytesOfRoutesToDelete.size());
                        }
                    }

                    // add routes to delete to map
                    if (indexPathToIndexDeletes.containsKey(indexPath)) {
                        indexPathToIndexDeletes.get(indexPath).addRoutesToDelete(startBytesOfRoutesToDelete);
                    }
                    else {
                        indexPathToIndexDeletes.put(indexPath, new IndexDelete(startBytesOfRoutesToDelete, lines));
                    }
                }
            }));

        // determine if count file will change for numeric types
        Map<String, CountChange> numericCountFileChanges = new HashMap<>();
        numericIndexPathToIndicesLeft.forEach( (indexPath, count) -> {
            if (count == 0) {
                String indexFolder = Path.of(indexPath).getParent().toString();

                // TODO fix but with count file value
                String countFile = indexFolder  + "/c.count";
                try {
                    if (numericCountFileChanges.containsKey(countFile)) {
                        numericCountFileChanges.get(countFile).decrementNewCount();
                    }
                    else {
                        long oldCount = BaseOperationService.getNumericCount(countFile);
                        numericCountFileChanges.put(
                            countFile,
                            new CountChange(
                                oldCount,
                                -1
                            )
                        );
                    }
                } catch (IOException e) {
                    throw new DavaException(INDEX_READ_ERROR, "Error getting count for numeric index: " + countFile, e);
                }
            }
        });

        return new Bundle<>(numericCountFileChanges, indexPathToIndexDeletes);
    }


}
