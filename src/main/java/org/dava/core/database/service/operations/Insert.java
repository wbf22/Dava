package org.dava.core.database.service.operations;

import org.dava.core.database.objects.dates.Date;
import org.dava.core.common.Bundle;
import org.dava.core.common.TypeUtil;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.operations.common.Batch;
import org.dava.core.database.service.operations.common.EmptiesPackage;
import org.dava.core.database.service.operations.common.WritePackage;
import org.dava.core.database.service.operations.delete.CountChange;
import org.dava.core.database.service.operations.insert.IndexWritePackage;
import org.dava.core.database.service.operations.insert.RowWritePackage;
import org.dava.core.database.service.structure.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.dava.core.common.Checks.safeCast;
import static org.dava.core.database.objects.exception.ExceptionType.*;


public class Insert {

    private Database database;
    private Table<?> table;
    private long tableSize;
    private String partition;

//    private Logger log = Logger.getLogger(Insert.class.getName());

    private EmptiesPackage rowEmpties;

    public Insert(Database database, Table<?> table, String partition) {
        this.database = database;
        this.table = table;
        this.tableSize = FileUtil.fileSize(table.getTablePath(partition));
        this.partition = partition;
    }


    /**
     * Inserts a list of rows into the table and partition provided when creating the insert.
     * 
     * <p> If 'replaceRollbackFile' is false, then this operation
     * will just append it's rollback information to the rollback file. This effective places
     * the work in this insert in a transaction with whatever the last operation was. To
     * have multiple statements in a transaction, you want to have the first operation replace
     * the rollback file, and then all subsequent operations append to it.
     * 
     * 
     * @param rows
     * @param replaceRollbackFile
     */
    public void insert(List<Row> rows, boolean replaceRollbackFile) {
        // TODO instead of using one partition, use multiple to be able to do all this in parallel

        this.rowEmpties = table.getEmptyRows(partition);

        // make write packages
        List<RowWritePackage> rowWritePackages = makeWritePackages(rows);

        // build batch
        Batch writeInsertBatch = groupIndexWrites(database, table, rowWritePackages);
        if (table.getMode() != Mode.LIGHT) {
            writeInsertBatch.setUsedTableEmtpies(this.rowEmpties);
        }
        writeInsertBatch.setRowsWritten(rowWritePackages);
        writeInsertBatch.setOldTableSize( table.getSize(partition) );

        // repartition numeric indices (this is a write, but is done safely, repartitions aren't rolled back)
        Map<String, List<IndexWritePackage>> updatedWritePackages =
            repartitionNumericIndices(table, partition, writeInsertBatch, replaceRollbackFile);
        writeInsertBatch.setIndexPathToIndicesWritten(updatedWritePackages);

        // build and log rollback
        logRollback(writeInsertBatch.makeRollbackString(table, partition), table.getRollbackPath(partition), replaceRollbackFile);

        // perform insert
        performInsert(writeInsertBatch);
    }

    private void logRollback(String logString, String rollbackLogPath, boolean replaceRollbackFile) {
        try {
            
            if (replaceRollbackFile)
                FileUtil.replaceFile(rollbackLogPath, logString.getBytes(StandardCharsets.UTF_8) );
            else
                FileUtil.writeBytesAppend(rollbackLogPath, logString.getBytes(StandardCharsets.UTF_8) );
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error writing to rollback log", e);
        }
    }

    private void performInsert(Batch insertBatch) {

        // add to table
        addToTable(table, partition, insertBatch.getRowsWritten(), insertBatch.getOldTableSize());

        // add indexes
        batchWriteToIndices(insertBatch.getIndexPathToIndicesWritten());

        // remove used empties from tables
        try {
            BaseOperationService.popRoutes(
                table.emptiesFilePath(partition),
                insertBatch.getUsedTableEmtpies().getUsedEmpties()
                    .values()
                    .stream().flatMap(Collection::stream)
                    .map(Empty::getIndex)
                    .toList()
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error removing empty in table meta file: " + table.emptiesFilePath(partition),
                e
            );
        }

        // reinit table leaves if a numeric repartition occurred
        if (!insertBatch.getNumericCountFileChanges().isEmpty()) {
            table.initColumnLeaves();
        }
    }

    private Map<String, List<IndexWritePackage>> repartitionNumericIndices(Table<?> table, String partition, Batch insertBatch, boolean replaceRollbackFile) {
        Set<String> foldersToRepartition = new HashSet<>();

        // get the repartitions and log rollback for that
        StringBuilder repartitionRollbackString = new StringBuilder();
        insertBatch.getNumericCountFileChanges().forEach((folderPath, count) -> {

            String countFile = folderPath + "/c.count";
            long currentCount = BaseOperationService.getCountFromCountFile(countFile);
            if (currentCount + count.getChange() > BaseOperationService.NUMERIC_PARTITION_SIZE) {
                repartitionRollbackString.append("N:").append(folderPath).append("\n");
                foldersToRepartition.add(folderPath);
            }
        });
        logRollback(repartitionRollbackString.toString(), table.getRollbackPath(partition), replaceRollbackFile);



        // do the repartition
        Map<String, List<String>> folderPathToIndexPaths = new HashMap<>();
        insertBatch.getIndexPathToIndicesWritten().forEach( (indexPath, writePackages) -> {
            IndexWritePackage firstPackage = writePackages.get(0);
            if( Index.isNumericallyIndexed(firstPackage.getColumnType()) ) {
                String folderPath = firstPackage.getFolderPath();
                List<String> indexPaths = folderPathToIndexPaths.get(folderPath);
                if (indexPaths == null) {
                    folderPathToIndexPaths.put(
                        folderPath,
                        new ArrayList<>(List.of(indexPath))
                    );
                }
                else {
                    indexPaths.add(indexPath);
                }
            }

        });

        foldersToRepartition.forEach(folderPath -> {
            List<String> affectedIndexPaths = folderPathToIndexPaths.get(folderPath);
            BigDecimal defaultMedian = new BigDecimal(
                Path.of( affectedIndexPaths.get(0) ).getFileName().toString()
                    .replace(".index", "").replace("+", "").replace("-", "")
            );
            if (affectedIndexPaths != null) {
                BigDecimal median = BaseOperationService.repartitionNumericIndex(folderPath, defaultMedian);
                affectedIndexPaths.forEach(indexPath -> {
                    List<IndexWritePackage> writePackages = insertBatch.getIndexPathToIndicesWritten().get(indexPath);

                    insertBatch.getIndexPathToIndicesWritten().remove(indexPath);
                    
                    BigDecimal numericValue = new BigDecimal( writePackages.get(0).getValue().toString() );

                    StringBuilder newIndexPathBD = new StringBuilder(folderPath);
                    if ( median.compareTo(numericValue) > 0 ) {
                        newIndexPathBD.append("/-").append(median);
                    }
                    else {
                        newIndexPathBD.append("/+").append(median);
                    }
                    writePackages.forEach( writePackage -> writePackage.setFolderPath(newIndexPathBD.toString()) );
                    newIndexPathBD.append("/").append(numericValue).append(".index");

                    String newIndexPath = newIndexPathBD.toString();
                    insertBatch.getIndexPathToIndicesWritten().put(newIndexPath, writePackages);
                });
            }
        });

        return insertBatch.getIndexPathToIndicesWritten();
    }

    private void rollbackNumericRepartitionFailure() {
        // on rollback, if not all were moved, delete all in new partitions, else delete all in current partition
        List<String> lines = new ArrayList<>();
        try {
            lines = List.of(
                new String(
                    FileUtil.readBytes(table.getRollbackPath(partition)), StandardCharsets.UTF_8
                ).split("\n")
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            for (String line : lines) {
                String folderPath = line.split(":")[1];
                File[] files = FileUtil.listFiles(folderPath);
                List<File> subFiles = new ArrayList<>();
                List<File> filesInCurrent = new ArrayList<>();
                List<File> directories = new ArrayList<>();
                for (File file : files) {
                    if (file.isDirectory()) {
                        directories.add(file);
                    }
                    else {
                        filesInCurrent.add(file);
                        subFiles.addAll(
                            List.of(FileUtil.listFiles(file.getPath()))
                        );
                    }
                }

                // if all were moved, delete files in current partition
                // subFiles.size() should be filesInCurrent.size() + 1 if moved
                // if all were moved and some were deleted from current, then we can safely delete all in current
                if (subFiles.size()-1 > filesInCurrent.size()) {
                    for (File file : filesInCurrent) {
                        FileUtil.deleteFile(file);
                    }
                }
                else {
                    for (File file : subFiles) {
                        FileUtil.deleteFile(file);
                    }
                }
            }
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error in rolling back repartition that failed during insert", e);
        }

    }


    /*
        Non-transaction (no write) helper methods
     */
    private List<RowWritePackage> makeWritePackages(List<Row> rows) {
        return rows.stream()
            .map(row ->{
                String rowString = Row.serialize(table, row.getColumnsToValues()) + "\n";
                byte[] bytes = rowString.getBytes(StandardCharsets.UTF_8);

                Long offset;
                int lengthInTable = bytes.length;
                if (rowEmpties.contains(lengthInTable)) {
                    offset = rowEmpties.getEmptyRemember(lengthInTable).getRoute().getOffsetInTable();
                }
                else {
                    offset = tableSize;
                    tableSize += lengthInTable;
                }

                Route route = new Route(
                    partition,
                    offset,
                    lengthInTable
                );

                return new RowWritePackage(route, row, bytes);
            })
            .toList();
    }

    private Batch groupIndexWrites(Database database, Table<?> table, List<RowWritePackage> writePackages) {
        Batch insertBatch = new Batch();
        Map<String, CountChange> countUpdates = new HashMap<>();
        Set<String> countedIndexPaths = new HashSet<>();
        writePackages.forEach( writePackage -> {
            Row row = writePackage.getRow();
            for (Map.Entry<String, Object> columnValue : row.getColumnsToValues().entrySet()) {
                Column<?> column = table.getColumn(columnValue.getKey());

                if (column.isIndexed()) {
                    String folderPath = Index.buildIndexRootPath(
                        database.getRootDirectory(),
                        table,
                        partition,
                        column,
                        columnValue.getValue()
                    );

                    Object value = Index.prepareValueForIndexName(columnValue.getValue(), column);
                    String indexPath = Index.indexPathBypass(folderPath, value);

                    if ( Index.isNumericallyIndexed(column.getType()) && !FileUtil.exists(indexPath) && !countedIndexPaths.contains(indexPath)) {
                        CountChange count = countUpdates.get(folderPath);
                        countUpdates.put(
                            folderPath,
                            new CountChange(0L, (count == null)? 1L : count.getChange() + 1)
                        );
                        countedIndexPaths.add(indexPath);
                    }

                    // add to batch
                    insertBatch.addIndexWritePackage(
                        indexPath,
                        new IndexWritePackage(
                            writePackage.getRoute(),
                            column,
                            value,
                            folderPath
                        )
                    );
                }
            }
        });

        insertBatch.setNumericCountFileChanges(countUpdates);
        return insertBatch;
    }



    /*
        Transactions (methods that write to files and need to be rolled back on failure)
     */
    private void batchWriteToIndices(Map<String, List<IndexWritePackage>> indexWritePackages) {

        indexWritePackages.forEach( (indexPath, indexPackages) -> {
                IndexWritePackage first = indexPackages.get(0);
                String folderPath = first.getFolderPath();
                Object value = first.getValue();
                Column<?> column = table.getColumn(first.getColumnName());
                BaseOperationService.addToIndex(folderPath, value, indexPackages, column.isUnique());
            });
    }

    private void addToTable(Table<?> table, String partition, List<RowWritePackage> writePackages, long oldTableSize) {
        String path = table.getTablePath(partition);
        try {

            FileUtil.writeBytes(
                path,
                (List<WritePackage>) (List<?>) writePackages
            );

            // update table row count in empties
            long newSize = oldTableSize + writePackages.size();
            table.setSize(partition, newSize);

        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing row to: " + path,
                e
            );
        }
    }




}
