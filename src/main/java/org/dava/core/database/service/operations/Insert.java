package org.dava.core.database.service.operations;

import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.Rollback;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.operations.common.Batch;
import org.dava.core.database.service.operations.common.EmptiesPackage;
import org.dava.core.database.service.operations.common.WritePackage;
import org.dava.core.database.service.operations.delete.CountChange;
import org.dava.core.database.service.operations.insert.IndexWritePackage;
import org.dava.core.database.service.operations.insert.RowWritePackage;
import org.dava.core.database.service.structure.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

import static org.dava.core.database.objects.exception.ExceptionType.*;


public class Insert {

    private Database database;
    private Table<?> table;
    private long tableSize;
    private String partition;

    public FileUtil fileUtil = new FileUtil();

//    private Logger log = Logger.getLogger(Insert.class.getName());

    private EmptiesPackage rowEmpties;

    public Insert(Database database, Table<?> table, String partition) {
        this.database = database;
        this.table = table;
        this.tableSize = fileUtil.fileSize(table.getTablePath(partition));
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
    public Batch insert(List<Row> rows, boolean replaceRollbackFile, Batch batch) {
        // TODO instead of using one partition, use multiple to be able to do all this in parallel

        this.rowEmpties = table.getEmptyRows(partition);

        // make write packages
        List<RowWritePackage> rowWritePackages = makeWritePackages(rows);

        // build batch
        groupIndexWrites(batch, database, table, rowWritePackages);
        if (table.getMode() != Mode.LIGHT) {
            batch.setUsedTableEmtpies(this.rowEmpties);
        }
        batch.setRowsWritten(rowWritePackages);
        batch.setOldTableSize( table.getSize(partition) );

        // repartition numeric indices (this is a write, but is done safely, repartitions aren't rolled back)
        try {
            Map<String, List<IndexWritePackage>> updatedWritePackages =
                repartitionNumericIndices(table, partition, batch, replaceRollbackFile);
                batch.setIndexPathToIndicesWritten(updatedWritePackages);
        } catch(Exception e) {
            // the repartition fails itself, then we determine how to finish it (can undo what was done or finish it)
            Rollback.handleNumericRepartitionFailure(table, partition);
        }
        
        // build and log rollback
        logRollback(batch.makeRollbackString(table, partition), table.getRollbackPath(partition), replaceRollbackFile);

        return batch;
        // perform insert
        // performInsert(batch);
    }

    private void logRollback(String logString, String rollbackLogPath, boolean replaceRollbackFile) {
        try {
            
            if (replaceRollbackFile)
                fileUtil.replaceFile(rollbackLogPath, logString.getBytes(StandardCharsets.UTF_8) );
            else
                fileUtil.writeBytesAppend(rollbackLogPath, logString.getBytes(StandardCharsets.UTF_8) );
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
        logRollback(repartitionRollbackString.toString(), table.getNumericRollbackPath(partition), replaceRollbackFile);



        // determine folders to repartition
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
            if (affectedIndexPaths != null) {
                BigDecimal defaultMedian = new BigDecimal(
                    Path.of( affectedIndexPaths.get(0) ).getFileName().toString()
                        .replace(".index", "").replace("+", "").replace("-", "")
                );
                
                // actual repartition takes place
                BigDecimal median = BaseOperationService.repartitionNumericIndex(folderPath, defaultMedian);
                insertBatch.setNumericRepartitionOccured(true);

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

    private void groupIndexWrites(Batch batch, Database database, Table<?> table, List<RowWritePackage> writePackages) {
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

                    // determine if we're planning on making new indices in a numeric repartition
                    if ( Index.isNumericallyIndexed(column.getType()) && !fileUtil.exists(indexPath) && !countedIndexPaths.contains(indexPath)) {
                        CountChange count = countUpdates.get(folderPath);
                        countUpdates.put(
                            folderPath,
                            new CountChange(0L, (count == null)? 1L : count.getChange() + 1)
                        );
                        countedIndexPaths.add(indexPath);
                    }

                    // add to batch
                    batch.addIndexWritePackage(
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

        batch.setNumericCountFileChanges(countUpdates);
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

            fileUtil.writeBytes(
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
