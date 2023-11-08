package org.dava.core.database.service;

import org.dava.common.TypeUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.*;
import org.dava.core.database.service.objects.insert.Batch;
import org.dava.core.database.service.objects.insert.IndexWritePackage;
import org.dava.core.database.service.objects.insert.RowWritePackage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.dava.core.database.objects.exception.ExceptionType.*;
import static org.dava.core.database.service.BaseOperationService.NUMERIC_PARTITION_SIZE;
import static org.dava.core.database.service.BaseOperationService.getNumericCount;


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


    public void insert(List<Row> rows) {
        // TODO instead of using one partition, use multiple to be able to do all this in parallel

        this.rowEmpties = table.getEmptyRows(partition);

        // make write packages
        List<RowWritePackage> rowWritePackages = makeWritePackages(rows);

        // build batch
        Batch writeBatch = groupIndexWrites(database, table, rowWritePackages);
        if (table.getMode() != Mode.LIGHT) {
            writeBatch.setNumericRepartitions( determineNumericPartitions(writeBatch) );
            writeBatch.setTableEmpties(this.rowEmpties);
        }
        writeBatch.setRowWritePackages(rowWritePackages);

        // build and log rollback
        logRollback(writeBatch, table.getRollbackPath(partition));

        // perform insert
        performInsert(writeBatch);
    }

    private List<String> determineNumericPartitions(Batch writeBatch) {
        // TODO decide if this is necessary. It might be fine to not rollback repartitions, however this has no significant cost
        List<String> indexRepartitions = new ArrayList<>();
        writeBatch.getIndexWriteGroups()
            .forEach( (indexPath, writeGroup) -> {
                if ( writeGroup.get(0).getColumn().isIndexed() && TypeUtil.isNumericClass(writeGroup.get(0).getColumnType()) ) {
                    String folderPath = writeGroup.get(0).getFolderPath();
                    try {
                        String countFile = folderPath + "/c.count";
                        long count = getNumericCount(countFile);
                        if (count > NUMERIC_PARTITION_SIZE)
                            indexRepartitions.add(folderPath);

                    } catch (IOException e) {
                        throw new DavaException(INDEX_CREATION_ERROR, "Repartition of numerical index failed: " + folderPath, e);
                    }
                }
            });
        return indexRepartitions;
    }

    private void logRollback(Batch writeBatch, String rollbackLogPath) {
        try {
            String rollback = writeBatch.makeRollbackString();
            FileUtil.replaceFile(rollbackLogPath, rollback.getBytes() );
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error writing to rollback log", e);
        }
    }

    private void performInsert(Batch batch) {

        // add to table
        addToTable(table, partition, batch.getRowWritePackages());

        // add indexes
        batchWriteToIndices(batch.getIndexWriteGroups());

        // remove used empties from tables
        try {
            BaseOperationService.popRoutes(
                table.emptiesFilePath(partition),
                batch.getTableEmpties().getUsedEmpties()
                    .values()
                    .stream().flatMap(Collection::stream)
                    .toList()
            );
        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error removing empty in table meta file: " + table.emptiesFilePath(partition),
                e
            );
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

                IndexRoute route = new IndexRoute(
                    partition,
                    offset,
                    lengthInTable
                );

                return new RowWritePackage(route, row, bytes);
            })
            .toList();
    }

    private Batch groupIndexWrites(Database database, Table<?> table, List<RowWritePackage> writePackages) {
        Batch batch = new Batch();
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
        return batch;
    }



    /*
        Transactions (methods that write to files and need to be rolled back on failure)
     */
    private void batchWriteToIndices(Map<String, List<IndexWritePackage>> indexWritePackages) {

        Map<String, Long> numericCounts = new HashMap<>();
        Map<String, Long> countUpdates = new HashMap<>();

        indexWritePackages.forEach( (indexPath, indexPackages) -> {
                IndexWritePackage first = indexPackages.get(0);
                String folderPath = first.getFolderPath();
                Object value = first.getValue();
                Column<?> column = table.getColumn(first.getColumnName());

                if ( TypeUtil.isNumericClass(indexPackages.get(0).getColumnType() ) ) {
                    if (!FileUtil.exists(indexPath)) {
                        long current = countUpdates.getOrDefault(folderPath, 0L);
                        countUpdates.put(folderPath, current + 1);
                    }
                    BaseOperationService.addToIndex(folderPath, value, indexPackages, column.isUnique());
                    boolean repartitioned = BaseOperationService.repartitionIfNeeded(folderPath, numericCounts);
                    if (repartitioned) {
                        table.initColumnLeaves();
                        countUpdates.remove(folderPath);
                    }
                }
                else {
                    BaseOperationService.addToIndex(folderPath, value, indexPackages, column.isUnique());
                }
            });

        countUpdates.forEach(BaseOperationService::updateNumericCountFile);
    }

    private void addToTable(Table<?> table, String partition, List<RowWritePackage> writePackages) {
        String path = table.getTablePath(partition);
        try {

            FileUtil.writeBytes(
                path,
                (List<WritePackage>) (List<?>) writePackages
            );

            // update table row count in empties
            long oldSize = table.getSize(partition);
            long newSize = oldSize + writePackages.size();
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
