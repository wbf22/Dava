package org.dava.core.database.service;

import org.dava.common.HashUtil;
import org.dava.common.TypeUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.dava.core.database.objects.exception.ExceptionType.INDEX_READ_ERROR;
import static org.dava.core.database.objects.exception.ExceptionType.ROLLBACK_ERROR;


public class Insert {

    private Database database;
    private Table<?> table;
    private String partition;
    boolean index;


    private EmptiesPackage rowEmpties;

    public Insert(Database database, Table<?> table, String partition, boolean index) {
        this.database = database;
        this.table = table;
        this.partition = partition;
        this.index = index;
    }


    public boolean insert(List<Row> rows) {
        String partition = table.getRandomPartition();
//        Long tableSize = FileUtil.fileSize(table.getTablePath(partition) + ".csv");
        String rollbackPath = table.getRollbackPath(partition);

        this.rowEmpties = table.getEmptyRows(partition, rows.size());

        // make write packages
        List<RowWritePackage> rowWritePackages = makeWritePackages(rows);

        // use empties to set routes for write packages if available
        Batch writeBatch = groupIndexWrites(database, table, rowWritePackages);
        writeBatch.setTableEmpties(this.rowEmpties);


        // build and log rollback
        logRollback(writeBatch, table.getRollbackPath(partition));

        // perform insert
        performInsert(rows, emptiesPackages, database, table, index, partition, tableSize, rollbackPath);

        return true;
    }

    private void logRollback(Batch writeBatch, String rollbackLogPath) {
        try {
            String rollback = writeBatch.makeRollbackString();
            FileUtil.replaceFile(rollbackLogPath, rollback.getBytes() );
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error writing to rollback log", e);
        }
    }

    private void performInsert(List<Row> rows, Map<Integer, Empty> emptiesPackages, Database database, Table<?> table, boolean index, String partition, Long tableSize, String rollbackPath) {
        // serialize rows and make routes/ write packages (using empty rows)
        List<RowWritePackage> writePackages = makeWritePackages(emptiesPackages, rows, table, partition, tableSize).getTransactionResult();
        table.popEmptiesRoutes(partition, emptiesToPop);

        // add to table
        addToTable(table, partition, rollbackPath, writePackages);

        // add indexes
        index(database, table, index, rollbackPath, writePackages);
    }




    /*
        Non-transaction (no write) helper methods
     */
    private List<RowWritePackage> makeWritePackages(List<Row> rows) {
        List<RowWritePackage> writePackages = rows.stream()
            .map(row ->{
                String rowString = Row.serialize(table, row.getColumnsToValues()) + "\n";
                byte[] bytes = rowString.getBytes(StandardCharsets.UTF_8);


                Long offset = null;
                int lengthInTable = bytes.length;
                if (rowEmpties.contains(lengthInTable)) {
                    offset = rowEmpties.getEmptyRemember(lengthInTable).getRoute().getOffsetInTable();
                }

                IndexRoute route = new IndexRoute(
                    partition,
                    offset,
                    lengthInTable
                );

                return new RowWritePackage(route, row, bytes);
            })
            .toList();

        return writePackages;
    }

    private Batch groupIndexWrites(Database database, Table<?> table, List<RowWritePackage> writePackages) {
        Batch batch = new Batch();
        Map<String, EmptiesPackage> indexEmpties = new HashMap<>();

        writePackages.parallelStream()
            .forEach( writePackage -> {
                Row row = writePackage.getRow();
                for (Map.Entry<String, Object> columnValue : row.getColumnsToValues().entrySet()) {
                    String folderPath = Index.buildIndexRootPath(
                        database.getRootDirectory(),
                        table.getTableName(),
                        partition,
                        table.getColumn(columnValue.getKey()),
                        columnValue.getValue()
                    );

                    Column<?> column = table.getColumn(columnValue.getKey());
                    Object value = getValue(columnValue, column);
                    String indexPath = folderPath + "/" + value + ".index";

                    // get empty index rows if applicable (any non-unique indices)
                    Empty indexEmpty = null;
                    if (!column.isUnique()) {
                        if (indexEmpties.containsKey(indexPath)) {
                            EmptiesPackage emptiesForIndex = indexEmpties.get(indexPath);
                            indexEmpty = emptiesForIndex.getEmptyRemember(10);
                        }
                        else {
                            try {
                                EmptiesPackage emptiesPackage = BaseOperationService.getEmpties(
                                    null,
                                    folderPath + "/" + value + ".empties",
                                    table.getRandom()
                                );
                                indexEmpties.put(indexPath, emptiesPackage);
                            } catch (IOException e) {
                                throw new DavaException(INDEX_READ_ERROR, "Error getting empties for index: " + indexPath,  e);
                            }
                        }
                    }

                    // add to batch
                    batch.addIndexWritePackage(
                        indexPath,
                        new IndexWritePackage(
                            writePackage.getRoute(),
                            indexEmpty,
                            column.getType(),
                            value,
                            folderPath
                        )
                    );

                }
            });
        batch.setIndexEmpties(indexEmpties);
        return batch;
    }


    public Object getValue(Map.Entry<String, Object> columnValue, Column<?> column) {
        Object value = columnValue.getValue();

        // if it's a date get the local data version
        if (Date.isDateSupportedDateType(column.getType() )) {
            value = Date.of(
                value.toString(),
                table.getColumn(columnValue.getKey()).getType()
            ).getDateWithoutTime().toString();
        }

        // limit file name less than 255 bytes for ext4 file system
        value = value.toString();
        byte[] bytes = value.toString().getBytes();
        if (bytes.length > 240) {
            value = HashUtil.hashToUUID(bytes);
        }

        return value;
    }



    /*
        Transactions (methods that write to files and need to be rolled back on failure)
     */

    private Transaction<Void> index(Database database, Table<?> table, boolean index, String rollbackPath, List<RowWritePackage> writePackages) {

    }

    private Transaction<Void> batchWriteToIndices(String rollbackPath, Map<String, List<IndexWritePackage>> indexWritePackageMap) {
        indexWritePackageMap.values().parallelStream()
            .forEach( indexPackages -> {

                String folderPath = indexPackages.get(0).getFolderPath();
                Object value = indexPackages.get(0).getValue();
                List<IndexRoute> routes = indexPackages.stream()
                    .map(IndexWritePackage::getRoute)
                    .toList();

                if ( TypeUtil.isNumericClass(indexPackages.get(0).getColumnType() ) ) {
                    if (!FileUtil.exists(folderPath + "/" + value + ".index")) {
                        BaseOperationService.updateNumericCountFile(folderPath);
                    }
                    BaseOperationService.addToIndex(folderPath, value, routes, rollbackPath);
                    BaseOperationService.repartitionIfNeeded(folderPath, rollbackPath);
                }
                else {
                    BaseOperationService.addToIndex(folderPath, value, routes, rollbackPath);
                }
            });

        return new Transaction<>("", null);
    }

    private Transaction<Void> addToTable(Table<?> table, String partition, String rollbackPath, List<RowWritePackage> writePackages) {
        String path = table.getTablePath(partition) + ".csv";
        try {
            StringBuilder rollbackString = new StringBuilder();
            writePackages.forEach(writePackage ->
                   rollbackString.append("W:")
                       .append(writePackage.getRoute().getOffsetInTable())
                       .append(",")
                       .append(writePackage.getRoute().getLengthInTable())
                       .append("\n")
            );

            BaseOperationService.performOperation(
                () -> FileUtil.writeBytes(
                    path,
                    writePackages
                ),
                rollbackString.toString(),
                rollbackPath
            );


            // update table row count in empties
            long oldSize = table.getSize(partition);
            long newSize = oldSize + writePackages.size();
            table.setSize(partition, oldSize, newSize);

        } catch (IOException e) {
            throw new DavaException(
                BASE_IO_ERROR,
                "Error writing row to: " + path,
                e
            );
        }
    }




}
