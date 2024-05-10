package org.dava.core.database.service.operations;

import java.util.List;

import org.dava.core.database.service.operations.common.Batch;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;

public class Update {

    private Delete delete;
    private Insert insert;


    public Update(Database database, Table<?> table) {
        delete = new Delete(database, table);
        insert = new Insert(database, table, table.getRandomPartition());
    }
    

    public Batch addToBatch(List<Row> oldRows, List<Row> newRows,  boolean replaceRollbackFile, Batch existingBatch) {
        existingBatch = delete.addToBatch(oldRows, replaceRollbackFile, existingBatch);
        existingBatch = insert.addToBatch(newRows, replaceRollbackFile, existingBatch);

        return existingBatch;
    }
}
