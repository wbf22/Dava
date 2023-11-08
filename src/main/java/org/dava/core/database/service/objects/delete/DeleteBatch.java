package org.dava.core.database.service.objects.delete;

import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.objects.database.structure.Table;

import java.util.List;
import java.util.Map;

public class DeleteBatch {
    private List<Row> rows;
    private long oldTableSize;
    private long oldEmptiesSize;
    private Map<String, IndexDelete> indexPathToIndicesToDelete;
    private Map<String, CountChange> numericCountFileChanges;



    public String getRollbackString(Table<?> table, String partition) {
        StringBuilder builder = new StringBuilder();
        builder.append("Delete Batch:\n");

        // all the rows that are being deleted
        rows.forEach( row -> {
            builder.append("R:");
            if (row.getLocationInTable() != null) {
                builder.append(row.getLocationInTable().getOffsetInTable())
                    .append(",")
                    .append(row.getLocationInTable().getLengthInTable())
                    .append(";");
            }
            builder.append(Row.serialize(table, row.getColumnsToValues()))
                .append("\n");
        });

        // table sizes
        builder.append("TS:")
            .append(oldTableSize)
            .append("\n");

        // emtpies sizes
        builder.append("ES:")
            .append(oldEmptiesSize)
            .append("\n");

        // indices to delete
        indexPathToIndicesToDelete.forEach( (indexPath, indexDelete) -> {
            builder.append("I:").append(indexPath).append(";");
            indexDelete.getRoutesToDelete().forEach(offset -> {
                builder.append("R:")
                    .append(offset)
                    .append(";");
            });
            // use the route above to remove lines from table. Then search for route in index and whitespace the route there
            builder.append("\n");
        });

        // old count file values
        numericCountFileChanges.forEach( (countFile, countChange) -> {
            builder.append("C:")
                .append(countFile)
                .append(",")
                .append(countChange.getOldCount())
                .append(";");
            // use the route above to remove lines from table. Then search for route in index and whitespace the route there
            builder.append("\n");
        });

        return builder.toString();
    }


    /*
        getter setter
     */

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public long getOldTableSize() {
        return oldTableSize;
    }

    public void setOldTableSize(long oldTableSize) {
        this.oldTableSize = oldTableSize;
    }

    public long getOldEmptiesSize() {
        return oldEmptiesSize;
    }

    public void setOldEmptiesSize(long oldEmptiesSize) {
        this.oldEmptiesSize = oldEmptiesSize;
    }

    public Map<String, IndexDelete> getIndexPathToIndicesToDelete() {
        return indexPathToIndicesToDelete;
    }

    public void setIndexPathToIndicesToDelete(Map<String, IndexDelete> indexPathToIndicesToDelete) {
        this.indexPathToIndicesToDelete = indexPathToIndicesToDelete;
    }

    public Map<String, CountChange> getNumericCountFileChanges() {
        return numericCountFileChanges;
    }

    public void setNumericCountFileChanges(Map<String, CountChange> numericCountFileChanges) {
        this.numericCountFileChanges = numericCountFileChanges;
    }
}
