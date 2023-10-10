package org.dava.core.database.objects.database.structure;


import org.dava.core.database.service.BaseOperationService;

import java.util.List;
import java.util.Random;


public class Table<T> {

    private List<Column<?>> columns;


    /**
     * Row lengths are added whenever a row is too long for the current length.
     * After a new row length is added, the table will wait 10 more rows before
     * determining a new row length based on the average length of the last ten.
     *
     * The row of the RowLength is the first row of that length
     */
    private List<RowLength> rowLengths;

    /**
     * Used in calculating offset
     */
    private long numRows;

    private long newRowSize;

    private String tableName;

    private String directory;
    private List<String> partitions;



    public String getTablePath(String partitionName) {
        return directory + "/" + partitionName;
    }

    /**
     * Get an empty row in the table.
     * @return next empty row in the table, or null there are no empty rows.
     */
    public Long popEmptyRow() {
        String partition = partitions.get(new Random().nextInt(0, partitions.size()));
        return BaseOperationService.popLong(getTablePath(partition), 8);
    }

    /*
    Getter Setters
     */

    public List<Column<?>> getColumns() {
        return columns;
    }

    public List<RowLength> getRowLengths() {
        return rowLengths;
    }

    public long getNumRows() {
        return numRows;
    }

    public long getNewRowSize() {
        return newRowSize;
    }

    public String getTableName() {
        return tableName;
    }



    public String getDirectory() {
        return directory;
    }

    public List<String> getPartitions() {
        return partitions;
    }
}
