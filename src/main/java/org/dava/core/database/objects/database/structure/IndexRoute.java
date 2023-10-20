package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.exception.DavaException;

import java.util.List;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

public class IndexRoute {

    private String partition;

    private Long rowInTable;

    private Long offsetInTable;

    private Integer lengthInTable;


    public IndexRoute(String partition, Long row, Long offset, Integer length) {
        this.partition = partition;
        this.rowInTable = row;
        this.offsetInTable = offset;
        this.lengthInTable = length;
    }


    /**
     * Indices are stored in csv files as byte values (every 8 bytes is a long)
     * So every 8 bytes is the next index
     */
    public static IndexRoute of(String partition, long indexRow, Table<?> table) {
        return calculateStartOfRow(
            partition,
            indexRow,
            table
        );
    }


    /**
     * Calculates the index of the row in the file. On insert if a row
     * has a length greater than the current row size then a value is saved
     * in table indicating rows afterwards have a different length.
     *
     * A table could have rows as follows:
     * 0: 56
     * 1: 125
     * 11: 120
     * 21: 115
     *
     * Here row 0 would have a length of 56, rows 1-10 would have a length of 125,
     * rows 11-20 would have a length of 120, etc...
     *
     * This method calculates the char offset from the beginning of a file for the row
     * so that the row can be read with random access.
     */
    public static IndexRoute calculateStartOfRow(String partition, Long indexRow, Table<?> table) {
        /*
            0: 56
            1: 125
            11: 120
            21: 115
         */

        long startOfRow = 0L;
        List<RowLength> rowLengths = table.getRowLengths(partition);
        for (int i = 0; i < rowLengths.size(); i++) {
            RowLength rowL = rowLengths.get(i);
            RowLength nextRowL = (i+1 < rowLengths.size())? rowLengths.get(i+1) : rowLengths.get(rowLengths.size()-1);
            long nextRow = (i+1 < rowLengths.size())? nextRowL.getRow() : table.getSize(partition);

            long mult = (indexRow > nextRow)? nextRow - rowL.getRow() : indexRow - rowL.getRow();

            startOfRow += rowL.getLength() * mult;

            if (nextRow > indexRow) {
                return new IndexRoute(partition, indexRow, startOfRow, rowL.getLength());
            }
            else if (nextRow == indexRow) {
                return new IndexRoute(partition, indexRow, startOfRow, nextRowL.getLength());
            }
        }

        throw new DavaException(BASE_IO_ERROR, "CRITICAL: Tried to find row that was greater " +
            "than the last row in the table of size: " + table.getSize(partition) + " row: " + indexRow, null);

    }



    public String getPartition() {
        return partition;
    }

    public Long getRowInTable() {
        return rowInTable;
    }

    public Long getOffsetInTable() {
        return offsetInTable;
    }

    public Integer getLengthInTable() {
        return lengthInTable;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public void setRowInTable(Long rowInTable) {
        this.rowInTable = rowInTable;
    }

    public void setOffsetInTable(Long offsetInTable) {
        this.offsetInTable = offsetInTable;
    }

    public void setLengthInTable(Integer lengthInTable) {
        this.lengthInTable = lengthInTable;
    }
}
