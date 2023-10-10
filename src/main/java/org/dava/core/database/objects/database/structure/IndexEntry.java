package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.IOException;
import java.util.List;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

public class IndexEntry {

    private String partition;

    private Long rowInTable;

    private Long offsetInTable;

    private Integer lengthInTable;


    public IndexEntry(String partition, Long row, Long offset, Integer length) {
        this.partition = partition;
        this.rowInTable = row;
        this.offsetInTable = offset;
        this.lengthInTable = length;
    }


    public static IndexEntry of(String partition, long indexRow, Table<?> table) {
        return (calculateStartOfRow(
            parse(partition, indexRow),
            table
        ));
    }

    /**
     * Indices are stored in csv files as byte values (every 8 bytes is a long)
     * So every 8 bytes is the next index
     */
    public static IndexEntry parse(String partition, long indexRow) {
        return new IndexEntry(partition, indexRow, null, null);
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
     * @param entry to find offset for
     * @param table table which contains this row
     * @return IndexRoute which contains an offset in chars from the beginning of the file
     * and the length of the row
     */
    public static IndexEntry calculateStartOfRow(IndexEntry entry, Table<?> table) {
        /*
            0: 56
            1: 125
            11: 120
            21: 115
         */

        long startOfRow = 0L;
        long entryRow = entry.getRowInTable();
        List<RowLength> rowLengths = table.getRowLengths();
        for (int i = 0; i < rowLengths.size(); i++) {
            RowLength rowL = rowLengths.get(i);
            long nextRow = (i+1 < rowLengths.size())? rowLengths.get(i+1).getRow() : table.getNumRows();

            long mult = (entryRow > nextRow)? nextRow - rowL.getRow() : entryRow - rowL.getRow();

            startOfRow += rowL.getLength() * mult;

            if (nextRow > entryRow) {
                entry.setOffsetInTable(startOfRow);
                entry.setLengthInTable(rowL.getLength());
                return entry;
            }
        }

        throw new DavaException(BASE_IO_ERROR, "CRITICAL: Tried to find row that was greater " +
            "than the last row in the table of size: " + table.getNumRows() + " row: " + entry.getRowInTable(), null);

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
