package org.dava.core.database.objects.database.structure;

public class RowLength {
    private long row;
    private int length;

    /**
     * @param row row at which this new length starts
     * @param length length of the row
     */
    public RowLength(long row, int length) {
        this.row = row;
        this.length = length;
    }


    public long getRow() {
        return row;
    }

    public int getLength() {
        return length;
    }
}
