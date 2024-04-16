package org.dava.core.database.service.operations;

public class WritePackage {

    private Long offsetInTable;
    private byte[] data;


    public WritePackage(Long offsetInTable, byte[] data) {
        this.offsetInTable = offsetInTable;
        this.data = data;
    }


    public Long getOffsetInTable() {
        return offsetInTable;
    }

    public byte[] getData() {
        return data;
    }
}
