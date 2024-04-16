package org.dava.core.database.service.structure;

import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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


    public static byte[] serializeList(List<RowLength> lengths) {
        byte[] bytes = new byte[lengths.size()*12];
        for (int i = 0; i < lengths.size(); i++) {
            byte[] rowBytes = lengths.get(i).serialize();
            System.arraycopy(rowBytes, 0, bytes, i*12, 12);
        }
        return bytes;
    }

    public byte[] serialize() {
        byte[] bytes = new byte[12];
        byte[] rowBytes = TypeToByteUtil.longToByteArray(row);
        byte[] lengthBytes = TypeToByteUtil.intToByteArray(length);
        System.arraycopy(rowBytes, 0, bytes, 0, 8);
        System.arraycopy(lengthBytes, 0, bytes, 8, 4);
        return bytes;
    }

    public static RowLength deserialize(byte[] bytes) {
        long row = TypeToByteUtil.byteArrayToLong(
            Arrays.copyOfRange(bytes, 0, 8)
        );
        int length = TypeToByteUtil.byteArrayToInt(
            Arrays.copyOfRange(bytes, 8, 12)
        );
        return new RowLength(row, length);
    }

    public static List<RowLength> deserializeList(byte[] bytes) {
        List<RowLength> lengths = new ArrayList<>();
        for (int i = 0; i < bytes.length; i+=12) {
            lengths.add(
                RowLength.deserialize(Arrays.copyOfRange(bytes, i, i+12))
            );
        }
        return lengths;
    }




    public long getRow() {
        return row;
    }

    public int getLength() {
        return length;
    }
}
