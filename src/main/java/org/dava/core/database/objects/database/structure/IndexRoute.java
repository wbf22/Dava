package org.dava.core.database.objects.database.structure;

import org.dava.common.ArrayUtil;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;

public class IndexRoute {

    private String partition;

    private Long offsetInTable;

    private Integer lengthInTable;


    public IndexRoute(String partition, Long offset, Integer length) {
        this.partition = partition;
        this.offsetInTable = offset;
        this.lengthInTable = length;
    }

    /**
     * Indices are made up of 10 bytes:
     *    - [6 bytes offset][4 bytes length]
     */
    public static List<IndexRoute> parseBytes(byte[] bytes, String partition) {
        return IntStream.range(0, bytes.length/10)
            .mapToObj(i -> {
                int index = i * 10;
                return parseRoute(
                    ArrayUtil.subRange(bytes, index, index+10),
                    partition
                );
            })
            .toList();

    }

    public static IndexRoute parseRoute(byte[] bytes, String partition) {
        byte[] offset = ArrayUtil.subRange(bytes, 0, 6);
        byte[] length = ArrayUtil.subRange(bytes, 6, 10);

        return new IndexRoute(
            partition,
            TypeToByteUtil.byteArrayToLong(
                ArrayUtil.appendArray(new byte[2], offset)
            ),
            TypeToByteUtil.byteArrayToInt(
                length
            )
        );
    }


    public byte[] getRouteAsBytes() {
        byte[] offset = TypeToByteUtil.longToByteArray(offsetInTable);
        byte[] length = TypeToByteUtil.longToByteArray(lengthInTable);

        return ArrayUtil.appendArray(
            ArrayUtil.subRange(offset, offset.length-6, offset.length),
            ArrayUtil.subRange(length, length.length-4, length.length)
        );
    }




    /*
        Getter setter
     */
    public String getPartition() {
        return partition;
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

    public void setOffsetInTable(Long offsetInTable) {
        this.offsetInTable = offsetInTable;
    }

    public void setLengthInTable(Integer lengthInTable) {
        this.lengthInTable = lengthInTable;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexRoute route = (IndexRoute) o;
        return Objects.equals(partition, route.partition) && Objects.equals(offsetInTable, route.offsetInTable) && Objects.equals(lengthInTable, route.lengthInTable);
    }
    @Override
    public String toString() {
        return "IndexRoute{" +
            "partition='" + partition + '\'' +
            ", offsetInTable=" + offsetInTable +
            ", lengthInTable=" + lengthInTable +
            '}';
    }
}
