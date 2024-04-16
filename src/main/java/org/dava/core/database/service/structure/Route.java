package org.dava.core.database.service.structure;

import org.dava.core.common.ArrayUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class Route {

    private String partition;

    private Long offsetInTable;

    private Integer lengthInTable;


    public Route(String partition, Long offset, Integer length) {
        this.partition = partition;
        this.offsetInTable = offset;
        this.lengthInTable = length;
    }

    /**
     * Indices are made up of 10 bytes:
     *    - [6 bytes offset][4 bytes length]
     */
    public static List<Route> parseBytes(byte[] bytes, String partition) {
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

    public static Route parseRoute(byte[] bytes, String partition) {
        byte[] offset = ArrayUtil.subRange(bytes, 0, 6);
        byte[] length = ArrayUtil.subRange(bytes, 6, 10);

        return new Route(
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


    // these are important for ensuring rollbacks work as well as other functionality
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Route route = (Route) o;
        return Objects.equals(partition, route.partition) && Objects.equals(offsetInTable, route.offsetInTable) && Objects.equals(lengthInTable, route.lengthInTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, offsetInTable, lengthInTable);
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
