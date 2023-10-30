package org.dava.core.database.service.objects;

import org.dava.core.database.objects.database.structure.IndexRoute;
import org.dava.core.database.objects.database.structure.Row;

import java.io.Serializable;
import java.util.Arrays;

public class RowWritePackage {

    private IndexRoute route;
    private Row row;
    private byte[] dataAsBytes;


    public RowWritePackage(IndexRoute route, Row row, byte[] dataAsBytes) {
        this.route = route;
        this.row = row;
        this.dataAsBytes = dataAsBytes;
    }


    /*
        getter setter
     */

    public IndexRoute getRoute() {
        return route;
    }

    public Row getRow() {
        return row;
    }

    public byte[] getDataAsBytes() {
        return dataAsBytes;
    }


    @Override
    public String toString() {
        return "RowWritePackage{" +
            "route=" + route +
            ", row=" + row +
            ", dataAsBytes=" + Arrays.toString(dataAsBytes) +
            '}';
    }
}
