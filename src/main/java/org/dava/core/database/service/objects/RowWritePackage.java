package org.dava.core.database.service.objects;

import org.dava.core.database.objects.database.structure.IndexRoute;
import org.dava.core.database.objects.database.structure.Row;

import java.io.Serializable;
import java.util.Arrays;

public class RowWritePackage extends WritePackage {

    private IndexRoute route;
    private Row row;


    public RowWritePackage(IndexRoute route, Row row, byte[] dataAsBytes) {
        super(route.getOffsetInTable(), dataAsBytes);
        this.route = route;
        this.row = row;
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



}
