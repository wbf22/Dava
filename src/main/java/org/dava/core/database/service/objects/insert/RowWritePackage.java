package org.dava.core.database.service.objects.insert;

import org.dava.core.database.objects.database.structure.Route;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.service.objects.WritePackage;

public class RowWritePackage extends WritePackage {

    private Route route;
    private Row row;


    public RowWritePackage(Route route, Row row, byte[] dataAsBytes) {
        super(route.getOffsetInTable(), dataAsBytes);
        this.route = route;
        this.row = row;
    }


    /*
        getter setter
     */

    public Route getRoute() {
        return route;
    }

    public Row getRow() {
        return row;
    }



}
