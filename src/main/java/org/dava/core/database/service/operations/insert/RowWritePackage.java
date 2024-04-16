package org.dava.core.database.service.operations.insert;

import org.dava.core.database.service.operations.WritePackage;
import org.dava.core.database.service.structure.Route;
import org.dava.core.database.service.structure.Row;

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
