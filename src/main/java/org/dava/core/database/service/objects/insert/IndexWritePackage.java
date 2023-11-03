package org.dava.core.database.service.objects.insert;

import org.dava.core.database.objects.database.structure.Column;
import org.dava.core.database.objects.database.structure.IndexRoute;
import org.dava.core.database.service.objects.Empty;
import org.dava.core.database.service.objects.WritePackage;

import java.io.Serializable;

public class IndexWritePackage extends WritePackage {

    private IndexRoute route;
    private Empty locationInIndex;
    private Column<?> column;
    private Object value;
    private String folderPath;


    public IndexWritePackage(IndexRoute route, Empty locationInIndex, Column<?> column, Object value, String folderPath) {
        super(
            (locationInIndex == null)? null :locationInIndex.getRoute().getOffsetInTable(),
            route.getRouteAsBytes()
        );
        this.route = route;
        this.locationInIndex = locationInIndex;
        this.column = column;
        this.value = value;
        this.folderPath = folderPath;
    }


    /*
        getter setter
     */
    public IndexRoute getRoute() {
        return route;
    }

    public Column<?> getColumn() {
        return column;
    }

    public String getColumnName() {
        return column.getName();
    }

    public Class<?> getColumnType() {
        return column.getType();
    }

    public Object getValue() {
        return value;
    }

    public String getFolderPath() {
        return folderPath;
    }
}
