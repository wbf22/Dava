package org.dava.core.database.service.objects.insert;

import org.dava.core.database.objects.database.structure.Column;
import org.dava.core.database.objects.database.structure.Route;
import org.dava.core.database.service.objects.WritePackage;

public class IndexWritePackage extends WritePackage {

    private Route route;
    private Column<?> column;
    private Object value;
    private String folderPath;


    public IndexWritePackage(Route route, Column<?> column, Object value, String folderPath) {
        super(
            null,
            route.getRouteAsBytes()
        );
        this.route = route;
        this.column = column;
        this.value = value;
        this.folderPath = folderPath;
    }


    /*
        getter setter
     */
    public Route getRoute() {
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

    public void setFolderPath(String folderPath) {
        this.folderPath = folderPath;
    }
}
