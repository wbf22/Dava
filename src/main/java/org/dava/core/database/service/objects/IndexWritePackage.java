package org.dava.core.database.service.objects;

import org.dava.core.database.objects.database.structure.IndexRoute;

import java.io.Serializable;

public class IndexWritePackage {

    private IndexRoute route;
    private Empty locationInIndex;
    private Class<?> columnType;
    private Object value;
    private String folderPath;


    public IndexWritePackage(IndexRoute route, Empty locationInIndex, Class<?> columnType, Object value, String folderPath) {
        this.route = route;
        this.locationInIndex = locationInIndex;
        this.columnType = columnType;
        this.value = value;
        this.folderPath = folderPath;
    }


    /*
        getter setter
     */
    public IndexRoute getRoute() {
        return route;
    }

    public Empty getLocationInIndex() {
        return locationInIndex;
    }

    public Class<?> getColumnType() {
        return columnType;
    }

    public Object getValue() {
        return value;
    }

    public String getFolderPath() {
        return folderPath;
    }
}
