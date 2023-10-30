package org.dava.core.database.service.objects;

import org.dava.core.database.objects.database.structure.IndexRoute;

public class Empty {

    private IndexRoute route;
    private Long startByte;


    /**
     * Object representing an empty row in a table or index.
     */
    public Empty(IndexRoute route, Long startByte) {
        this.route = route;
        this.startByte = startByte;
    }

    public IndexRoute getRoute() {
        return route;
    }

    public Long getStartByte() {
        return startByte;
    }
}

