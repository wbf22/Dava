package org.dava.core.database.service.structure;

import org.dava.core.common.ArrayUtil;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class Empty {

    private int index;
    private Route route;

    public Empty(int index, Route route) {
        this.index = index;
        this.route = route;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Route getRoute() {
        return route;
    }

    public void setRoute(Route route) {
        this.route = route;
    }
}
