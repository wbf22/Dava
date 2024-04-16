package org.dava.core.sql.conditions;

import java.util.List;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;

public class Null {

    private Object value;

    public Null(Object value) {
        this.value = value;
    }

    public static Null isNull(Object value) {
        return new Null(value);
    }

}
