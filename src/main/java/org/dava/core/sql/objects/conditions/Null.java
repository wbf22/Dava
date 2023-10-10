package org.dava.core.sql.objects.conditions;

import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;

import java.util.List;

public class Null {

    private Object value;

    public Null(Object value) {
        this.value = value;
    }

    public static Null isNull(Object value) {
        return new Null(value);
    }

}
