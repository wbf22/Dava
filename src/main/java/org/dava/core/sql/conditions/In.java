package org.dava.core.sql.conditions;

import java.util.List;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;

public class In<T> {

    private List<T> collection;
    private T value;

    public In(List<T> collection, T value) {
        this.collection = collection;
        this.value = value;
    }

    public static <S> In<S> in(List<S> collection, S value) {
        return new In<>(collection, value);
    }


}
