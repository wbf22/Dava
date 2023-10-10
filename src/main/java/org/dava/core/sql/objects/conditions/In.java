package org.dava.core.sql.objects.conditions;

import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;

import java.util.List;

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
