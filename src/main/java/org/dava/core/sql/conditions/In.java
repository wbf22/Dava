package org.dava.core.sql.conditions;

import java.util.List;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;

public class In<T> implements Condition {

    private List<T> collection;
    private T value;

    public In(List<T> collection, T value) {
        this.collection = collection;
        this.value = value;
    }

    public static <S> In<S> in(List<S> collection, S value) {
        return new In<>(collection, value);
    }

    @Override
    public boolean filter(Row row) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'filter'");
    }

    @Override
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Long limit, Long offset) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'retrieve'");
    }

    @Override
    public Long getCountEstimate(Table<?> table) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCountEstimate'");
    }


}
