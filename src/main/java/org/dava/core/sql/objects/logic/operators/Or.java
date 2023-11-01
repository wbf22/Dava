package org.dava.core.sql.objects.logic.operators;

import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.sql.objects.conditions.Condition;

import java.util.List;

public class Or implements Condition, Operator {

    private Condition leftCondition;
    private Condition rightCondition;


    @Override
    public boolean filter(Row row) {
        return leftCondition.filter(row) || rightCondition.filter(row);
    }

    /**
     * WHERE name='bob' AND (price=40.00 OR date='12/04/2024')
     */
    @Override
    public List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset) {
        List<Row> leftRows = leftCondition.retrieve(database, parentFilters, from, limit, offset);
        List<Row> rightRows = rightCondition.retrieve(database, parentFilters, from, limit, offset);
        leftRows.addAll(rightRows);
        return leftRows;
    }

    @Override
    public Long getCountEstimate(Database database, String from) {
        Long leftCount = leftCondition.getCountEstimate(database, from);
        Long rightCount = rightCondition.getCountEstimate(database, from);

        if (leftCount != null && rightCount != null)
            return leftCount + rightCount;
        return null;
    }
}
