package org.dava.core.sql.operators;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;
import org.dava.core.sql.conditions.Condition;

import java.util.List;

public class Or implements Condition, Operator {

    private Condition leftCondition;
    private Condition rightCondition;


    public Or(Condition leftCondition, Condition rightCondition) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
    }

    @Override
    public boolean filter(Row row) {
        return leftCondition.filter(row) || rightCondition.filter(row);
    }

    /**
     * WHERE name='bob' AND (price=40.00 OR date='12/04/2024')
     */
    @Override
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Integer limit, Long offset) {
        List<Row> leftRows = leftCondition.retrieve(table, parentFilters, limit, offset);
        List<Row> rightRows = rightCondition.retrieve(table, parentFilters, limit, offset);
        leftRows.addAll(rightRows);
        return leftRows;
    }

    @Override
    public Long getCountEstimate(Table<?> table) {
        Long leftCount = leftCondition.getCountEstimate(table);
        Long rightCount = rightCondition.getCountEstimate(table);

        if (leftCount != null && rightCount != null)
            return leftCount + rightCount;
        return null;
    }
}
