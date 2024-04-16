package org.dava.core.sql.operators;

import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;
import org.dava.core.sql.objects.conditions.Condition;

import java.util.List;

public class And implements Condition, Operator{

    private Condition leftCondition;
    private Condition rightCondition;


    public And(Condition leftCondition, Condition rightCondition) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
    }

    @Override
    public boolean filter(Row row) {
        return leftCondition.filter(row) && rightCondition.filter(row);
    }

    /**
     * WHERE name='bob' AND (price=40.00 AND date='12/04/2024')
     */
    @Override
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Long limit, Long offset) {
        // find most restricting condition first
        Condition mostRestrictingCondition = findMostRestrictingCondition(table);
        parentFilters.add(this);

        return mostRestrictingCondition.retrieve(table, parentFilters, limit, offset);
    }

    private Condition findMostRestrictingCondition(Table<?> table) {
        Long leftCount = leftCondition.getCountEstimate(table);
        Long rightCount = rightCondition.getCountEstimate(table);
        Condition mostRestrictingCondition;
        if (leftCount != null && rightCount != null) {
            mostRestrictingCondition = (leftCount <= rightCount)? leftCondition : rightCondition;
        }
        else if (leftCount == null && rightCount == null){
            mostRestrictingCondition = leftCondition;
        }
        else {
            mostRestrictingCondition = (leftCount != null)? leftCondition : rightCondition;
        }

        return mostRestrictingCondition;
    }


    @Override
    public Long getCountEstimate(Table<?> table) {
        Long leftCount = leftCondition.getCountEstimate(table);
        Long rightCount = rightCondition.getCountEstimate(table);
        if (leftCount != null && rightCount != null) {
            return (leftCount <= rightCount)? leftCount : rightCount;
        }
        else {
            return (leftCount != null)? leftCount : rightCount;
        }
    }
}
