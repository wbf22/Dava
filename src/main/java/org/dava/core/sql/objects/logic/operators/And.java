package org.dava.core.sql.objects.logic.operators;

import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.sql.objects.conditions.Condition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    public List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset) {
        // find most restricting condition first
        Condition mostRestrictingCondition = findMostRestrictingCondition(database, from);
        parentFilters.add(this);

        return mostRestrictingCondition.retrieve(database, parentFilters, from, limit, offset);
    }

    private Condition findMostRestrictingCondition(Database database, String from) {
        Long leftCount = leftCondition.getCount(database, from);
        Long rightCount = rightCondition.getCount(database, from);
        Condition mostRestrictingCondition = null;
        if (leftCount != null && rightCount != null) {
            mostRestrictingCondition = (leftCount <= rightCount)? leftCondition : rightCondition;
        }
        else if (leftCount == null && rightCount == null){
            mostRestrictingCondition = null;
        }
        else {
            mostRestrictingCondition = (leftCount != null)? leftCondition : rightCondition;
        }

        return mostRestrictingCondition;
    }


    @Override
    public Long getCount(Database database, String from) {
        Long leftCount = leftCondition.getCount(database, from);
        Long rightCount = rightCondition.getCount(database, from);
        if (leftCount != null && rightCount != null) {
            return (leftCount <= rightCount)? leftCount : rightCount;
        }
        else {
            return (leftCount != null)? leftCount : rightCount;
        }
    }
}
