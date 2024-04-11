package org.dava.core.sql.objects.conditions;

import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.service.BaseOperationService;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class NumericCondition implements Condition {
    protected String columnName;
    protected BigDecimal value;
    protected Comparator<BigDecimal> compareValues;
    protected Predicate<BigDecimal> filter;
    protected Function<String, BigDecimal> fileNameConverter;
    protected boolean descending;


    @Override
    public List<Row> retrieve(Table<?> table, List<Condition> parentFilters, Long limit, Long offset) {
        return BaseOperationService.getRowsComparingNumeric(
            table,
            columnName,
            compareValues,
            filter,
            fileNameConverter,
            offset,
            offset + limit,
            descending,
            false
        );
    }



    @Override
    public Long getCountEstimate(Table<?> table) {
        /* 
            Punting on this one. Numeric greater than or less than operations will tend to be the
            longer operations in a combined statement, so returning max value will ensure other operations
            will be performed first
        */ 
        return Long.MAX_VALUE;
    }
}