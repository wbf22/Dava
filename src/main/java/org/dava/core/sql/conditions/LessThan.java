package org.dava.core.sql.conditions;

import java.math.BigDecimal;

import org.dava.core.common.TypeUtil;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.structure.Row;

public class LessThan extends NumericCondition {


    public LessThan( String column, BigDecimal value, boolean descending, Class<?> type ) {
        this.columnName = column;
        this.value = value;
        this.descending = descending;
        this.compareValues = (first, other) -> {
            if (first == null) 
                return 1;
            if (other == null)
                return -1;
            return first.compareTo(other);
        };
        this.filter = other -> {
            if (other == null)
                return true;

            return other.compareTo(value) < 0;
        };
        this.fileNameConverter = BaseOperationService::convertFileNameToBigDecimalLowerNull;
        this.columnType = type;
    }


    @Override
    public boolean filter(Row row) {
        String rowValue = row.getValue(columnName).toString();

        BigDecimal other = null;
        if (TypeUtil.isDate(columnType)) 
            other = Date.ofForDava(rowValue, columnType).getMillisecondsSinceTheEpoch();
        else 
            other = new BigDecimal(rowValue);

        return other.compareTo(value) < 0;
    }



}
