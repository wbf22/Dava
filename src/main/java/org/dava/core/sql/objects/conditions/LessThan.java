package org.dava.core.sql.objects.conditions;

import java.math.BigDecimal;

import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.service.BaseOperationService;

public class LessThan extends NumericCondition {


    public LessThan( String column, BigDecimal value, boolean descending ) {
        this.columnName = column;
        this.value = value;
        this.descending = descending;
        this.compareValues = (first, other) -> other.compareTo(first);
        this.filter = other -> {
            if (other == null)
                return true;

            return other.compareTo(value) < 0;
        };
        this.fileNameConverter = (string) -> BaseOperationService.convertFileNameToBigDecimalLowerNull(string);
    }


    @Override
    public boolean filter(Row row) {
        String rowValue = row.getValue(columnName).toString();

        BigDecimal other = new BigDecimal(rowValue);

        return other.compareTo(value) < 0;
    }



}
