package org.dava.core.sql.objects.conditions;

import java.math.BigDecimal;

import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.service.BaseOperationService;

public class GreaterThan extends NumericCondition {



    public GreaterThan( String column, BigDecimal value, boolean descending ) {
        this.columnName = column;
        this.value = value;
        this.descending = descending;
        this.compareValues = Comparable::compareTo;
        this.filter = other -> {
            if (other == null)
                return true;

            return other.compareTo(value) > 0;
        };
        this.fileNameConverter = (string) -> BaseOperationService.convertFileNameToBigDecimalUpperNull(string);
    }


    @Override
    public boolean filter(Row row) {
        String rowValue = row.getValue(columnName).toString();

        BigDecimal other = new BigDecimal(rowValue);

        return other.compareTo(value) > 0;
    }


}
