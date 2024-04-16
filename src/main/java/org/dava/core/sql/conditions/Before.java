package org.dava.core.sql.conditions;

import org.dava.core.database.objects.dates.Date;

public class Before<T extends Date<?>> extends LessThan {


    public Before(String column, T date, boolean descending) {
        super(column, date.getMillisecondsSinceTheEpoch(), descending, date.getClass());
    }

}
