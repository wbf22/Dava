package org.dava.core.sql.objects.conditions;

import org.dava.core.database.objects.dates.Date;




public class After <T extends Date<?>> extends GreaterThan {


    public After(String column, T date, boolean descending) {
        super(column, date.getMillisecondsSinceTheEpoch(), descending, date.getClass());
    }


}
