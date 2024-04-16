package org.dava.core.sql;


import java.util.List;

import org.dava.core.sql.service.conditions.Condition;

public class Select {

    private List<As> asNamesOfFields;
    private String from;
    private Condition where;


    public Select(List<As> asNamesOfFields, String from, Condition where) {
        this.asNamesOfFields = asNamesOfFields;
        this.from = from;
        this.where = where;
    }


}
