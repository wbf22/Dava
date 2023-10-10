package org.dava.core.sql.objects;

import org.dava.core.sql.objects.conditions.Condition;

import java.util.List;

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
