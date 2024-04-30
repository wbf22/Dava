package org.dava.core.sql;


import java.util.List;
import java.util.Map;

import org.dava.core.sql.conditions.Condition;


public class Select {

    private List<As> asNamesOfFields;
    private From from;
    private List<Join> joins;
    private Condition where;
    private Map<String, String> aliasToTableName;



    public <T> List<T> retrieve() {

        return null;
    }


}
