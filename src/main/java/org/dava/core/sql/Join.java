package org.dava.core.sql;

import java.util.Map;

import org.dava.core.sql.conditions.Condition;
import org.dava.core.sql.conditions.Equals;



public class Join {

    private String tableName;
    private Map<String, String> aliasToField;
    

    public static Join parse(String joinString) {

        String[] onSplit = joinString.split("on");
        Join join = new Join();
        join.tableName = onSplit[0];

        String equals = onSplit[1];
        String[] aliasFields = equals.split("=");
        for (String aliasField : aliasFields) {
            String[] split = aliasField.split(".");
            join.aliasToField.put(split[0], split[1]);
        }

        return join;
    }
}
