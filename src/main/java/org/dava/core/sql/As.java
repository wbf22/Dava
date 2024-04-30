package org.dava.core.sql;

public class As {

    private String tableAlias;
    private String name;
    private String newName;

    public static As parse (String statement) {
        As newAs = new As();

        String[] split = statement.split(" as ");
        if (split.length > 1)
            newAs.newName = split[1];

        String[] aliasSplit = split[0].split(".");
        if (aliasSplit.length > 1) {
            newAs.tableAlias = aliasSplit[0];
            newAs.name = aliasSplit[1];
        }
        else {
            newAs.name = aliasSplit[0];
        }
        

        return newAs;
    }
}
