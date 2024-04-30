package org.dava.core.sql;

public class From {
    private String tableName;
    private String alias;

    
    public static From parse(String fromString) {
        String[] split = fromString.split(" ");
       
        From from = new From();
        from.tableName = split[1];
        from.alias = split[2];

        return from;
    }


    public String getTableName() {
        return tableName;
    }


    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public String getAlias() {
        return alias;
    }


    public void setAlias(String alias) {
        this.alias = alias;
    }

    

}
