package org.dava.core.database.objects.database.structure;

public class Column<T> {
    private String name;
    private Class<T> type;


    public Column(String name, Class<T> type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Class<T> getType() {
        return type;
    }
}
