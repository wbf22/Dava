package org.dava.core.database.objects.database.structure;

public class Column<T> {
    private String name;
    private Class<T> type;
    private boolean isUnique;


    public Column(String name, Class<T> type, boolean isUnique) {
        this.name = name;
        this.type = type;
        this.isUnique = isUnique;
    }

    public String getName() {
        return name;
    }

    public Class<T> getType() {
        return type;
    }

    public boolean isUnique() {
        return isUnique;
    }
}
