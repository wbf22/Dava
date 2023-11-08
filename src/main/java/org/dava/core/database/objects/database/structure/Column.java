package org.dava.core.database.objects.database.structure;

public class Column<T> {
    private final String name;
    private final Class<T> type;
    private final boolean isIndexed;
    private final boolean isUnique;


    public Column(String name, Class<T> type, boolean isIndexed, boolean isUnique) {
        this.name = name;
        this.type = type;
        this.isIndexed = isIndexed;
        this.isUnique = isUnique;
    }

    public String getName() {
        return name;
    }

    public Class<T> getType() {
        return type;
    }

    public boolean isIndexed() {
        return isIndexed;
    }

    public boolean isUnique() {
        return isUnique;
    }


    @Override
    public String toString() {
        return "Column{" +
            "name='" + name + '\'' +
            ", type=" + type +
            ", isIndexed=" + isIndexed +
            ", isUnique=" + isUnique +
            '}';
    }
}
