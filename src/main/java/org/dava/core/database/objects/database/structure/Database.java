package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.exception.DavaException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.dava.core.database.objects.exception.ExceptionType.MISSING_TABLE;
import static org.dava.core.database.objects.exception.ExceptionType.NOT_A_TABLE;

public class Database {

    private String rootDirectory;

    private Map<String, Table<?>> tables;


    public Database(String rootDirectory, List<Class<?>> tableClasses, List<Mode> tableModes, long seed) {
        this.rootDirectory = rootDirectory;
        this.tables = IntStream.range(0, tableClasses.size())
            .mapToObj(i -> new Table<>(tableClasses.get(i), rootDirectory, tableModes.get(i), seed))
            .collect(Collectors.toMap(Table::getTableName, obj -> obj));

    }



    @SuppressWarnings("unchecked")
    public <T> Table<T> getTableForRow(T row) {
        org.dava.external.annotations.Table annotation = row.getClass().getAnnotation(
            org.dava.external.annotations.Table.class);

        try {
            String tableName = (annotation.name().isEmpty())? row.getClass().getName() : annotation.name();
            Table<?> table = getTableByName(tableName);

            // cast the table to the type for the row
            Type genericSuperclass = table.getClass().getGenericSuperclass();
            if (genericSuperclass instanceof ParameterizedType parameterizedType) {
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                if (actualTypeArguments.length > 0) {
                    Type firstTypeArgument = actualTypeArguments[0];
                    if (firstTypeArgument instanceof Class<?> paramTypeFromClass
                        && paramTypeFromClass.equals(row.getClass())) {
                        return (Table<T>) table;
                    }
                }
            }
            throw new DavaException(MISSING_TABLE, "Couldn't get table", null);
        }
        catch (Exception e) {
            throw new DavaException(
                NOT_A_TABLE,
                "Trying to get table: " + row.getClass().getName() + ", but wasn't a table." +
                    "Add @Table annotation to your object",
                e
            );
        }
    }

    public Table<?> getTableByName(String name) {
        if (tables.containsKey(name)) {
            return tables.get(name);
        }

        throw new DavaException(
            NOT_A_TABLE,
            "Could not find table with name: " + name,
            null
        );
    }


    /*
        Getter Setter
     */
    public String getRootDirectory() {
        return rootDirectory;
    }
}
