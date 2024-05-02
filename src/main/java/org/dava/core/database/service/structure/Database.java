package org.dava.core.database.service.structure;

import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.fileaccess.FileUtil;

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


    public Database(String rootDirectory, List<Class<?>> tableClasses, List<Mode> tableModes) {
        
        this.rootDirectory = rootDirectory;
        this.tables = IntStream.range(0, tableClasses.size())
            .mapToObj(i -> new Table<>(tableClasses.get(i), rootDirectory, tableModes.get(i), 0L))
            .collect(Collectors.toMap(Table::getTableName, obj -> obj));
    }

    public Database(String rootDirectory, List<Class<?>> tableClasses, List<Mode> tableModes, long seed) {
        
        this.rootDirectory = rootDirectory;
        this.tables = IntStream.range(0, tableClasses.size())
            .mapToObj(i -> new Table<>(tableClasses.get(i), rootDirectory, tableModes.get(i), seed))
            .collect(Collectors.toMap(Table::getTableName, obj -> obj));

    }





    @SuppressWarnings("unchecked")
    public <T> Table<T> getTableForRow(T row) {
        org.dava.api.annotations.Table annotation = row.getClass().getAnnotation(
            org.dava.api.annotations.Table.class);

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



    public static class Builder {
        private String builderRootDirectory;
        private List<Class<?>> tableClasses;
        private List<Mode> tableModes;
        private long randomSeed;

        /**
         * This builder is used to set up Dava database. This builder
         * uses any existing database and tables in the same path, or
         * if they don't exist, it creates new ones. A good way to use
         * this is by creating a global variable holding your database
         * object every time your app starts up.
         * @param rootDirectory this value specifies the folder where
         *                      your database should be created. Under
         *                      this each table will have its own folder.
         *                      NEVER modify table folders UNLESS the table
         *                      is run in LIGHT mode.
         */
        public Builder(String rootDirectory) {
            this.builderRootDirectory = rootDirectory;
            this.randomSeed = 0L;
        }

        /**
         * This builder is used to set up Dava database. This builder
         * uses any existing database and tables in the same path, or
         * if they don't exist, it creates new ones. A good way to use
         * this is by creating a global variable holding your database
         * object every time your app starts up.
         * @param rootDirectory this value specifies the folder where
         *                      your database should be created. Under
         *                      this each table will have its own folder.
         *                      NEVER modify table folders UNLESS the table
         *                      is run in LIGHT mode.
         * @param randomSeed each table that is partitioned has a random
         *                   generator to select a random partition used
         *                   on
         */
        public Builder(String rootDirectory, long randomSeed) {
            this.builderRootDirectory = rootDirectory;
            this.randomSeed = randomSeed;
        }

        public Builder withTableFromClass(Class<?> tableClass, Mode tableMode) {
            tableClasses.add(tableClass);
            tableModes.add(tableMode);
            return this;
        }

        public Builder withFieldB(String fieldB) {
            return this;
        }

        public Database build() {
            return new Database(builderRootDirectory, tableClasses, tableModes, randomSeed);
        }
    }


}
