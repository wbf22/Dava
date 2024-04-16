package org.dava.api;

import org.dava.api.annotations.PrimaryKey;
import org.dava.api.annotations.Query;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.MarshallingService;
import org.dava.core.database.service.operations.Delete;
import org.dava.core.database.service.operations.Insert;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.database.service.structure.Table;
import org.dava.core.sql.Select;
import org.dava.core.sql.conditions.All;
import org.dava.core.sql.conditions.Condition;
import org.dava.core.sql.conditions.Equals;
import org.dava.core.sql.conditions.In;
import org.dava.core.sql.parsing.SqlService;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.crypto.Data;

import static org.dava.core.common.Checks.safeCastParameterized;
import static org.dava.core.database.objects.exception.ExceptionType.REPOSITORY_ERROR;


public class Repository<T, ID> {

    private Database database;
    private Table<T> table;



    public Repository (Database database){
        this.database = database;
        String tableName = this.getClass().getTypeParameters()[0].getClass().getName();
        this.table = safeCastParameterized(database.getTableByName(tableName), Table.class);
    }



    /**
     * finds a table record using the primary key.
     * 
     * @param primaryKey
     * @return returns the record or null if not found
     */
    public T findById(ID primaryKey) {
        String columnName = MarshallingService.getPrimaryKeyField(table.getTableClass()).getName();
    
        Equals equals = new Equals(columnName, primaryKey.toString());
        
        return equals.retrieve(table, List.of(), null, null).stream()
            .map(row -> MarshallingService.parseObject(row, table.getTableClass()))
            .findFirst()
            .orElse(null);
    }



    /**
     * finds all table records in the list of ids.
     * 
     * @param primaryKeys
     * @return returns all records found
     */
    public T findAllById(List<ID> primaryKeys) {

        In in = new In<>(null, null);
        String columnName = MarshallingService.getPrimaryKeyField(table.getTableClass()).getName();
    
        Equals equals = new Equals(columnName, primaryKey.toString());
        
        return equals.retrieve(table, List.of(), null, null).stream()
            .map(row -> MarshallingService.parseObject(row, table.getTableClass()))
            .findFirst()
            .orElse(null);
    }



    /**
     * Finds all table records for the given column name and value.
     * 
     * @param columnName name of the column in the table
     * @param value that each returned record should have for the provided column
     * @return 
     */
    public List<T> findByColumn(String columnName, String value) {
        
        Equals equals = new Equals(columnName, value);
        
        return equals.retrieve(table, List.of(), null, null).stream()
            .map(row -> MarshallingService.parseObject(row, table.getTableClass()))
            .toList();
    }

    /**
     * Returns all table records in the table
     * @return
     */
    public List<T> findAll() {

        All all = new All();

        return all.retrieve(table, List.of(), null, null).stream()
            .map(row -> MarshallingService.parseObject(row, table.getTableClass()))
            .toList();
    }

    /**
     * Performs a custom SQL style query. The sytax most closely resembles Postgresql but 
     * some features will probably be missing. 
     * 
     * <p>This method extracts the '@Query' annotation on the calling method and parses it
     * to perform the database operations. This method could be slower than other methods
     * in this class, as it performs an extra parsing step before accessing the database.
     * However the speed difference is probably negligible.
     * 
     * @param params list of params to be used in the sql query. Params in the query should be denoted with 
     * ':<param-name>' and that same name should be used as a key in this params map.
     * @return
     */
    public List<T> query(Map<String, String> params) {
        String query = getQueryFromCaller(
            Thread.currentThread().getStackTrace()
        );

        Select select = SqlService.parse(query, table);

        return select.retrieve();
    }

    private String getQueryFromCaller(StackTraceElement[] stackTrace) {
        Method callingMethod = getCallingMethod(stackTrace);
        if (callingMethod != null) {
            Query queryAnnotation = callingMethod.getAnnotation(Query.class);
            if (queryAnnotation != null) {
                return queryAnnotation.query();
            }
        }
        throw new DavaException(REPOSITORY_ERROR, "Tried to invoke Repository<T, ID>.query() " +
                                    "in a method without an @Query annotation", null);
    }

    private Method getCallingMethod(StackTraceElement[] stackTrace) {
        StackTraceElement element = stackTrace[2];
        Class<?> callerClass = this.getClass();
        Method[] methods = callerClass.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals(element.getMethodName())) {
                return method;
            }
        }
        return null;
    }

    /**
     * Returns a T object after querying the database, using the matching fields in the Q object.
     * 
     * <p> So if your 'Q' object has a field named 'userName', all rows in the table that match that value
     * in the 'userName' column will be returned. If the 'Q' object has multiple fields, then only rows
     * that match those all values will be returned.
     * 
     * <p> Null values in the 'Q' object will match null values in the table
     * 
     * <p> <i>Note: (if speed is important, this method can be faster than using the query method above, since no
     * sql parsing is needed. However, the difference may be negligible)
     * 
     * <p> <i>Note: (if speed is important, put the most limiting field first in the 'Q' class. The field names
     * in the 'Q' class only have to match the names of the fields in the 'T' class of this repository. They
     * don't have to be ordered the same. This method will first retrieve rows from the table based on the first
     * field, and then filter out the returned rows using the other fields. If you know one of your fields will
     * filter out more rows than others, put that field first in the class.) 
     * 
     * @param <Q> the type of object that has matching field names to the 'T' param type of this repository
     * @param objectWithSomeFields provided 'Q' object populated with fields to search on
     * @return
     */
    public <Q> List<T> findByProvidedFields(Q objectWithSomeFields) {

        List<Condition> equals = new ArrayList<>();

        for (Field field : objectWithSomeFields.getClass().getFields()) {
            equals.add(
                new Equals(field.getName(), MarshallingService.getFieldValue(objectWithSomeFields, field).toString())
            );
        }

        Condition first = equals.remove(equals.size() - 1);

        return first.retrieve(table, equals, null, null).stream()
            .map(row -> MarshallingService.parseObject(row, table.getTableClass()))
            .toList();
    }

    /**
     * Saves the object and any sub objects into their respective tables. If one of the fields
     * of the class isn't a normal java type, it's assumed to be it's own table created by you.
     * 
     * <p> This method is transactional in that if one of the these sub objects fails during the save,
     * all the changes will be rolled back.
     * 
     * @param row
     */
    public void save(T row) {

        Map<String, List<Row>> tableNameToRows = MarshallingService.parseRow(row);

        for (String tableName : tableNameToRows.keySet()) {
            Table<?> tableOfRow = database.getTableByName(tableName);
            Insert insert = new Insert(database, tableOfRow, tableOfRow.getRandomPartition());
            insert.insert(
                tableNameToRows.get(tableName)
            );
        }
        
    }

    /**
     * Saves all the objects and any sub objects into their respective tables. If one of the fields
     * of a class isn't a normal java type, it's assumed to be it's own table created by you.
     * 
     * <p> This method is transactional in that if any of the these objects or sub objects fails during the save,
     * all the changes will be rolled back.
     * 
     * @param row
     */
    public void saveAll(List<T> rows) {

        Map<String, List<Row>> tableNameToRows = new HashMap<>();

        for (T row : rows) {
            Map<String, List<Row>> rowMap = MarshallingService.parseRow(row);
            for (String tableName : rowMap.keySet()) {

                if (tableNameToRows.containsKey(tableName))
                    tableNameToRows.get(tableName).addAll(rowMap.get(tableName));
                else 
                    tableNameToRows.put(tableName, rowMap.get(tableName));

            }
        }

        saveRows(tableNameToRows);
    }

    private void saveRows(Map<String, List<Row>> tableNameToRows) {
        for (String tableName : tableNameToRows.keySet()) {
            Table<?> tableOfRow = database.getTableByName(tableName);
            Insert insert = new Insert(database, tableOfRow, tableOfRow.getRandomPartition());
            insert.insert(
                tableNameToRows.get(tableName)
            );
        }
    }

    /**
     * Delete a record in the table by it's primary key.
     * 
     * @param primaryKey
     * @param cascade wether or not sub object (or other table references) should be deleted as well.
     */
    public void delete(ID primaryKey, boolean cascade) {

        Equals equals = new Equals(MarshallingService.getPrimaryKeyField(table.getTableClass()).getName(), primaryKey.toString());
        List<Row> rows = equals.retrieve(table, List.of(), null, null);

        Delete delete = new Delete(database, table);
        delete.delete(rows);

        if (cascade) {
            List<Row> childRows = getRowsOfAllSubObjects(rows, null);

        }

    }

    /**
     * Delete multiple records in the table by the provided primary keys
     * @param primaryKeys
     * @param cascade wether or not sub object (or other table references) should be deleted as well.
     */
    public void deleteAll(List<ID> primaryKeys, boolean cascade) {

    }

    private List<Row> getRowsOfAllSubObjects(List<Row> rows, Class<T> tableClass) {
        List<Row> childRows = new ArrayList<>();

        for (Field field : tableClass.getFields()) {
            Class<?> fieldType = field.getType();
            if (fieldType.getAnnotation(org.dava.api.annotations.Table.class) != null) {
                String primaryKeyFieldName = MarshallingService.getPrimaryKeyField(fieldType).getName();

                List<Row> newRows = rows.parallelStream()
                    .flatMap(row -> {
                        String primaryKey = row.getValue(primaryKeyFieldName).toString();

                        Equals equals = new Equals(primaryKeyFieldName, primaryKey);
                        return equals.retrieve(table, List.of(), null, null).stream();
                    })
                    .toList();

                childRows.addAll(newRows);
            }

        }


        return childRows;
    }




}
