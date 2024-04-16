package org.dava.core.database.service;

import org.dava.api.annotations.Column;
import org.dava.api.annotations.PrimaryKey;
import org.dava.api.annotations.Table;
import org.dava.core.common.TypeUtil;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.objects.exception.ExceptionType;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Row;
import org.dava.core.sql.Select;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.Map.Entry;

import static org.dava.core.common.Checks.mapIfNotNull;
import static org.dava.core.common.Checks.safeCast;
import static org.dava.core.common.TypeUtil.isBasicJavaType;
import static org.dava.core.database.objects.exception.ExceptionType.*;


public class MarshallingService {

    private static final String NOT_A_TABLE_MSG = ". It may be that one of your table objects has a field that isn't supported.";


    /**
     * This method takes a table row (entity) and validates it with the table schema.
     * It checks the following constraints:
     *   type,
     *   not null,
     *   unique,
     *   foreign key,
     *   check/custom,
     *   multiple/table-wide, (ie column1 can't be this if column2 is this)
     *   composite key,
     *
     * If the row/entity also has nested objects (rows/entities) than those objects will
     * be validated with the schema as well, and returned as separate parsed rows.
     *
     * @param row the row to validate
     * @return a list of validated and parsed rows.
     * @param <T> row type
     */
    public static <T> Map<String, List<Row>> parseRow(T row) {
        Table annotation = Optional.ofNullable(
            row.getClass().getAnnotation( org.dava.api.annotations.Table.class )
        ).orElseThrow(
            () -> new DavaException(NOT_A_TABLE, "Class missing @Table annotation: " + row.getClass().getName() + NOT_A_TABLE_MSG, null)
        );

        String tableName = (annotation.name().isEmpty())? row.getClass().getSimpleName() : annotation.name();


        // TODO check constraints in this method
        // parse rows and apply column constraints
        /*
         *   type,
         *   not null,
         *   unique,
         *   foreign key,
         *   check/custom,
         */
        Map<String, List<Row>> parsedRows = new HashMap<>();
        Map<String, Object> columnsToValues = new HashMap<>();
        for (Field field : row.getClass().getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            String columnName = mapIfNotNull(column, Column::name, "");
            columnName = (columnName.isEmpty())? field.getName() : columnName;

            Object value = getFieldValue(row, field);

            if (TypeUtil.isBasicJavaType(value.getClass())) {
                columnsToValues.put(columnName, value.toString());
            }
            else {

                Map<String, List<Row>> otherTableRows = parseRow(value);
                for (String key : otherTableRows.keySet()) {

                    if (parsedRows.containsKey(key))
                        parsedRows.get(key).addAll(otherTableRows.get(key));
                    else 
                        parsedRows.put(key, otherTableRows.get(key));

                }

                Object primaryKey = getPrimaryKeyOfObject(value);
                columnsToValues.put(columnName, primaryKey);
            }
        }

        Row parsedRow = new Row(columnsToValues, tableName);

        if (parsedRows.containsKey(tableName))
            parsedRows.get(tableName).add(parsedRow);
        else 
            parsedRows.put(tableName, new ArrayList<>(List.of(parsedRow)));

        // apply table constraints
        /*
         *   multiple/table-wide,
         *   composite key,
         */

        return parsedRows;
    }


    private static Object getPrimaryKeyOfObject(Object value) {
        Class<?> valueClass = value.getClass();

        Field field = getPrimaryKeyField(valueClass);

        boolean originalAccessibility = field.canAccess(value);
        try {
            field.setAccessible(true);

            return field.get(value);

        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new DavaException(
                ExceptionType.TABLE_PARSE_ERROR, 
                "Failed trying to get primary key when saving child object. Class: " + valueClass.getName(), 
                e
            );
        } finally {
            // Restore the original accessibility status
            field.setAccessible(originalAccessibility);
        }
    }

    public static <T> Field getPrimaryKeyField(Class<T> tableClass) {
        for (Field field : tableClass.getFields()) {
            Annotation primaryKeyAnnotation = tableClass.getAnnotation(PrimaryKey.class);

            if (primaryKeyAnnotation != null) 
                return field;
        }


        throw new DavaException(
            ExceptionType.TABLE_PARSE_ERROR, 
            "Missing primary key annotation on table object. Failed when trying to save child object. Class: " + tableClass.getName(), 
            null
        );
    }

    public static <T> Object getFieldValue(T row, Field field) {
        boolean originalAccessibility = field.canAccess(row);

        try {
            field.setAccessible(true);

            return field.get(row);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new DavaException(
                ExceptionType.TABLE_PARSE_ERROR, 
                "Failed trying to get field for class: " + row.getClass().getName(), 
                e
            );
        } finally {
            // Restore the original accessibility status
            field.setAccessible(originalAccessibility);
        }
    }
    
    private static <T> Object getFieldValueUsingGetter(T row, Field field) {
        String getterMethodName = "get" +
            field.getName().substring(0, 1).toUpperCase() +
            field.getName().substring(1);

        try {
            Method getterMethod = row.getClass().getMethod(getterMethodName);

            return getterMethod.invoke(row);
        }
        catch (ReflectiveOperationException e) {
            throw new DavaException(
                ROW_MISSING_PUBLIC_GETTER,
                "Row missing public getter for field: '" + field.getName() +
                "' in class '" + row.getClass() + "'. Looked for method '" + getterMethodName + "()' but could not find.",
                e
            );
        }

    }



    public static <T> T parseObject(Row row, Class<T> tableClass) {
        
        T object;
        try {
            Constructor<?> defaultConstructor = tableClass.getDeclaredConstructor();
            object = safeCast(defaultConstructor.newInstance(), tableClass);

            Map<String, Object> rowValues = row.getColumnsToValues();
        
            for (Field field : tableClass.getFields()) {
                boolean originalAccessibility = field.canAccess(object);

                try {
                    field.setAccessible(true);

                    field.set(
                        object, 
                        rowValues.get(field.getName())
                    );
                } finally {
                    // Restore the original accessibility status
                    field.setAccessible(originalAccessibility);
                }
            }
        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new DavaException(ExceptionType.TABLE_PARSE_ERROR, "Couldn't find or call no args constructor on your table: " + tableClass.getName(), e);
        }
        
        return object;
    }



}
