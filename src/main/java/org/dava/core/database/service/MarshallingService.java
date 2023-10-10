package org.dava.core.database.service;

import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.sql.objects.Select;
import org.dava.external.annotations.Column;

import java.lang.reflect.*;
import java.util.*;

import static org.dava.common.Checks.mapIfNotNull;
import static org.dava.core.database.objects.exception.ExceptionType.ROW_MISSING_PUBLIC_GETTER;


public class MarshallingService {


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
     * @param database the Database object with parsed schema information for each table
     * @return a list of validated and parsed rows.
     * @param <T> row type
     */
    public static <T> List<Row> parseAndValidateRow(T row, Database database) {

        // parse rows and apply column constraints
        /*
         *   type,
         *   not null,
         *   unique,
         *   foreign key,
         *   check/custom,
         */
        List<Row> parsedRows = new ArrayList<>();
        Map<String, String> columnsToValues = new HashMap<>();
        for (Field field : row.getClass().getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            String columnName = mapIfNotNull(column, Column::name, "");
            columnName = (columnName.isEmpty())? field.getName() : columnName;

            Object value = getFieldValueUsingGetter(row, field);

            if (isBasicJavaType(value.getClass())) {
                columnsToValues.put(columnName, value.toString());
            }
            else {
                parsedRows.addAll(
                    parseAndValidateRow(value, database)
                );
            }

        }

        Row parsedRow = new Row(columnsToValues);

        parsedRows.add(parsedRow);

        // apply table constraints
        /*
         *   multiple/table-wide,
         *   composite key,
         */

        return parsedRows;
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
                "Row missing public getter for field: " + field.getName() +
                "Looked for method '" + getterMethodName + "()' but could not find.",
                e
            );
        }

    }


    public static List<Row> executeSelect(Select select, Database database) {



        return null;
    }



    private static boolean isBasicJavaType(Class<?> type) {
        return isNumericClass(type) || Date.class.isAssignableFrom(type) || type == String.class;
    }

    private static boolean isNumericClass(Class<?> type) {
        return Number.class.isAssignableFrom(type) || isPrimitiveNumericClass(type);
    }

    private static boolean isPrimitiveNumericClass(Class<?> type) {
        return type == int.class || type == long.class || type == double.class
            || type == float.class || type == short.class || type == byte.class;
    }


}
