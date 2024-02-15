package org.dava.core.database.service;

import org.dava.common.TypeUtil;
import org.dava.core.database.objects.database.structure.Database;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.sql.objects.Select;
import org.dava.external.annotations.Column;

import java.lang.reflect.*;
import java.time.temporal.Temporal;
import java.util.*;

import static org.dava.common.Checks.mapIfNotNull;
import static org.dava.common.TypeUtil.isBasicJavaType;
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
    public static <T> List<Row> parseRow(T row) {
        org.dava.external.annotations.Table annotation = Optional.ofNullable(
            row.getClass().getAnnotation( org.dava.external.annotations.Table.class )
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
        List<Row> parsedRows = new ArrayList<>();
        Map<String, Object> columnsToValues = new HashMap<>();
        for (Field field : row.getClass().getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            String columnName = mapIfNotNull(column, Column::name, "");
            columnName = (columnName.isEmpty())? field.getName() : columnName;

            Object value = getFieldValueUsingGetter(row, field);

            if (TypeUtil.isBasicJavaType(value.getClass())) {
                columnsToValues.put(columnName, value.toString());
            }
            else {
                parsedRows.addAll(
                    parseRow(value)
                );
            }
        }

        Row parsedRow = new Row(columnsToValues, tableName);

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
                "Row missing public getter for field: '" + field.getName() +
                "' in class '" + row.getClass() + "'. Looked for method '" + getterMethodName + "()' but could not find.",
                e
            );
        }

    }


    public static List<Row> executeSelect(Select select, Database database) {



        return null;
    }





}
