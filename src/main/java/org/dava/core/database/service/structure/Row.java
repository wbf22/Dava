package org.dava.core.database.service.structure;

import org.dava.core.common.TypeUtil;
import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;

import static org.dava.core.common.Checks.*;
import static org.dava.core.database.objects.exception.ExceptionType.CORRUPTED_ROW_ERROR;

public class Row {
    private Map<String, Object> columnsToValues;
    private String tableName;
    private Route locationInTable;


    public Row() {}

    public Row(String line, Table<?> table, Route locationInTable) {
        try {
            this.locationInTable = locationInTable;
            line = line.trim();
            List<String> values = getValuesFromLine(line);
            columnsToValues = new HashMap<>();
            List<Map.Entry<String, Column<?>>> list = new ArrayList<>(table.getColumns().entrySet());
            for (int i = 0; i < values.size(); i++) {
                Column<?> column = list.get(i).getValue();
                columnsToValues.put(
                    column.getName(),
                    parseValue(column.getType(), values.get(i))
                );
            }
            tableName = table.getTableName();
        } catch(Exception e) {//IndexOutOfBoundsException
            throw new DavaException(
                CORRUPTED_ROW_ERROR,
                "Error trying to parse table row to table type " + table.getTableName() + ". Raw row: " + line,
                e
            );
        }
    }

    public static Object parseValue(Class<?> columnType, String stringValue) {
        if (TypeUtil.isNumericClass(columnType)){
            return new BigDecimal(stringValue);
        }
        if (Date.isDateSupportedDateType(columnType)) {
            return Date.of(stringValue, columnType);
        }
        return stringValue;
    }

    public static List<String> getValuesFromLine(String line) {
        List<String> values = new ArrayList<>();

        boolean inQuotes = false;
        StringBuilder value = new StringBuilder();
        for (char c : line.toCharArray()) {

            if (c == '\"') {
                inQuotes = !inQuotes;
            }
            else {
                if (c == ',' && !inQuotes) {
                    values.add(value.toString());
                    value = new StringBuilder();
                }
                else {
                    value.append(c);
                }
            }
        }
        values.add(value.toString());

        return values;
    }

    public static String serialize(Table<?> table, Map<String, Object> columnsToValuesMap) {
        StringBuilder serialization = new StringBuilder();
        table.getColumns().values().forEach(column -> {
            String value = columnsToValuesMap.get(column.getName()).toString();
            if (column.getType() == String.class && value.contains(",")) {
                serialization.append("\"").append( value ).append("\"").append(",");
            }
            else {
                serialization.append( value ).append(",");
            }
        });
        serialization.delete(serialization.length()-1, serialization.length());
        return serialization.toString();
    }

    public Row(Map<String, Object> columnsToValues, String tableName) {
        this.columnsToValues = columnsToValues;
        this.tableName = tableName;
    }

    public Row copy() {
        Row row = new Row();
        row.columnsToValues = new HashMap<>();
        for (Entry<String, Object> entry : this.columnsToValues.entrySet()) {
            row.columnsToValues.put(entry.getKey(), entry.getValue());
        }
        row.tableName = this.tableName;
        row.locationInTable = this.locationInTable;

        return row;
    }



    /*
        Getter Setter
     */
    public Object getValue(String column) {
        return columnsToValues.get(column);
    }

    public Map<String, Object> getColumnsToValues() {
        return columnsToValues;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Route getLocationInTable() {
        return locationInTable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            tableName,
            locationInTable,
            columnsToValues.entrySet().stream()
                .map( entry -> Objects.hash(entry.getKey(), entry.getValue()) )
                .reduce( Objects::hash )
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return row.getColumnsToValues().entrySet().stream()
            .map( entry -> {
                String columnName = entry.getKey();
                String value = entry.getValue().toString();
                String thisValue = this.columnsToValues.get(columnName).toString();

                return value.equals(thisValue);
            })
            .reduce(Boolean::logicalAnd)
            .orElse(false);
   }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Object obj : columnsToValues.values()) {
            builder.append(obj).append(", ");
        }
        return builder.substring(0, builder.length()-2);
    }

    public String toStringColumns(Set<String> columns) {
        StringBuilder builder = new StringBuilder();
        for (String column : columns) {
            Object obj = columnsToValues.get(column);
            builder.append(obj).append(", ");
        }
        return builder.substring(0, builder.length()-2);
    }


    public String toStringExcludeColumns(Set<String> columnsToExclude) {
        StringBuilder builder = new StringBuilder();
        for (Entry<String, Object> entry : columnsToValues.entrySet()) {
            if (!columnsToExclude.contains(entry.getKey())) {
                builder.append(entry.getValue()).append(", ");
            }
        }
        return builder.substring(0, builder.length()-2);
    }
}
