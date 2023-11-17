package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;

import java.util.*;

import static org.dava.common.Checks.*;
import static org.dava.core.database.objects.exception.ExceptionType.CORRUPTED_ROW_ERROR;

public class Row {
    private Map<String, Object> columnsToValues;
    private String tableName;
    private Route locationInTable;



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
                    mapIfTrue(
                        Date.isDateSupportedDateType(column.getType()),
                        values.get(i),
                        stringValue -> Date.of(stringValue, column.getType())
                    )
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

}
