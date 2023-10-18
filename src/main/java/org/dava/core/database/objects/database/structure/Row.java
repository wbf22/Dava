package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.dates.Date;
import org.dava.core.database.objects.exception.DavaException;

import java.util.*;

import static org.dava.common.Checks.*;
import static org.dava.core.database.objects.exception.ExceptionType.CORRUPTED_ROW_ERROR;

public class Row {
    private Map<String, Object> columnsToValues;
    private String tableName;


    public Row(String line, Table<?> table) {
        try {
            line = line.trim();
            String[] values = line.split(",");
            columnsToValues = new HashMap<>();
            List<Map.Entry<String, Column<?>>> list = new ArrayList<>(table.getColumns().entrySet());
            for (int i = 0; i < values.length; i++) {
                Column<?> column = list.get(i).getValue();
                columnsToValues.put(
                    column.getName(),
                    mapIfTrue(
                        Date.isDateSupportedDateType(column.getType()),
                        values[i],
                        stringValue -> Date.of(stringValue, column.getType())
                    )
                );
            }
            tableName = table.getTableName();
        } catch(IndexOutOfBoundsException e) {
            throw new DavaException(
                CORRUPTED_ROW_ERROR,
                "Error trying to parse table row to table type " + table.getTableName() + ". Raw row: " + line,
                e
            );
        }
    }


    public static String serialize(Table<?> table, Map<String, Object> columnsToValuesMap) {
        StringBuilder serialization = new StringBuilder();
        table.getColumns().values().forEach(column ->
            serialization.append( columnsToValuesMap.get(column.getName()) ).append(",")
        );
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
}
