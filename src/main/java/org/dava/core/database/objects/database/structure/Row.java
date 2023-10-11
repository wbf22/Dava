package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.exception.DavaException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.dava.core.database.objects.exception.ExceptionType.CORRUPTED_ROW_ERROR;

public class Row {
    private Map<String, String> columnsToValues;
    private String tableName;


    public Row(String line, Table<?> table) {
        try {
            String[] values = line.split(",");
            List<Column<?>> columns = table.getColumns();
            columnsToValues = new HashMap<>();
            for (int i = 0; i < values.length; i++) {
                columnsToValues.put(
                    columns.get(i).getName(),
                    values[i]
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


    public static String serialize(Table<?> table, Map<String, String> columnsToValuesMap) {
        StringBuilder serialization = new StringBuilder();
        table.getColumns().forEach(column ->
            serialization.append( columnsToValuesMap.get(column.getName()) ).append(",")
        );
        serialization.delete(serialization.length()-1, serialization.length());
        serialization.append("\n");
        return serialization.toString();
    }


    public Row(Map<String, String> columnsToValues, String tableName) {
        this.columnsToValues = columnsToValues;
        this.tableName = tableName;
    }


    /*
        Getter Setter
     */
    public String getValue(String column) {
        return columnsToValues.get(column);
    }

    public Map<String, String> getColumnsToValues() {
        return columnsToValues;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
