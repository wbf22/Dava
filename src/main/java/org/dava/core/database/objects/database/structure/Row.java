package org.dava.core.database.objects.database.structure;

import org.dava.core.database.objects.exception.DavaException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.dava.core.database.objects.exception.ExceptionType.BASE_IO_ERROR;
import static org.dava.core.database.objects.exception.ExceptionType.CORRUPTED_ROW_ERROR;

public class Row {
    private Map<String, String> columnsToValues;


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
        } catch(IndexOutOfBoundsException e) {
            throw new DavaException(
                CORRUPTED_ROW_ERROR,
                "Error trying to parse table row to table type " + table.getTableName() + ". Raw row: " + line,
                e
            );
        }
    }


    public String serialize(Table<?> table) {
        StringBuilder serialization = new StringBuilder();
        table.getColumns().forEach(column ->
            serialization.append( columnsToValues.get(column.getName()) ).append(",")
        );
        serialization.append("\n");
        return serialization.toString();
    }


    public Row(Map<String, String> columnsToValues) {
        this.columnsToValues = columnsToValues;
    }




    public String getValue(String column) {
        return columnsToValues.get(column);
    }
 }
