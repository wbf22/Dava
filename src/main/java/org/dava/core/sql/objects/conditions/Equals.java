package org.dava.core.sql.objects.conditions;

import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.service.BaseOperationService;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.dava.core.database.service.BaseOperationService.getCountForIndexPath;

public class Equals<T> implements Condition {

    private String target;
    private String value;

    public Equals(String target, String value) {
        this.target = target;
        this.value = value;
    }

    public static Equals in(String target, String value) {
        return new Equals<>(target, value);
    }


    @Override
    public boolean filter(Row row) {
        return row.getValue(target).equals(value);
    }

    @Override
    public List<Row> retrieve(Database database, List<Condition> parentFilters, String from, Long limit, Long offset) {

        List<Row> rows = new ArrayList<>();
        if (limit == null) {
            rows = BaseOperationService.getAllRowsFromIndex(database, from, target, value);
            rows = rows.stream()
                .filter(row -> {
                    boolean match = true;
                    for (Condition condition : parentFilters) {
                        match &= condition.filter(row);
                    }
                    return match;
                })
                .toList();
        }
        else {
            long startRow = 0L;
            long rowsPerIteration = 3L * limit / (4L * database.getTableByName(from).getPartitions().size());
            do {
                List<Row> retrieved = BaseOperationService.getRowsFromIndex(database, from, target, value, startRow, rowsPerIteration);
                retrieved = retrieved.stream()
                    .filter(row -> {
                        boolean match = true;
                        for (Condition condition : parentFilters) {
                            match &= condition.filter(row);
                        }
                        return match;
                    })
                    .toList();
                rows.addAll(retrieved);
            } while (rows.size() < limit);
            rows = rows.subList(0, Math.toIntExact(limit)); // TODO this could throw an overflow exception
        }

        return rows;
    }

    @Override
    public Long getCount(Database database, String from) {
        Table<?> table = database.getTableByName(from);
        return table.getPartitions().stream()
            .map(partition ->
                getCountForIndexPath(
                    Index.buildIndexPath(database.getRootDirectory(), from, partition, target, value)
                )
            )
            .filter(Objects::nonNull)
            .reduce(Long::sum)
            .orElse(null);

    }
}
