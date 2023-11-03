package org.dava.core.database.service;

import org.dava.common.Bundle;
import org.dava.common.TypeUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.objects.delete.DeleteBatch;
import org.dava.core.database.service.objects.delete.IndexDelete;
import org.dava.core.sql.objects.conditions.Condition;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.dava.core.database.objects.exception.ExceptionType.INDEX_READ_ERROR;

public class Delete {


    private Database database;
    private Table<?> table;


    public void delete(List<Row> rows, List<Condition> conditionals) {
        DeleteBatch batch = new DeleteBatch();

        // determine table rows to change

        // determine changes to table empties file (table size in this file)

        // determine all indices that point to the row and determine changes to indices (routes and file deletes)
        // determine changes to numeric count files
        Bundle< Map<String, Integer>, Map<String, IndexDelete> > data = collectIndexData(rows);
        batch.setNumericCountFileChanges(data.getFirst());
        batch.setIndicesToRemove(data.getSecond());

        // determine changes to index empties files


        // log rollback

        // perform delete
    }

    private Bundle<Map<String, Integer>, Map<String, IndexDelete>> collectIndexData(List<Row> rows) {

        Map<String, Integer> numericCountFileChanges = new HashMap<>();

        Map<String, IndexDelete> indexDeletes = rows.parallelStream()
            .flatMap( row -> row.getColumnsToValues().entrySet().stream()
                    .flatMap( entry -> {
                            Column<?> column = table.getColumn(entry.getKey());

                            return table.getPartitions().stream()
                                .flatMap(partition -> {
                                    String indexPath = Index.buildIndexPath(database.getRootDirectory(), table.getTableName(), partition, column, table.getLeafList(partition, entry.getKey()), entry.getValue().toString());

                                    Bundle<Long, List<IndexRoute>> bundle = BaseOperationService.getRoutes(
                                        indexPath,
                                        partition,
                                        8L,
                                        null
                                    );

                                    // figure out which routes (ordered) should be deleted
                                    List<IndexRoute> lines = bundle.getSecond();
                                    List<Integer> routesToDelete = IntStream.range(0, lines.size())
                                            .filter(i -> lines.get(i).equals(row.getLocationInTable()))
                                            .boxed()
                                            .toList();

                                    // figure out if index is being deleted too
                                    boolean isEmpty = bundle.getFirst() - routesToDelete.size() * 10L <= 8;
                                    IndexDelete indexDelete = new IndexDelete(routesToDelete, isEmpty);

                                    // determine if count file will change for numeric types
                                    if (isEmpty && TypeUtil.isNumericClass(column.getType())) {
                                        String indexFolder = Index.buildIndexRootPath(
                                            database.getRootDirectory(),
                                            table.getTableName(),
                                            partition,
                                            column,
                                            table.getLeafList(partition, column.getName()),
                                            entry.getValue()
                                        );

                                        String countFile = indexFolder  + "/c.count";

                                        try {
                                            numericCountFileChanges.put(
                                                countFile,
                                                (int) BaseOperationService.getNumericCount(countFile)
                                            );
                                        } catch (IOException e) {
                                            throw new DavaException(INDEX_READ_ERROR, "Error getting count for numeric index: " + countFile, e);
                                        }
                                    }

                                    return Stream.of(Map.entry(indexPath, indexDelete));
                                });
                    }))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new Bundle<>(numericCountFileChanges, indexDeletes);
    }


}
