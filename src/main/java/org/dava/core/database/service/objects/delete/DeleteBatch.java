package org.dava.core.database.service.objects.delete;

import org.dava.core.database.objects.database.structure.Route;
import org.dava.core.database.objects.database.structure.Row;
import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.service.objects.Batch;
import org.dava.core.database.service.objects.EmptiesPackage;
import org.dava.core.database.service.objects.insert.IndexWritePackage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public class DeleteBatch {
    private List<Row> rows;
    private long oldTableSize;
    private long oldEmptiesSize;
    private Map<String, IndexDelete> indexPathToIndicesToDelete;
    private Map<String, CountChange> numericCountFileChanges;

    public static Batch parse(List<String> lines) {
        Map<String, List<IndexWritePackage>> writeGroups = new ConcurrentHashMap<>();
        List<Route> empties = new ArrayList<>();
        List<String> numericPartitions = new ArrayList<>();

        lines.parallelStream().forEach( line -> {
            if (line.startsWith("I:")) {
                String[] subs = line.split(";");
                String indexPath = subs[0].substring(2);

                List<IndexWritePackage> writePackages = IntStream.range(1, subs.length)
                    .mapToObj( i -> {
                        String nums = subs[i].substring(2);
                        Long offset = Long.parseLong(nums.split(",")[0]);
                        Integer length = Integer.parseInt(nums.split(",")[1]);

                        return new IndexWritePackage(
                            new Route(
                                null,
                                offset,
                                length
                            ),
                            null,
                            null,
                            null
                        );

                    })
                    .toList();

                writeGroups.put(indexPath, writePackages);
            }
            else if (line.startsWith("E:")) {
                String nums = line.substring(2);
                Long offset = Long.parseLong(nums.split(",")[0]);
                Integer length = Integer.parseInt(nums.split(",")[1]);

                empties.add(
                    new Route(
                        null,
                        offset,
                        length
                    )
                );
            }
            else if (line.startsWith("N:")) {
                String folderPath = line.substring(2);
                numericPartitions.add(folderPath);
            }
        });

        Batch insertBatch = new Batch();

        EmptiesPackage emptiesPackage = new EmptiesPackage();
        emptiesPackage.setRollbackEmpties(empties);
//        insertBatch.tableEmpties = emptiesPackage;
//        insertBatch.indexWriteGroups = writeGroups;
//        insertBatch.numericRepartitions = numericPartitions;

        return insertBatch;
    }


    public String getRollbackString(Table<?> table, String partition) {
        StringBuilder builder = new StringBuilder();
        builder.append("Delete Batch:\n");


        return builder.toString();
    }


    /*
        getter setter
     */

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public long getOldTableSize() {
        return oldTableSize;
    }

    public void setOldTableSize(long oldTableSize) {
        this.oldTableSize = oldTableSize;
    }

    public long getOldEmptiesSize() {
        return oldEmptiesSize;
    }

    public void setOldEmptiesSize(long oldEmptiesSize) {
        this.oldEmptiesSize = oldEmptiesSize;
    }

    public Map<String, IndexDelete> getIndexPathToIndicesToDelete() {
        return indexPathToIndicesToDelete;
    }

    public void setIndexPathToIndicesToDelete(Map<String, IndexDelete> indexPathToIndicesToDelete) {
        this.indexPathToIndicesToDelete = indexPathToIndicesToDelete;
    }

    public Map<String, CountChange> getNumericCountFileChanges() {
        return numericCountFileChanges;
    }

    public void setNumericCountFileChanges(Map<String, CountChange> numericCountFileChanges) {
        this.numericCountFileChanges = numericCountFileChanges;
    }
}
