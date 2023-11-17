package org.dava.core.database.service.objects.insert;

import org.dava.common.ArrayUtil;
import org.dava.core.database.objects.database.structure.Route;
import org.dava.core.database.objects.database.structure.Table;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.EmptiesPackage;
import org.dava.core.database.service.objects.WritePackage;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.dava.core.database.objects.exception.ExceptionType.ROLLBACK_ERROR;

public class InsertBatch {

    private EmptiesPackage tableEmpties;
    private List<RowWritePackage> rowWritePackages;
    private Map<String, List<IndexWritePackage>> indexWriteGroups;
    private List<String> numericRepartitions;


    public InsertBatch() {
        this.indexWriteGroups = new HashMap<>();
        this.numericRepartitions = new ArrayList<>();
        this.tableEmpties = new EmptiesPackage();
    }


    public static InsertBatch parse(List<String> lines) {
        Map<String, List<IndexWritePackage>> writeGroups = new ConcurrentHashMap<>();
        List<Route> empties = new ArrayList<>();
        List<String> numericPartitions = new ArrayList<>();

        lines.parallelStream().forEach( line -> {
                if (line.startsWith("R:")) {
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

        InsertBatch insertBatch = new InsertBatch();

        EmptiesPackage emptiesPackage = new EmptiesPackage();
        emptiesPackage.setRollbackEmpties(empties);
        insertBatch.tableEmpties = emptiesPackage;
        insertBatch.indexWriteGroups = writeGroups;
        insertBatch.numericRepartitions = numericPartitions;

        return insertBatch;
    }

    public void addIndexWritePackage(String indexPath, IndexWritePackage writePackage) {
        if ( indexWriteGroups.containsKey( indexPath ) ) {
            indexWriteGroups.get(indexPath).add(writePackage);
        }
        else {
            indexWriteGroups.put(
                indexPath,
                new ArrayList<>(
                    List.of( writePackage )
                )
            );
        }
    }

    public void addNumericRepartition(String indexPath) {
        this.numericRepartitions.add(indexPath);
    }

    public String makeRollbackString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Insert Batch:\n");

        // for rolling back rows added to the table
        // and new routes added to each index
        indexWriteGroups.forEach( (indexPath, groups) -> {
            builder.append("I:").append(indexPath).append(";");
            groups.forEach(writePackage ->
                builder.append("R:")
                    .append(writePackage.getRoute().getOffsetInTable())
                    .append(",")
                    .append(writePackage.getRoute().getLengthInTable())
                    .append(";")
            );
            // use the route above to remove lines from table. Then search for route in index and whitespace the route there
            builder.append("\n");
        });

        // for rolling back used row empties
        tableEmpties.getUsedEmpties().forEach( (length, empties) ->
            empties.forEach(empty ->
                builder.append("E:")
                    .append(empty.getOffsetInTable())
                    .append(",")
                    .append(empty.getLengthInTable())
                    .append(";")
                    .append("\n")
            // use the route to just add empties back to empties file (empties in table are whitespaced out in previous step)
        ));

        // for rolling back numeric repartitions
        numericRepartitions.forEach( folderPath ->
            builder.append("N:")
                .append(folderPath)
                .append("\n")
        );

        return builder.toString();
    }


    public void rollback(Table<?> table, String partition) {
        // TODO make sure this works even if the whole insert operation didn't complete

        // whitespace empty rows, and write empties in empties file
        Set<Route> emptyRoutes = new HashSet<>();
        List<WritePackage> whitespacePackagesFromEmtpies = tableEmpties.getRollbackEmpties().stream()
            .map( empty -> {
                emptyRoutes.add(empty);

                return new WritePackage(
                    empty.getOffsetInTable(),
                    Table.getWhitespaceBytes(empty.getLengthInTable())
                );
            })
            .toList();

        try {
            FileUtil.writeBytesIfPossible(
                table.getTablePath(partition),
                whitespacePackagesFromEmtpies
            );

            List<Route> empties = BaseOperationService.getAllEmpties(table.emptiesFilePath(partition));
            if (empties != null)
                emptyRoutes.addAll( empties );

            FileUtil.writeBytesAppend(
                table.emptiesFilePath(partition),
                ArrayUtil.appendArrays(
                    emptyRoutes.stream()
                        .map(route -> (Object) route.getRouteAsBytes())
                        .toList(),
                    10
                )
            );
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error undoing insert", e);
        }


        // delete indices referring to rows, and the rows themselves as well
        Set<Route> routesToDeleteFromTable = new HashSet<>();
        indexWriteGroups.forEach( (indexPath, writePackages) -> {
            try {
                List<Long> startBytes = writePackages.stream()
                    .map(write -> {
                        routesToDeleteFromTable.add(write.getRoute());
                        return write.getRoute().getOffsetInTable();
                    })
                    .collect(Collectors.toList());

                FileUtil.popBytes(indexPath, 10, startBytes);

            } catch (IOException e) {
                throw new DavaException(ROLLBACK_ERROR, "Error updating indices undoing insert", e);
            }
        });

        try {
            FileUtil.writeBytesIfPossible(
                table.getTablePath(partition),
                routesToDeleteFromTable.stream()
                    .map(route ->
                        new WritePackage(
                            route.getOffsetInTable(), Table.getWhitespaceBytes(route.getLengthInTable())
                        )
                    ).toList()
            );
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error deleting rows undoing insert", e);
        }


        // update table size
        long currentSize = table.getSize(partition);
        table.setSize(partition, currentSize - whitespacePackagesFromEmtpies.size() - routesToDeleteFromTable.size());


        // NOTE numeric count files aren't update. indices are just removed, and then they're left as is.
    }



    /*
        Getter setter
     */

    public EmptiesPackage getUsedTableEmtpies() {
        return tableEmpties;
    }

    public List<RowWritePackage> getRowsWritten() {
        return rowWritePackages;
    }

    public void setRowsWritten(List<RowWritePackage> rowsWritten) {
        this.rowWritePackages = rowsWritten;
    }

    public Map<String, List<IndexWritePackage>> getIndexPathToIndicesWritten() {
        return indexWriteGroups;
    }

    public void setUsedTableEmtpies(EmptiesPackage usedTableEmtpies) {
        this.tableEmpties = usedTableEmtpies;
    }

    public List<String> getNumericRepartitions() {
        return numericRepartitions;
    }

    public void setNumericRepartitions(List<String> numericRepartitions) {
        this.numericRepartitions = numericRepartitions;
    }

}
