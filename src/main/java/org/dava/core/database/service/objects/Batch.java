package org.dava.core.database.service.objects;

import org.dava.common.ArrayUtil;
import org.dava.core.database.objects.database.structure.*;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.service.BaseOperationService;
import org.dava.core.database.service.fileaccess.FileUtil;
import org.dava.core.database.service.objects.delete.CountChange;
import org.dava.core.database.service.objects.delete.IndexDelete;
import org.dava.core.database.service.objects.insert.IndexWritePackage;
import org.dava.core.database.service.objects.insert.RowWritePackage;
import org.dava.core.database.service.type.compression.TypeToByteUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.dava.core.database.objects.exception.ExceptionType.ROLLBACK_ERROR;

public class Batch {


    // insert
    private EmptiesPackage usedTableEmtpies;
    private List<RowWritePackage> rowsWritten;
    private Map<String, List<IndexWritePackage>> indexPathToIndicesWritten;

    // delete
    private List<Row> rows;
    private Long oldTableSize;
    private Long oldEmptiesSize;
    private Map<String, IndexDelete> indexPathToInvalidRoutes;

    // shared
    private Map<String, CountChange> numericCountFileChanges;


    public Batch() {
        this.usedTableEmtpies = new EmptiesPackage();
        this.rowsWritten = new ArrayList<>();
        this.indexPathToIndicesWritten = new HashMap<>();

        this.rows = new ArrayList<>();
        this.indexPathToInvalidRoutes = new HashMap<>();
        this.numericCountFileChanges = new HashMap<>();
    }

    public static Batch parse(List<String> lines, Table<?> table, String partition) {
        Map<String, List<IndexWritePackage>> writeGroups = new HashMap<>();
        List<RowWritePackage> rowsWritten = new ArrayList<>();
        List<Empty> empties = new ArrayList<>();
        List<Row> rows = new ArrayList<>();
        Long oldTableSize = null;
        Long oldEmptiesSize = null;
        Map<String, IndexDelete> indexPathToInvalidRoutes = new HashMap<>();
        Map<String, CountChange> numericCountFileChanges = new HashMap<>();


        for (String line : lines) {
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
                    new Empty(
                        -1,
                        new Route(
                            null,
                            offset,
                            length
                        )
                    )
                );
            }
            else if (line.startsWith("Rw:")) {
                String nums = line.substring(5);
                Long offset = Long.parseLong(nums.split(",")[0]);
                Integer length = Integer.parseInt(nums.split(",")[1]);

                rowsWritten.add(
                    new RowWritePackage(
                        new Route(
                            null,
                            offset,
                            length
                        ),
                        null,
                        null
                    )
                );

            }
            else if (line.startsWith("Ro:")) {
                line = line.substring(3);
                StringBuilder bd = new StringBuilder();
                int i = 0;
                while (line.charAt(i) != ',') {
                    bd.append(line.charAt(i));
                    i++;
                }
                Long offset = Long.parseLong(bd.toString());

                while (line.charAt(i) != ';') {
                    i++;
                    bd.append(line.charAt(i));
                }
                Integer length = Integer.parseInt(bd.substring(0, bd.length()-1));

                rows.add(
                    new Row(
                        line.substring(i+1),
                        table,
                        new Route(
                            partition,
                            offset,
                            length
                        )
                    )
                );
            }
            else if (line.startsWith("TS:")) {
                line = line.substring(3);
                oldTableSize = Long.parseLong(line);
            }
            else if (line.startsWith("ES:")) {
                line = line.substring(3);
                oldEmptiesSize = Long.parseLong(line);
            }
            else if (line.startsWith("ID:")) {
                String[] subs = line.split(";");
                String indexPath = subs[0].substring(3);

                indexPathToInvalidRoutes.put(
                    indexPath,
                    new IndexDelete(
                        new ArrayList<>(),
                        IntStream.range(1, subs.length)
                            .mapToObj( i -> {
                                String nums = subs[i].substring(2);
                                Long offset = Long.parseLong(nums.split(",")[0]);
                                Integer length = Integer.parseInt(nums.split(",")[1]);

                                return new Route(
                                    null,
                                    offset,
                                    length
                                );
                            })
                            .toList()
                    )
                );
            }
            else if (line.startsWith("C:")) {
                String[] subs = line.split(",");
                String countFilePath = subs[0].substring(2);
                long count = Long.parseLong(subs[1]);
                numericCountFileChanges.put(
                    countFilePath,
                    new CountChange(
                        count,
                        0L
                    )
                );
            }
        }

        Batch batch = new Batch();

        EmptiesPackage emptiesPackage = new EmptiesPackage();
        emptiesPackage.setRollbackEmpties(empties);
        batch.usedTableEmtpies = emptiesPackage;
        batch.rowsWritten = rowsWritten;
        batch.indexPathToIndicesWritten = writeGroups;
        batch.rows = rows;
        batch.oldTableSize = oldTableSize;
        batch.oldEmptiesSize = oldEmptiesSize;
        batch.indexPathToInvalidRoutes = indexPathToInvalidRoutes;
        batch.numericCountFileChanges = numericCountFileChanges;

        return batch;
    }

    public void addIndexWritePackage(String indexPath, IndexWritePackage writePackage) {
        if ( indexPathToIndicesWritten.containsKey(indexPath ) ) {
            indexPathToIndicesWritten.get(indexPath).add(writePackage);
        }
        else {
            indexPathToIndicesWritten.put(
                indexPath,
                new ArrayList<>(
                    List.of( writePackage )
                )
            );
        }
    }

    /**
     * Keys in Rollback String:
     * - I: index that has been added
     * - E: used empties route
     * - N: numeric repartition
     * - Rw: row that was written
     * - Ro: row that was removed
     * - TS: old table size
     * - ES: old empties size
     * - ID: invalidated index by row removal
     * - C: numeric count file change
     */
    public String makeRollbackString(Table<?> table, String partition) {
        StringBuilder builder = new StringBuilder();


        // for rolling back rows added to the table
        // and new routes added to each index
        indexPathToIndicesWritten.forEach((indexPath, groups) -> {
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
        usedTableEmtpies.getUsedEmpties().forEach((length, empties) ->
            empties.forEach(empty ->
                builder.append("E:")
                    .append(empty.getRoute().getOffsetInTable())
                    .append(",")
                    .append(empty.getRoute().getLengthInTable())
                    .append("\n")
                // use the route to just add empties back to empties file (empties in table are whitespaced out in previous step)
            ));

        // for rolling back rows added to the table
        rowsWritten.forEach(rowWritePackage ->
            builder.append("Rw:")
                .append("R:")
                .append(rowWritePackage.getRoute().getOffsetInTable())
                .append(",")
                .append(rowWritePackage.getRoute().getLengthInTable())
                .append("\n")
        );

        // all the rows that are being deleted
        rows.forEach( row -> {
            builder.append("Ro:");
            if (row.getLocationInTable() != null) {
                builder.append(row.getLocationInTable().getOffsetInTable())
                    .append(",")
                    .append(row.getLocationInTable().getLengthInTable())
                    .append(";");
            }
            builder.append(Row.serialize(table, row.getColumnsToValues()))
                .append("\n");
        });

        // table sizes
        if (oldTableSize != null)
            builder.append("TS:")
                .append(oldTableSize)
                .append("\n");

        // emtpies sizes
        if (oldEmptiesSize != null)
            builder.append("ES:")
                .append(oldEmptiesSize)
                .append("\n");


        // indices to delete
        indexPathToInvalidRoutes.forEach((indexPath, indexDelete) -> {
            builder.append("ID:").append(indexPath).append(";");
            indexDelete.getOriginalRoutes().forEach(route -> {
                builder.append("R:")
                    .append(route.getOffsetInTable())
                    .append(",")
                    .append(route.getLengthInTable())
                    .append(";");
            });
            // use the route above to remove lines from table. Then search for route in index and whitespace the route there
            builder.append("\n");
        });

        // old count file values
        numericCountFileChanges.forEach( (countFile, countChange) -> {
            builder.append("C:")
                .append(countFile)
                .append(",")
                .append(countChange.getOldCount());
            // use the route above to remove lines from table. Then search for route in index and whitespace the route there
            builder.append("\n");
        });



        return builder.toString();
    }

    //TODO maybe split up this giant rollback method
    public void rollback(Table<?> table, String partition) {

        // whitespace empty rows, and write empties in empties file
        Set<Route> emptyRoutes = new HashSet<>();
        List<WritePackage> whitespacePackagesFromEmtpies = usedTableEmtpies.getRollbackEmpties().stream()
            .map( empty -> {
                emptyRoutes.add(empty.getRoute());

                return new WritePackage(
                    empty.getRoute().getOffsetInTable(),
                    Table.getWhitespaceBytes(empty.getRoute().getLengthInTable())
                );
            })
            .toList();

        try {
            FileUtil.writeBytesIfPossible(
                table.getTablePath(partition),
                whitespacePackagesFromEmtpies
            );

            if (!whitespacePackagesFromEmtpies.isEmpty()) {
                List<Route> empties = BaseOperationService.getAllEmpties(table.emptiesFilePath(partition));
                if (empties != null)
                    emptyRoutes.addAll( empties );

                FileUtil.writeBytes(
                    table.emptiesFilePath(partition),
                    8L,
                    ArrayUtil.appendArrays(
                        emptyRoutes.stream()
                            .map(route -> (Object) route.getRouteAsBytes())
                            .toList(),
                        10
                    )
                );
            }

        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error undoing insert", e);
        }


        // delete indices referring to rows
        indexPathToIndicesWritten.forEach((indexPath, writePackages) -> {
            try {
                List<Route> routes = new ArrayList<>(
                    BaseOperationService.getFileSizeAndRoutes(indexPath, partition, 0L, 10).getSecond()
                );
                routes.removeAll(
                    writePackages.stream()
                        .map(IndexWritePackage::getRoute)
                        .toList()
                );

                FileUtil.replaceFile(
                    indexPath,
                    routes.stream().map(route -> new WritePackage(
                        route.getOffsetInTable(),
                        route.getRouteAsBytes()
                    ))
                    .toList()
                );

                if (FileUtil.fileSize(indexPath) == 0)
                    FileUtil.deleteFile(indexPath);

            } catch (IOException e) {
                throw new DavaException(ROLLBACK_ERROR, "Error updating indices undoing insert", e);
            }
        });

        // delete actual inserted rows from table
        try {
            if (table.getMode() != Mode.LIGHT) {
                FileUtil.writeBytesIfPossible(
                    table.getTablePath(partition),
                    rowsWritten.stream()
                        .map(RowWritePackage::getRoute)
                        .map(route ->
                                 new WritePackage(
                                     route.getOffsetInTable(), Table.getWhitespaceBytes(route.getLengthInTable())
                                 )
                        ).toList()
                );
            }
            else if (rowsWritten.size() > 0) {
                // if light mode, just get all the rows out, remove those to delete, and then write them back
                List<Route> routesToDelete = rowsWritten.stream()
                    .map(RowWritePackage::getRoute)
                    .toList();

                List<Row> allRowsWithoutThoseToDelete = BaseOperationService.getAllRows(table, partition).stream()
                    .filter(row -> {
                        Route location = row.getLocationInTable();
                        // check if routesToDelete contains the row route
                        boolean containsRowRoute = routesToDelete.stream()
                            .map(route ->
                                location.getOffsetInTable().equals(route.getOffsetInTable()) && location.getLengthInTable().equals(route.getLengthInTable())
                            )
                            .reduce(Boolean::logicalOr)
                            .orElse(false);
                        return !containsRowRoute;
                    })
                    .toList();

                byte[] columnTitles = table.makeColumnTitles().getBytes(StandardCharsets.UTF_8);
                long offset = columnTitles.length;
                List<WritePackage> writePackages = new ArrayList<>(
                    List.of(
                        new WritePackage(
                            0L,
                            columnTitles
                        )
                    )
                );
                for (Row row : allRowsWithoutThoseToDelete) {
                    String line = Row.serialize(table, row.getColumnsToValues());
                    line += "\n";
                    byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                    writePackages.add(
                        new WritePackage(
                            offset,
                            bytes
                        )
                    );
                    offset += bytes.length;
                }

                FileUtil.deleteFile(table.getTablePath(partition));

                FileUtil.writeBytes(
                    table.getTablePath(partition),
                    writePackages
                );

            }

        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error deleting rows undoing insert", e);
        }



        // NOTE repartitions aren't undone. indices are just removed, and then they're left as is. index files aren't deleted
        // so count files don't need to be updated


        // add back rows that were deleted
        try {
            if (table.getMode() == Mode.LIGHT) { // if it's light mode we need to put a new line on the end
                String tablePath = table.getTablePath(partition);
                FileUtil.writeBytes(tablePath, FileUtil.fileSize(tablePath), "\n".getBytes(StandardCharsets.UTF_8));
            }
            AtomicReference<Long> offset = new AtomicReference<>(
                FileUtil.fileSize(table.getTablePath(partition))
            );
            List<WritePackage> writePackages = rows.stream()
                .map(row -> {
                    Route route = row.getLocationInTable();
                    String rowString = Row.serialize(table, row.getColumnsToValues()) + "\n";
                    byte[] bytes = rowString.getBytes(StandardCharsets.UTF_8);

                    if (table.getMode() == Mode.LIGHT) { // light mode these routes won't be valid so we need to append instead
                        route.setOffsetInTable(offset.get());
                        offset.getAndUpdate(val -> val + bytes.length);
                    }

                    return new WritePackage(
                        route.getOffsetInTable(),
                        bytes
                    );
                })
                .toList();

            FileUtil.writeBytes(
                table.getTablePath(partition),
                writePackages
            );
        } catch (IOException e) {
            throw new DavaException(ROLLBACK_ERROR, "Error adding back rows rolling back failed delete", e);
        }


        // reset table size and empties file size
        if (oldTableSize != null) {
            table.setSize(partition, oldTableSize);
        }
        if (oldEmptiesSize != null) {
            try {
                FileUtil.truncate(table.emptiesFilePath(partition), oldEmptiesSize);
            } catch (IOException e) {
                throw new DavaException(ROLLBACK_ERROR, "Error rolling back empties file after failed delete", e);
            }
        }

        // add back index routes pointing to deleted rows
        indexPathToInvalidRoutes.forEach( (indexPath, indexDelete) -> {
            try {
                Set<Route> routes = new HashSet<>(
                    BaseOperationService.getFileSizeAndRoutes(indexPath, partition, 0L, 10).getSecond()
                );
                routes.addAll(
                    indexDelete.getOriginalRoutes()
                );

                FileUtil.replaceFile(
                    indexPath,
                    routes.stream().map(route -> new WritePackage(
                        route.getOffsetInTable(),
                        route.getRouteAsBytes()
                    ))
                    .toList()
                );
            } catch (IOException e) {
                throw new DavaException(ROLLBACK_ERROR, "Error rolling back index file after failed delete: " + indexPath, e);
            }
        });

    }



    /*
        Getter setter
     */

    public EmptiesPackage getUsedTableEmtpies() {
        return usedTableEmtpies;
    }

    public void setUsedTableEmtpies(EmptiesPackage usedTableEmtpies) {
        this.usedTableEmtpies = usedTableEmtpies;
    }

    public List<RowWritePackage> getRowsWritten() {
        return rowsWritten;
    }

    public void setRowsWritten(List<RowWritePackage> rowsWritten) {
        this.rowsWritten = rowsWritten;
    }

    public Map<String, List<IndexWritePackage>> getIndexPathToIndicesWritten() {
        return indexPathToIndicesWritten;
    }

    public void setIndexPathToIndicesWritten(Map<String, List<IndexWritePackage>> indexPathToIndicesWritten) {
        this.indexPathToIndicesWritten = indexPathToIndicesWritten;
    }

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

    public Map<String, IndexDelete> getIndexPathToInvalidRoutes() {
        return indexPathToInvalidRoutes;
    }

    public void setIndexPathToInvalidRoutes(Map<String, IndexDelete> indexPathToInvalidRoutes) {
        this.indexPathToInvalidRoutes = indexPathToInvalidRoutes;
    }

    public Map<String, CountChange> getNumericCountFileChanges() {
        return numericCountFileChanges;
    }

    public void setNumericCountFileChanges(Map<String, CountChange> numericCountFileChanges) {
        this.numericCountFileChanges = numericCountFileChanges;
    }

}
