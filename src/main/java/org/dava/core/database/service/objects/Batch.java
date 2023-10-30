package org.dava.core.database.service.objects;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Batch {

    private EmptiesPackage tableEmpties;

    private Map<String, List<IndexWritePackage>> indexWriteGroups;

    private Map<String, EmptiesPackage> indexEmpties;


    public Batch() {
        this.indexWriteGroups = new HashMap<>();
        this.indexEmpties = new HashMap<>();
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

    public EmptiesPackage getEmptiesPackage(String indexPath) {
        if (indexEmpties.containsKey(indexPath)) {
            return indexEmpties.get(indexPath);
        }
        return null;
    }

    public String makeRollbackString() {
        StringBuilder builder = new StringBuilder();

        // for rolling back rows added to the table
        // and new routes added to each index
        indexWriteGroups.forEach( (indexPath, groups) -> {
            builder.append("I:").append(indexPath);
            groups.forEach(writePackage ->
                builder.append("R:")
                    .append(indexPath)
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
                    .append(empty.getRoute().getOffsetInTable())
                    .append(",")
                    .append(empty.getRoute().getLengthInTable())
                    .append(";")
                    .append("\n")
            // use the route to just add empties back to empties file (empties in table are whitespaced out in previous step)
        ));

        // for rolling back used index empties
        indexEmpties.forEach( (indexPath, emptiesPackage) -> {
            builder.append("IE:").append(indexPath);
            emptiesPackage.getUsedEmpties().forEach( (length, empties) ->
                empties.forEach( empty ->
                    builder.append("E:")
                        .append(empty.getRoute().getOffsetInTable())
                        .append(",")
                        .append(empty.getRoute().getLengthInTable())
                        .append(";")
                )
            );
            builder.append("\n");
            // add each route above to the respective index empties file
        });

        return builder.toString();
    }



    /*
        Getter setter
     */

    public Map<String, List<IndexWritePackage>> getIndexWriteGroups() {
        return indexWriteGroups;
    }

    public Map<String, EmptiesPackage> getIndexEmpties() {
        return indexEmpties;
    }

    public void setIndexEmpties(Map<String, EmptiesPackage> indexEmpties) {
        this.indexEmpties = indexEmpties;
    }

    public void setTableEmpties(EmptiesPackage tableEmpties) {
        this.tableEmpties = tableEmpties;
    }
}
