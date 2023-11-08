package org.dava.core.database.service.objects.insert;

import org.dava.core.database.service.objects.EmptiesPackage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Batch {

    private EmptiesPackage tableEmpties;
    private List<RowWritePackage> rowWritePackages;
    private Map<String, List<IndexWritePackage>> indexWriteGroups;
    private List<String> numericRepartitions;


    public Batch() {
        this.indexWriteGroups = new HashMap<>();
        this.numericRepartitions = new ArrayList<>();
        this.tableEmpties = new EmptiesPackage();
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
                    .append(empty.getRoute().getOffsetInTable())
                    .append(",")
                    .append(empty.getRoute().getLengthInTable())
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



    /*
        Getter setter
     */

    public EmptiesPackage getTableEmpties() {
        return tableEmpties;
    }

    public List<RowWritePackage> getRowWritePackages() {
        return rowWritePackages;
    }

    public void setRowWritePackages(List<RowWritePackage> rowWritePackages) {
        this.rowWritePackages = rowWritePackages;
    }

    public Map<String, List<IndexWritePackage>> getIndexWriteGroups() {
        return indexWriteGroups;
    }


    public void setTableEmpties(EmptiesPackage tableEmpties) {
        this.tableEmpties = tableEmpties;
    }

    public List<String> getNumericRepartitions() {
        return numericRepartitions;
    }

    public void setNumericRepartitions(List<String> numericRepartitions) {
        this.numericRepartitions = numericRepartitions;
    }
}
