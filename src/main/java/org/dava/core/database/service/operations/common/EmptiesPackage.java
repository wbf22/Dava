package org.dava.core.database.service.operations.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.dava.core.database.service.structure.Empty;
import org.dava.core.database.service.structure.Route;

public class EmptiesPackage {

    private Map<Integer, List<Empty>> remainingEmpties;
    private Map<Integer, List<Empty>> usedEmpties;
    private List<Empty> rollbackEmpties;



    /**
     * Object that contains a map of empty rows which can be
     * queried for by row length
     */
    public EmptiesPackage() {
        remainingEmpties = new HashMap<>();
        usedEmpties = new HashMap<>();
    }

    public void addEmpty(Empty empty) {
        if(remainingEmpties.containsKey(empty.getRoute().getLengthInTable())) {
            remainingEmpties.get(empty.getRoute().getLengthInTable()).add(empty);
        }
        else {
            remainingEmpties.put(empty.getRoute().getLengthInTable(), new ArrayList<>( List.of(empty) ));
        }
    }

    public boolean contains(int desiredLength) {
        return remainingEmpties.containsKey(desiredLength);
    }

    public Empty getEmptyRemember(int desiredLength) {
        if (remainingEmpties.containsKey(desiredLength)) {
            List<Empty> empties = remainingEmpties.get(desiredLength);
            Empty empty = empties.get(0);
            empties = empties.subList(1, empties.size());

            if (!empties.isEmpty()) {
                remainingEmpties.put(desiredLength, empties);
            }
            else {
                remainingEmpties.remove(desiredLength);
            }

            if(usedEmpties.containsKey(desiredLength)) {
                usedEmpties.get(desiredLength).add(empty);
            }
            else {
                usedEmpties.put(desiredLength, new ArrayList<>( List.of(empty) ));
            }
            return empty;
        }
        return null;
    }


    /*
        Getter Setter
     */

    public Map<Integer, List<Empty>> getUsedEmpties() {
        return usedEmpties;
    }

    public List<Empty> getRollbackEmpties() {
        return rollbackEmpties;
    }

    public void setRollbackEmpties(List<Empty> rollbackEmpties) {
        this.rollbackEmpties = rollbackEmpties;
    }

    public String getRemaingingEmptiesString() {
        return remainingEmpties.values().stream()
            .map( empties ->
                empties.stream().map(empty ->
                     empty.getRoute().getOffsetInTable() + ":" + empty.getRoute().getLengthInTable()
                )
                .collect(Collectors.joining(";"))
            )
            .collect(Collectors.joining("\n"));
    }
}
