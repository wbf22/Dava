package org.dava.core.database.service.objects;

import org.dava.core.database.objects.database.structure.Route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EmptiesPackage {

    private Map<Integer, List<Route>> remainingEmpties;
    private Map<Integer, List<Route>> usedEmpties;
    private List<Route> rollbackEmpties;



    /**
     * Object that contains a map of empty rows which can be
     * queried for by row length
     */
    public EmptiesPackage() {
        remainingEmpties = new HashMap<>();
        usedEmpties = new HashMap<>();
    }

    public void addEmpty(Route empty) {
        if(remainingEmpties.containsKey(empty.getLengthInTable())) {
            remainingEmpties.get(empty.getLengthInTable()).add(empty);
        }
        else {
            remainingEmpties.put(empty.getLengthInTable(), new ArrayList<>( List.of(empty) ));
        }
    }

    public boolean contains(int desiredLength) {
        return remainingEmpties.containsKey(desiredLength);
    }

    public Route getEmptyRemember(int desiredLength) {
        if (remainingEmpties.containsKey(desiredLength)) {
            List<Route> empties = remainingEmpties.get(desiredLength);
            Route empty = empties.get(0);
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

    public Map<Integer, List<Route>> getUsedEmpties() {
        return usedEmpties;
    }

    public List<Route> getRollbackEmpties() {
        return rollbackEmpties;
    }

    public void setRollbackEmpties(List<Route> rollbackEmpties) {
        this.rollbackEmpties = rollbackEmpties;
    }

    public String getRemaingingEmptiesString() {
        return remainingEmpties.values().stream()
            .map( empties ->
                empties.stream().map(empty ->
                     empty.getOffsetInTable() + ":" + empty.getLengthInTable()
                )
                .collect(Collectors.joining(";"))
            )
            .collect(Collectors.joining("\n"));
    }
}
