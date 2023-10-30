package org.dava.core.database.service.objects;

import org.dava.core.database.objects.database.structure.IndexRoute;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RollbackRecord {
    private List<RowWritePackage> rowWrites;
    private Map<String, List<IndexWritePackage>> indexWrites;
    private List<IndexRoute> poppedEmpties;

    public RollbackRecord(List<RowWritePackage> rowWrites, Map<String, List<IndexWritePackage>> indexWrites, List<IndexRoute> poppedEmpties) {
        this.rowWrites = rowWrites;
        this.indexWrites = indexWrites;
        this.poppedEmpties = poppedEmpties;
    }


    /*
        Getter Setter
     */



    @Override
    public String toString() {
        String rowWritesData = rowWrites.stream()
            .map( rowWritePackage ->
                      rowWritePackage.getRoute().getOffsetInTable() + "," + rowWritePackage.getRoute().getLengthInTable() + ";"
            )
            .collect(Collectors.joining());

        //TODO handle where index empties have been replaced
        String indexWritesData = indexWrites.values().stream()
            .map( indexWritePackages ->
                indexWritePackages.get(0).getFolderPath() + "," + indexWritePackages.size() + ";"
            )
            .collect(Collectors.joining());

        String emptiesData = poppedEmpties.stream()
            .map( emptyRoute ->
                      emptyRoute.getOffsetInTable() + "," + emptyRoute.getLengthInTable() + ";"
            )
            .collect(Collectors.joining());

        return "RowWrites:" + rowWritesData + "IndexWrites:" + indexWritesData + "Empties:" + emptiesData;
    }
}
