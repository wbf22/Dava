package org.dava.core.database.service.objects;

import org.dava.core.database.service.fileaccess.FileUtil;

import java.util.Map;

public class InsertData {



    private String partition;
    private String rollbackPath;
    private EmptiesPackage tableEmpties;
    private Map<String, EmptiesPackage> indexEmpties;


    public InsertData(String partition, String rollbackPath, EmptiesPackage tableEmpties, Map<String, EmptiesPackage> indexEmpties) {
        this.partition = partition;
        this.rollbackPath = rollbackPath;
        this.tableEmpties = tableEmpties;
        this.indexEmpties = indexEmpties;
    }

    public String getPartition() {
        return partition;
    }

    public String getRollbackPath() {
        return rollbackPath;
    }

    public EmptiesPackage getTableEmpties() {
        return tableEmpties;
    }

    public Map<String, EmptiesPackage> getIndexEmpties() {
        return indexEmpties;
    }
}
