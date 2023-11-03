package org.dava.core.database.service.objects.delete;

import org.dava.core.database.objects.database.structure.IndexRoute;

import java.util.List;
import java.util.Map;

public class DeleteBatch {

    public List<IndexRoute> rowLocationsInTable;

    public Map<String, IndexDelete> indicesToRemove;

    public List<String> deletedIndices;

    public Map<String, Integer> numericCountFileChanges;

    public Map<String, Integer> emptiesFileOldFileLengths;

    public long oldTableSize;


    /*
        getter setter
     */

    public List<IndexRoute> getRowLocationsInTable() {
        return rowLocationsInTable;
    }

    public void setRowLocationsInTable(List<IndexRoute> rowLocationsInTable) {
        this.rowLocationsInTable = rowLocationsInTable;
    }

    public Map<String, IndexDelete> getIndicesToRemove() {
        return indicesToRemove;
    }

    public void setIndicesToRemove(Map<String, IndexDelete> indicesToRemove) {
        this.indicesToRemove = indicesToRemove;
    }

    public List<String> getDeletedIndices() {
        return deletedIndices;
    }

    public void setDeletedIndices(List<String> deletedIndices) {
        this.deletedIndices = deletedIndices;
    }

    public Map<String, Integer> getNumericCountFileChanges() {
        return numericCountFileChanges;
    }

    public void setNumericCountFileChanges(Map<String, Integer> numericCountFileChanges) {
        this.numericCountFileChanges = numericCountFileChanges;
    }

    public Map<String, Integer> getEmptiesFileOldFileLengths() {
        return emptiesFileOldFileLengths;
    }

    public void setEmptiesFileOldFileLengths(Map<String, Integer> emptiesFileOldFileLengths) {
        this.emptiesFileOldFileLengths = emptiesFileOldFileLengths;
    }

    public long getOldTableSize() {
        return oldTableSize;
    }

    public void setOldTableSize(long oldTableSize) {
        this.oldTableSize = oldTableSize;
    }
}
