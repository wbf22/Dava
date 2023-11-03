package org.dava.core.database.service.objects.delete;

import java.util.List;

public class IndexDelete {
    private List<Integer> indicesToDelete;
    private boolean isIndexEmpty;


    public IndexDelete(List<Integer> indicesToDelete, boolean isIndexEmpty) {
        this.indicesToDelete = indicesToDelete;
        this.isIndexEmpty = isIndexEmpty;
    }

    public List<Integer> getIndicesToDelete() {
        return indicesToDelete;
    }

    public boolean isIndexEmpty() {
        return isIndexEmpty;
    }
}
