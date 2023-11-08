package org.dava.core.database.service.objects.delete;

import java.util.ArrayList;
import java.util.List;

public class IndexDelete {
    private List<Long> indicesToDelete;


    public IndexDelete(List<Long> indicesToDelete) {
        this.indicesToDelete = new ArrayList<>(indicesToDelete);
    }


    public void addRoutesToDelete(List<Long> routeNumbers) {
        indicesToDelete.addAll(routeNumbers);
    }

    public List<Long> getRoutesToDelete() {
        return indicesToDelete;
    }

}
