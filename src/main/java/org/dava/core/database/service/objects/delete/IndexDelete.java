package org.dava.core.database.service.objects.delete;

import org.dava.core.database.objects.database.structure.Route;

import java.util.ArrayList;
import java.util.List;

public class IndexDelete {
    private List<Long> indicesToDelete;

    private List<Route> originalRoutes;

    public IndexDelete(List<Long> indicesToDelete, List<Route> originalRoutes) {
        this.indicesToDelete = new ArrayList<>(indicesToDelete);
        this.originalRoutes = originalRoutes;
    }

    public void addRoutesToDelete(List<Long> routeNumbers) {
        indicesToDelete.addAll(routeNumbers);
    }

    public List<Long> getIndicesToDelete() {
        return indicesToDelete;
    }

    public List<Route> getOriginalRoutes() {
        return originalRoutes;
    }
}
