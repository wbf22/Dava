package org.dava.core.database.service.objects;

import org.dava.core.database.objects.database.structure.Table;

public interface Batch {

    void rollback(Table<?> table, String partition);
}
