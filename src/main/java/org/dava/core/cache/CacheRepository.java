package org.dava.core.cache;

import org.dava.api.Repository;
import org.dava.core.database.service.structure.Database;

public class CacheRepository extends Repository<CacheValue, String> {

    public CacheRepository(Database database) {
        super(database);
    }


    
    
}
