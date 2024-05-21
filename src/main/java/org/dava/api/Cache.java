package org.dava.api;

import org.dava.core.cache.CacheRepository;
import org.dava.core.cache.InMemoryCache;
import org.dava.core.cache.PersistedCache;
import org.dava.core.database.service.structure.Database;

/**
 * Efficient Cache which can store key and value pairs.
 * Useful for Rest-API's, allowing you to store the result 
 * of a query to speed up repetive requests.
 * 
 * <p> By default this an in memory cache, meaning when your
 * app is terminated the cache will be lost. You can choose to 
 * make this a persisted cache if you'd rather it be preserved upon
 * termination. By default it persists the cache values on the same
 * server/device you run your app on. This allows quick
 * cache retrievals, though the cache won't be shared between
 * instances. If you'd like to have a shared cache between instances
 * you'll have to make aanother server that maintains the shared cache.
 * 
 */
public interface Cache {
    

    public static Cache build() {
        return new InMemoryCache();
    }

    public static Cache build(String folderCachePersistsToo) {
        return new PersistedCache(folderCachePersistsToo);
    }


    void put(String key, String value);

    String get(String key);

    void clear();

    void clear(String key);

}
