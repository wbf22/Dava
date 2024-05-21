package org.dava.core.cache;

import java.util.List;

import org.dava.api.Cache;
import org.dava.core.database.objects.exception.DavaException;
import org.dava.core.database.objects.exception.ExceptionType;
import org.dava.core.database.service.structure.Database;
import org.dava.core.database.service.structure.Mode;


public class PersistedCache implements Cache {

    private CacheRepository repository;

    public PersistedCache(String databaseRoot) {
        Database database = new Database(databaseRoot, List.of(CacheValue.class), List.of(Mode.MANUAL));
        this.repository = new CacheRepository(database);
    }

    @Override
    public void put(String key, String value) {
        try {
            repository.save(new CacheValue(key, value));
        } catch(Exception e) {
            throw new DavaException(ExceptionType.CACHE_ERROR, "Error saving cache value", e);
        }
    }

    @Override
    public String get(String key) {
        try {
            return repository.findById(key).getValue();
        } catch(Exception e) {
            throw new DavaException(ExceptionType.CACHE_ERROR, "Error retrieving cache value", e);
        }
    }

    @Override
    public void clear() {
        try {
            List<String> ids = repository.findAll().stream()
                .map(CacheValue::getKey)
                .toList();
            
            repository.deleteAll(ids, false);
        } catch(Exception e) {
            throw new DavaException(ExceptionType.CACHE_ERROR, "Error clearing cache", e);
        }
    }

    @Override
    public void clear(String key) {
        try {
            repository.delete(key, false);
        } catch(Exception e) {
            throw new DavaException(ExceptionType.CACHE_ERROR, "Error clearing cache value", e);
        }
    }
    
}
