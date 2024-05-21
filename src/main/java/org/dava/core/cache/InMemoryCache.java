package org.dava.core.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dava.api.Cache;

public class InMemoryCache implements Cache {

    private Map<String, String> map = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        map.put(key, value);
    }

    @Override
    public String get(String key) {
        return map.get(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public void clear(String key) {
        map.remove(key);
    }
    
}
