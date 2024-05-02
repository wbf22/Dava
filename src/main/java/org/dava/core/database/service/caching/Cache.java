package org.dava.core.database.service.caching;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * Class for maintaining a cache of time sensitive operations.
 */
public class Cache {

    private static final int MAX_CACHE_SIZE = 1000000;
    private static final int MAX_RESOURCE_SIZE = 1000;

    
    private final Object lock = new Object(); // this is just a semaphore that helps prevent threads from using the map below at the same time
    private Map<String, Map<String, Object>> pathCache = new HashMap<>();


    /**
     * 
     * Calls the 'resourceCall' lambda (supplier) if 'resourceName' 'operationHash' isn't in
     * the cache. Then caches the result.
     *
     * <p> For example, say you access a certain file's contents. You could provide the filepath
     * as the 'resourceName', and some hash representing the specifics of the way you read this
     * file (say for this example you read the first line of the file) for the 'operationHash'.
     * Then for the 'resourceCall' you'd provide a lambda of you calling the function that reads
     * your file.
     * 
     * <p> A resource should usually be something like a file or maybe a table in the database.
     * Having a resourceName and the operationHash, allows you to cache results for specific 
     * operations on a resource (read first line, read all lines, etc), but then invalidate the cache 
     * for an entire resource when and edit is performed. 
     * 
     * <p> Without tying all the operations to resources, when a resource is invalidated, you'd have to
     * invalidate each operation individually. Somehow you'd have to keep track of which operations where
     * on what resource. This way you can just invalidate the resource and all operations tied to it at once.
     *
     *
     * @param resourceName name for this resource in the cache. You'll use this when you call 'write'
     *                     to invalidate the cache for this resource
     * @param operationHash name for whatever you're doing with the resource. The same resource could
     *                      have multiple operations performed on it but be invalidated by the same write.
     *                      This allows all operations tied to a resource to be invalidated when a write is
     *                      performed on the resource.
     * @param resourceCall the lambda function that does the actual work on the resource
     * @return the result of the lambda function or whatever was in the cache.
     */
    public <T, E extends Exception> T get(String resourceName, String operationHash, CheckedSupplier<T, E> resourceCall) throws E {

        // check if in cache
        synchronized (lock){
            Map<String, Object> resource = pathCache.get(resourceName);
            if (resource != null) {
                T storedResult = (T) resource.get(operationHash);
                if (storedResult != null)
                    return storedResult;
            }
        }
        
        // call the provided lambda if no entry was in the cache
        T result = resourceCall.get();

        // make a resource if needed, and put the value in the resource
        synchronized (lock){ 
            Map<String, Object> resource = pathCache.get(resourceName);
            if (resource != null) {
                resource.put(operationHash, result);
                // handle cache getting too big
                if (resource.size() > MAX_RESOURCE_SIZE) invalidate(resourceName);
            }
            else {
                Map<String, Object> resourceMap = new HashMap<>();
                resourceMap.put(operationHash, result);
                pathCache.put(resourceName, resourceMap);
            }
        }

        // handle cache getting too big
        if (pathCache.size() > MAX_CACHE_SIZE) {
            invalidateCacheAll();
        }

        return result;
    }

    /**
     * Invalidates the cache for 'resourceName'.
     * @param resourceName name for a resource in the cache that needs to be invalidated when this logic is
     *                     run.
     */
    public void invalidate(String resourceName) {
        synchronized (lock){
            pathCache.remove(resourceName);
        }
    }

    /**
     * resets this cache, invalidating all resources
     */
    public void invalidateCacheAll() {
        synchronized (lock){
            pathCache.clear();
        }
    }

    /**
     * Utility functions for creating a hash from a list of method params. Good for use in Cache.read()
     * call.
     * 
     * After testing this function is slower than just concatenating strings, but it almost always won't
     * have significant overhead unless called repeatedly. (sub 1ms)
     */
    public static String hash(Object operationName) {
        StringBuilder stringBuilder = new StringBuilder(operationName.toString());
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }

    public static <T> String hashList(List<T> paramList) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object param : paramList) {
            stringBuilder.append(param);
        }
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }


    public static <T> String hashMap(Map<T, T> paramMap) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Entry<T, T> entry : paramMap.entrySet()) {
            stringBuilder
                .append(entry.getKey())
                .append(entry.getValue());
        }
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }

    public static String hash(Object operationName, Object param1) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(operationName);
        stringBuilder.append(param1);
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }


    public static String hash(Object operationName, Object param1, Object param2) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(operationName);
        stringBuilder.append(param1);
        stringBuilder.append(param2);
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }


    public static String hash(Object operationName, Object param1, Object param2, Object param3) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(operationName);
        stringBuilder.append(param1);
        stringBuilder.append(param2);
        stringBuilder.append(param3);
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }

}
