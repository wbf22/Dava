package org.dava.core.database.service.fileaccess;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class for maintaining a cache of time sensitive operations.
 */
public class Cache {

    private Map<String, Map<String, Object>> pathCache = new ConcurrentHashMap<>();


    /**
     * <pre>
     * Calls the 'resourceCall' lambda (supplier) if 'resourceName' 'operationHash' isn't in
     * the cache. Then caches the result.
     *
     * For example, say you access a certain file's contents. You could provide the filepath
     * as the 'resourceName', and some hash representing the specifics of the way you read this
     * file (say for this example you read the first line of the file) for the 'operationHash'.
     * Then for the 'resourceCall' you'd provide a lambda of you calling the function that reads
     * your file.
     *
     *
     * </pre>
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

        // TODO handle cache getting too big
        if (pathCache.containsKey(resourceName)) {
            Map<String, Object> resource = pathCache.get(resourceName);
            if (resource.containsKey(operationHash))
                return (T) resource.get(operationHash);
        }

        T result = resourceCall.get();

        if (pathCache.containsKey(resourceName))
            pathCache.get(resourceName).put(operationHash, result);
        else
            pathCache.put(resourceName, new HashMap<>(Map.of(operationHash, result)));

        return result;
    }

    /**
     * Invalidates the cache for 'resourceName'.
     * @param resourceName name for a resource in the cache that needs to be invalidated when this logic is
     *                     run.
     */
    public void invalidate(String resourceName) {
        pathCache.remove(resourceName);
    }

    /**
     * resets this cache, invalidating all resources
     */
    public void invalidateCache() {
        pathCache = new ConcurrentHashMap<>();
    }

    /**
     * Utility functions for creating a hash from a list of method params. Good for use in Cache.read()
     * call.
     */


    public static String hash(Object methodParamInCall) {
        StringBuilder stringBuilder = new StringBuilder(methodParamInCall.toString());
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }


    public static String hash(Object param1, Object param2) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(param1);
        stringBuilder.append(param2);
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }


    public static String hash(Object param1, Object param2, Object param3) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(param1);
        stringBuilder.append(param2);
        stringBuilder.append(param3);
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }


    public static String hash(Object param1, Object param2, Object param3, Object param4) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(param1);
        stringBuilder.append(param2);
        stringBuilder.append(param3);
        stringBuilder.append(param4);
        return UUID.nameUUIDFromBytes(
            stringBuilder.toString()
                .getBytes()
        ).toString();
    }

}
