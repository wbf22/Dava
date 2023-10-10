package org.dava.common;


import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Checks {

    private static final String CAST_FAIL = "Failed to Cast ";

    private static final Logger log = Logger.getLogger(Checks.class.getName());

    private Checks() {}


    /**
     * Tries to cast the object to the desired type. Throwing an exception if not possible.
     *
     * @param obj object to cast
     * @param desiredType type to cast too
     * @return cast object of type T
     * @param <T> type
     */
    public static <T> T safeCast(Object obj, Class<T> desiredType) {
        if (desiredType.isInstance(obj)) {
            return desiredType.cast(obj);
        }
        String message = CAST_FAIL + obj.getClass().getName() + " to " + desiredType.getName();
        log.log(Level.SEVERE, message);
        throw new CheckException(message, null);
    }

    /**
     * Tries to cast the map to the desired type. Throwing an exception if not possible.
     *
     * @param obj map to cast
     * @param keyType type to cast keys too
     * @param valueType type to cast values too
     * @return cast map of type Map<T,S>
     * @param <T> type
     * @param <S> type
     */
    public static <T, S> Map<T,S> safeCastMap(Object obj, Class<T> keyType, Class<S> valueType) {
        try {
            Map<T, S> newMap = new HashMap<>();
            for(Object entry : safeCast(obj, Map.class).entrySet()) {
                if (entry instanceof Map.Entry<?, ?> newEntry) {
                    newMap.put(
                        safeCast(newEntry.getKey(), keyType),
                        safeCast(newEntry.getValue(), valueType)
                    );
                }
            }
            return newMap;
        } catch(CheckException e){
            String message = CAST_FAIL + obj.getClass().getName()
                + " to map of " + keyType.getName() + ", " + valueType.getName();
            log.log(Level.SEVERE, message);
            throw new CheckException(message, null);
        }
    }

    /**
     * Tries to cast the list to the desired type. Throwing an exception if not possible.
     *
     * @param list list to cast
     * @param type type to cast keys too
     * @return cast list of type List<T>
     * @param <T> type
     */
    public static <T> List<T> safeCastList(List<?> list, Class<T> type) {
        try {
            List<T> newList = new ArrayList<>();
            if (list == null) {
                return newList;
            }
            for(Object item : list) {
                newList.add(
                    safeCast(item, type)
                );
            }
            return newList;
        } catch(CheckException e) {
            String message = CAST_FAIL + list.getClass().getName()
                + " to list of " + type.getName();
            log.log(Level.SEVERE, message);
            throw new CheckException(message, e);
        }

    }

    /**
     * Get's a key from the map and safe casts it to the desired type.
     *
     * @param map provided map
     * @param key key to extract
     * @param desiredType the type to safe cast the value too
     * @return a T object
     * @param <T> type
     */
    public static <T> T mapGet(Map<String, Object> map, String key, Class<T> desiredType) {
        try {
            Object obj = map.get(key);
            return mapIfNotNull(obj, value -> safeCast(obj, desiredType));
        } catch(CheckException e) {
            String message = "Failed to get map value or cast map value for key "
                + key + " to type " + desiredType.getName();
            log.log(Level.SEVERE, message);
            throw new CheckException(message, e);
        }
    }

    /**
     * Get's a key from the map and safe casts it to the desired map type.
     *
     * @param map parent map
     * @param key key to child map
     * @param keyType type to cast keys too
     * @param valueType type to cast values too
     * @return cast map of type Map<T,S>
     * @param <T> type
     * @param <S> type
     */
    public static <T, S> Map<T,S> mapGetChildMap(Map<String, Object> map, String key, Class<T> keyType, Class<S> valueType) {
        try {
            Object obj = map.get(key);
            return mapIfNotNull(obj, value -> safeCastMap(obj, keyType, valueType));
        } catch(CheckException e) {
            String message = "Failed to get child map for key " + key
                + " and cast to map of " + keyType.getName() + ", " + valueType.getName();
            log.log(Level.SEVERE, message);
            throw new CheckException(message, null);
        }
    }

    /**
     * Get's a key from the map and safe casts it to the desired type.
     *
     * @param map provided map
     * @param key key to extract
     * @param desiredType the type to safe cast the value too
     * @return a T object
     * @param <T> type
     */
    public static <T> List<T> mapGetList(Map<String, Object> map, String key, Class<T> desiredType) {
        List<?> list = mapGet(map, key, List.class);
        List<Object> objects = new ArrayList<>(list);
        return objects.stream()
            .map(obj -> safeCast(obj, desiredType) )
            .toList();

    }

    /**
     * Helps replace Optional.of(obj).map(i->i.someFunction()).orElse(null);
     * Provide a lambda and the object and the function will do the null check and
     * run the lambda.
     *
     * @param value possibly null object
     * @param lambda lambda function
     * @return a R object
     * @param <T> incoming type
     * @param <R> output of lambda type or null if value is null
     */
    public static <T, R> R mapIfNotNull(T value, Function<T, R> lambda) {
        return mapIfNotNull(value, lambda, null);
    }

    /**
     * Helps replace Optional.of(obj).map(i->i.someFunction()).orElse(null);
     * Provide a lambda and the object and the function will do the null check and
     * run the lambda.
     *
     * @param value possibly null object
     * @param lambda lambda function
     * @param orElse what to return if object is null
     * @return a R object
     * @param <T> incoming type
     * @param <R> output of lambda type
     */
    public static <T, R> R mapIfNotNull(T value, Function<T, R> lambda, R orElse) {
        try {
            return Optional.ofNullable(value).map(lambda).orElse(orElse);
        } catch(Exception e) {
            String message = "Failure in lambda for when trying to map: " + e.getMessage();
            log.log(Level.SEVERE, message);
            throw new CheckException(message, e);
        }
    }

    /**
     * Runs a function to map a value. If exception is thrown return the onFailure value
     * @param value value to map
     * @param lambda mapping function
     * @param onFailure failure value
     * @return either mapped value or onFailure
     * @param <T> input type
     * @param <R> output type
     */
    public static <T, R> R tryMap(T value, Function<T, R> lambda, R onFailure) {
        try {
            return Optional.ofNullable(value).map(lambda).orElseThrow();
        } catch(Exception e) {
            return onFailure;
        }
    }


}
