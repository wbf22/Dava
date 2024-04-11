package org.dava.external;

import java.security.SecureRandom;
import java.util.UUID;
import java.util.stream.IntStream;

public class DavaId {

    private static final int DIV_LENGTH = 8;
    
    public static String generateId(String tableName, String seed) {
        return tableName + "_" + UUID.nameUUIDFromBytes(seed.getBytes()).toString().substring(0, 7).toUpperCase();
    }


    /**
     * 
     * Creates a random id using secure random.
     * Prefixes with the provided prefix.
     * 
     * @param prefix
     * @param length
     * @return
     */
    public static String randomId(String prefix, Integer length, Integer divLength, boolean upperCase) {
        String uuid = UUID.randomUUID().toString().replace("-", "");

        uuid = upperCase ? uuid.toUpperCase() : uuid.toLowerCase();

        StringBuilder id = new StringBuilder(prefix);
        for (int i = 0; i < uuid.length(); i+=divLength) {
            id.append("-").append(uuid.substring(i, i+divLength));
        }

        length = length + prefix.length() + length/divLength;
        if (id.length() > length) return id.substring(0, length);
        return id.toString();
    }
}
