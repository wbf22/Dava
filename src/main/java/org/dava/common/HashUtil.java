package org.dava.common;

import java.util.UUID;

public class HashUtil {
    public static long hashString(byte[] value) {
        long h = 0;

        for (byte b : value) {
            h = (h << 5) - h + (b & 0xFF);
        }

        return h;
    }

    public static String hashToUUID(byte[] bytes) {
        return UUID.nameUUIDFromBytes(bytes).toString();
    }
}
