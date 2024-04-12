package org.dava.api;

import io.hypersistence.tsid.TSID;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class DavaTSID {
    public static String generateId(String tableName) {
        return tableName + "_" + TSID.Factory.getTsid();
    }

    public static String generateId(String tableName, String seed) {
        return tableName + "_" + UUID.nameUUIDFromBytes(seed.getBytes()).toString().substring(0, 7).toUpperCase();
    }
}
