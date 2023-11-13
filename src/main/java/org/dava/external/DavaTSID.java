package org.dava.external;

import io.hypersistence.tsid.TSID;

public class DavaTSID {
    public static String generateId(String tableName) {
        return tableName + "_" + TSID.Factory.getTsid();
    }
}
