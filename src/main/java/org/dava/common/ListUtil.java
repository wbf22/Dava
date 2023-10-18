package org.dava.common;


import java.util.List;

public class ListUtil {


    public static <T> List<T> limit(List<T> list, long size) {
        if (list.size() <= size)
            return list;

        size = Math.min(size, Integer.MAX_VALUE);

        return list.subList(0, Math.toIntExact(size));
    }
}
