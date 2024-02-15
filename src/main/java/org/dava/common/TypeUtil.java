package org.dava.common;

import java.time.temporal.Temporal;

public class TypeUtil {


    public static boolean isBasicJavaType(Class<?> type) {
        return isNumericClass(type) || Temporal.class.isAssignableFrom(type) || type == String.class;
    }

    public static boolean isNumericClass(Class<?> type) {
        return Number.class.isAssignableFrom(type) || isPrimitiveNumericClass(type);
    }

    public static boolean isPrimitiveNumericClass(Class<?> type) {
        return type == int.class || type == long.class || type == double.class
            || type == float.class || type == short.class || type == byte.class;
    }

}
