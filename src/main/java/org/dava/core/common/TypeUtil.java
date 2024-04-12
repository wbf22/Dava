package org.dava.core.common;

import java.time.temporal.Temporal;

import org.dava.core.database.objects.dates.Date;

public class TypeUtil {

    public static boolean isDate(Class<?> type) {
        return Temporal.class.isAssignableFrom(type) || Date.class.isAssignableFrom(type);
    }

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
