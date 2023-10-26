package org.dava.common;

import java.util.Arrays;

public class ArrayUtil {


    public static <T> T[] appendArray(T[] array1, T[] array2) {
        T[] result = Arrays.copyOf(array1, array1.length + array2.length);
        System.arraycopy(array2, 0, result, array1.length, array2.length);
        return result;
    }

    public static byte[] appendArray(byte[] array1, byte[] array2) {
        byte[] result = Arrays.copyOf(array1, array1.length + array2.length);
        System.arraycopy(array2, 0, result, array1.length, array2.length);
        return result;
    }

    public static <T> T[] subRange(T[] array, int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > array.length || startIndex > endIndex) {
            throw new IllegalArgumentException("Invalid sub-range parameters.");
        }

        return Arrays.copyOfRange(array, startIndex, endIndex);
    }

    public static byte[] subRange(byte[] array, int startIndex, int endIndex) {
        if (startIndex < 0 || endIndex > array.length || startIndex > endIndex) {
            throw new IllegalArgumentException("Invalid sub-range parameters.");
        }

        return Arrays.copyOfRange(array, startIndex, endIndex);
    }

}
