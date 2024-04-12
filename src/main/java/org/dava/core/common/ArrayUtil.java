package org.dava.core.common;

import java.util.Arrays;
import java.util.List;

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

    public static byte[] appendArrays(List<Object> arrays, int arraySize) {
        byte[] newArray = new byte[arraySize * arrays.size()];

        int index = 0;
        for (Object array : arrays) {
            byte[] bytes = (byte[]) array;
            System.arraycopy(bytes, 0, newArray, index, bytes.length);
            index += arraySize;
        }

        return newArray;
    }


    public static byte[] appendArrays(List<Object> arrays) {
        if (arrays.isEmpty())
            return new byte[0];

        int arraySize = ((byte[]) arrays.get(0)).length;

        return appendArrays(arrays, arraySize);
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
