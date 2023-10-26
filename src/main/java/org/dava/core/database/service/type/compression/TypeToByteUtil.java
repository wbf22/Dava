package org.dava.core.database.service.type.compression;

import java.util.Arrays;

public class TypeToByteUtil {

    // Conversions
    public static byte[] intToByteArray(int value) {
        byte[] bytes = new byte[4]; // 4 bytes for an integer
        bytes[0] = (byte) (value >> 24);
        bytes[1] = (byte) (value >> 16);
        bytes[2] = (byte) (value >> 8);
        bytes[3] = (byte) value;
        return bytes;
    }

    public static int byteArrayToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
            ((bytes[1] & 0xFF) << 16) |
            ((bytes[2] & 0xFF) << 8) |
            (bytes[3] & 0xFF);
    }

    public static byte[] floatToByteArray(float value) {
        int bits = Float.floatToIntBits(value);
        return intToByteArray(bits);
    }

    public static float byteArrayToFloat(byte[] bytes) {
        int bits = byteArrayToInt(bytes);
        return Float.intBitsToFloat(bits);
    }

    public static byte[] doubleToByteArray(double value) {
        long bits = Double.doubleToLongBits(value);
        return longToByteArray(bits);
    }

    public static double byteArrayToDouble(byte[] bytes) {
        long bits = byteArrayToLong(bytes);
        return Double.longBitsToDouble(bits);
    }

    public static byte[] longToByteArray(long value) {
        byte[] bytes = new byte[8]; // 8 bytes for a long
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (value >> (56 - (i * 8)));
        }
        return bytes;
    }

    public static long byteArrayToLong(byte[] bytes) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (bytes[i] & 0xFF) << (56 - (i * 8)));
        }
        return value;
    }

    public static long[] byteArrayToLongArray(byte[] bytes) {
        long[] longs = new long[bytes.length/8];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = byteArrayToLong(
                Arrays.copyOfRange(bytes, i * 8, (i+1) * 8)
            );
        }
        return longs;
    }


    /**
     * Comma check, since we're storing in a csv which splits values by comma,
     * if the value contains a comma we want to just store the plain string
     * representation of the value instead
     */
    public static boolean containsComma(byte[] bytes) {
        for (int i = 0; i < bytes.length - 1; i++) {
            if (bytes[i] == 0x2C && bytes[i+1] == 0x00) {
                return true;
            }
        }
        return false;
    }


}
