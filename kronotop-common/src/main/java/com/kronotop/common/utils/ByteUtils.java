package com.kronotop.common.utils;

public class ByteUtils {
    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

    public static byte[] fromLong(Long data) {
        if (data == null) {
            return null;
        }

        return new byte[]{
                (byte) (data >>> 56),
                (byte) (data >>> 48),
                (byte) (data >>> 40),
                (byte) (data >>> 32),
                (byte) (data >>> 24),
                (byte) (data >>> 16),
                (byte) (data >>> 8),
                data.byteValue()
        };
    }

    public static Long toLong(byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length != 8) {
            throw new IllegalArgumentException("Size of data is not 8");
        }

        long value = 0;
        for (byte b : data) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    public static String toHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }
}