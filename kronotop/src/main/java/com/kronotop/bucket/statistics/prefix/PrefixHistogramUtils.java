/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.bucket.statistics.prefix;

import java.util.Arrays;

/**
 * Helper utility functions for Fixed N-Byte Prefix Histogram operations.
 */
public class PrefixHistogramUtils {

    /**
     * Right-pads a byte array to ensure it has at least N bytes.
     * If the array is shorter than N, pads with 0x00 bytes.
     */
    public static byte[] rightPad(byte[] bytes, int N, byte padByte) {
        if (bytes == null) {
            bytes = new byte[0];
        }
        if (bytes.length >= N) {
            return bytes;
        }
        
        byte[] padded = new byte[N];
        System.arraycopy(bytes, 0, padded, 0, bytes.length);
        Arrays.fill(padded, bytes.length, N, padByte);
        return padded;
    }

    /**
     * Extracts the first N bytes from a byte array as the prefix.
     * If the array is shorter than N, right-pads with 0x00.
     */
    public static byte[] pN(byte[] bytes, int N) {
        byte[] padded = rightPad(bytes, N, (byte) 0x00);
        byte[] result = new byte[N];
        System.arraycopy(padded, 0, result, 0, N);
        return result;
    }

    /**
     * Gets the (N+1)th byte from a byte array, or 0x00 if not present.
     * Used for fractioning in range estimation.
     */
    public static int byteAtOr0(byte[] bytes, int index) {
        if (bytes == null || index >= bytes.length) {
            return 0;
        }
        return bytes[index] & 0xFF;  // Convert to unsigned int
    }

    /**
     * Creates lexicographic successor by appending 0xFF bytes until we get a "next" value.
     * Used for half-open range bounds like [v, v_next).
     */
    public static byte[] lexSuccessor(byte[] bytes) {
        if (bytes == null) {
            return new byte[]{0x00};
        }
        
        // Try to increment the last byte
        for (int i = bytes.length - 1; i >= 0; i--) {
            if ((bytes[i] & 0xFF) < 255) {
                byte[] result = bytes.clone();
                result[i]++;
                return result;
            }
        }
        
        // All bytes are 0xFF - append a 0x00 byte
        byte[] result = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, result, 0, bytes.length);
        result[bytes.length] = 0x00;
        return result;
    }

    /**
     * Creates a byte array padded with 0xFF bytes to the specified length.
     * Used for creating upper bounds in range queries.
     */
    public static byte[] padFF(byte[] bytes, int length) {
        if (bytes == null) {
            bytes = new byte[0];
        }
        if (bytes.length >= length) {
            return Arrays.copyOf(bytes, length);
        }
        
        byte[] padded = new byte[length];
        System.arraycopy(bytes, 0, padded, 0, bytes.length);
        Arrays.fill(padded, bytes.length, length, (byte) 0xFF);
        return padded;
    }

    /**
     * Computes the fractional contribution from the (N+1)th byte for left edge of range [A, B).
     * Returns (256 - byteValue) / 256.0
     */
    public static double fracLeft(byte[] A, int N) {
        int aNextByte = byteAtOr0(A, N);
        return (256.0 - aNextByte) / 256.0;
    }

    /**
     * Computes the fractional contribution from the (N+1)th byte for right edge of range [A, B).
     * Returns byteValue / 256.0
     */
    public static double fracRight(byte[] B, int N) {
        int bNextByte = byteAtOr0(B, N);
        return bNextByte / 256.0;
    }

    /**
     * Clamps a value between 0.0 and 1.0
     */
    public static double clamp01(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    /**
     * Checks if two byte arrays are equal (null-safe)
     */
    public static boolean bytesEqual(byte[] a, byte[] b) {
        return Arrays.equals(a, b);
    }

    /**
     * Converts a byte array to a hex string for debugging
     */
    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}