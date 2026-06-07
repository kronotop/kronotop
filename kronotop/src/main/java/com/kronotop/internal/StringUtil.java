/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.internal;

public class StringUtil {

    /**
     * Splits a string by dot delimiter without regex overhead.
     *
     * @param input the string to split
     * @return array of segments between dots
     */
    public static String[] split(String input) {
        // Fast path: no dots mean a single segment
        int firstDot = input.indexOf('.');
        if (firstDot == -1) {
            return new String[]{input};
        }

        // Count dots to a pre-size array
        int count = 1;
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '.') {
                count++;
            }
        }

        String[] segments = new String[count];
        int segmentIndex = 0;
        int start = 0;

        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '.') {
                segments[segmentIndex++] = input.substring(start, i);
                start = i + 1;
            }
        }
        segments[segmentIndex] = input.substring(start);

        return segments;
    }

    /**
     * Converts ASCII letters to uppercase, returning the original string if already uppercase.
     * Non-ASCII characters are passed through unchanged.
     *
     * @param input the string to convert
     * @return the uppercase string, or the original reference if no conversion needed
     */
    public static String toUpperCaseAscii(String input) {
        int len = input.length();
        for (int i = 0; i < len; i++) {
            char c = input.charAt(i);
            if (c >= 'a' && c <= 'z') {
                // Found lowercase, need to convert
                char[] chars = new char[len];
                // Copy already-checked prefix
                input.getChars(0, i, chars, 0);
                // Convert this char
                chars[i] = (char) (c & 0xDF);
                // Process remainder
                for (int j = i + 1; j < len; j++) {
                    char ch = input.charAt(j);
                    chars[j] = (ch >= 'a' && ch <= 'z') ? (char) (ch & 0xDF) : ch;
                }
                return new String(chars);
            }
        }
        return input; // Zero-copy: already uppercase
    }

    /**
     * Converts ASCII letters to lowercase, returning the original string if already lowercase.
     * Non-ASCII characters are passed through unchanged.
     *
     * @param input the string to convert
     * @return the lowercase string, or the original reference if no conversion needed
     */
    public static String toLowerCaseAscii(String input) {
        int len = input.length();
        for (int i = 0; i < len; i++) {
            char c = input.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                // Found uppercase, need to convert
                char[] chars = new char[len];
                input.getChars(0, i, chars, 0);
                chars[i] = (char) (c | 0x20);
                for (int j = i + 1; j < len; j++) {
                    char ch = input.charAt(j);
                    chars[j] = (ch >= 'A' && ch <= 'Z') ? (char) (ch | 0x20) : ch;
                }
                return new String(chars);
            }
        }
        return input; // Zero-copy: already lowercase
    }

    /**
     * Converts ASCII letters to uppercase in-place within a byte array.
     *
     * @param bytes  the byte array to modify
     * @param offset the starting position
     * @param length the number of bytes to process
     * @return true if any conversion was performed
     */
    public static boolean toUpperCaseAscii(byte[] bytes, int offset, int length) {
        boolean modified = false;
        int end = offset + length;
        for (int i = offset; i < end; i++) {
            byte b = bytes[i];
            if (b >= 'a' && b <= 'z') {
                bytes[i] = (byte) (b & 0xDF);
                modified = true;
            }
        }
        return modified;
    }

    /**
     * Converts ASCII letters to lowercase in-place within a byte array.
     *
     * @param bytes  the byte array to modify
     * @param offset the starting position
     * @param length the number of bytes to process
     * @return true if any conversion was performed
     */
    public static boolean toLowerCaseAscii(byte[] bytes, int offset, int length) {
        boolean modified = false;
        int end = offset + length;
        for (int i = offset; i < end; i++) {
            byte b = bytes[i];
            if (b >= 'A' && b <= 'Z') {
                bytes[i] = (byte) (b | 0x20);
                modified = true;
            }
        }
        return modified;
    }
}
