/*
 * Copyright (c) 2023-2025 Kronotop
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

package com.kronotop.common.utils;

public class Utils {
    /**
     * Generates 32-bit murmur2 hash from byte array
     *
     * @param data byte array to hash
     * @return 32-bit hash of the given array
     */
    @SuppressWarnings("fallthrough")
    public static int murmur2(final byte[] data) {
        // Source: https://github.com/apache/kafka/blob/9aef9921184d4f8fbf2e7af4875ca922173e2a8d/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L476
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolute
     * value.
     * <p>
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition since it is used
     * in producer's partition selection logic.
     *
     * @param number a given number
     * @return a positive number.
     * <p>
     */
    public static int toPositive(int number) {
        // Source: https://github.com/apache/kafka/blob/9aef9921184d4f8fbf2e7af4875ca922173e2a8d/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L1064
        return number & 0x7fffffff;
    }

    public static boolean isEmpty(final CharSequence cs) {
        // Source: https://commons.apache.org/proper/commons-lang/javadocs/api-release/src-html/org/apache/commons/lang3/StringUtils.html#line.3583
        return cs == null || cs.length() == 0;
    }

    public static boolean isNumeric(final CharSequence cs) {
        // Source: https://commons.apache.org/proper/commons-lang/javadocs/api-release/src-html/org/apache/commons/lang3/StringUtils.html#line.3752
        if (isEmpty(cs)) {
            return false;
        }
        final int sz = cs.length();
        for (int i = 0; i < sz; i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
