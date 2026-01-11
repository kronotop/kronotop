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
}
