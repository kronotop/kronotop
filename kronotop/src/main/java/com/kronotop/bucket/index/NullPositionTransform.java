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

package com.kronotop.bucket.index;

import java.util.Arrays;

public class NullPositionTransform {
    private static final byte[] NULL = new byte[]{0x00};
    private static final byte[] NULL_LAST = new byte[]{(byte) 0xFE};
    private static final byte[] DESC_NULL = new byte[]{(byte) 0xFF, (byte) 0xFF};
    private static final byte[] DESC_NULL_FIRST = new byte[]{(byte) 0x01, (byte) 0xFF};

    public static byte[] apply(byte[] value, SortDirection direction, NullPosition position) {
        if (Arrays.equals(value, NULL) && direction == SortDirection.ASC && position == NullPosition.LAST) {
            return NULL_LAST;
        }

        if (Arrays.equals(value, DESC_NULL) && direction == SortDirection.DESC && position == NullPosition.FIRST) {
            return DESC_NULL_FIRST;
        }

        return value;
    }

    public static byte[] revert(byte[] value, SortDirection direction, NullPosition position) {
        if (Arrays.equals(value, NULL_LAST) && direction == SortDirection.ASC && position == NullPosition.LAST) {
            return NULL;
        }

        if (Arrays.equals(value, DESC_NULL_FIRST) && direction == SortDirection.DESC && position == NullPosition.FIRST) {
            return DESC_NULL;
        }

        return value;
    }
}