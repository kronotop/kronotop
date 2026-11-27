/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.index;

import com.apple.foundationdb.tuple.ByteArrayUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SortDirectionTransformTest {

    @Test
    void shouldReverseOrderOfValuesThatDifferAtAByte() {
        // Behavior: Applies DESC to 61, 61 00, 61 00 00 and 61 01. The last one is the largest of the four,
        // and its result sorts before the other three results.
        byte[] a = (byte[]) SortDirectionTransform.apply(new byte[]{0x61}, SortDirection.DESC);
        byte[] b = (byte[]) SortDirectionTransform.apply(new byte[]{0x61, 0x00}, SortDirection.DESC);
        byte[] c = (byte[]) SortDirectionTransform.apply(new byte[]{0x61, 0x00, 0x00}, SortDirection.DESC);
        byte[] d = (byte[]) SortDirectionTransform.apply(new byte[]{0x61, 0x01}, SortDirection.DESC);

        assertTrue(ByteArrayUtil.compareUnsigned(d, c) < 0);
        assertTrue(ByteArrayUtil.compareUnsigned(d, b) < 0);
        assertTrue(ByteArrayUtil.compareUnsigned(d, a) < 0);
    }

    @Test
    void shouldReverseOrderOfValuesThatExtendEachOtherWithZeroBytes() {
        // Behavior: Applies DESC to 61, 61 00 and 61 00 00, where each value is the previous one plus a 00 byte.
        // The results sort with the longest value first: 61 00 00, 61 00, 61.
        byte[] a = (byte[]) SortDirectionTransform.apply(new byte[]{0x61}, SortDirection.DESC);
        byte[] b = (byte[]) SortDirectionTransform.apply(new byte[]{0x61, 0x00}, SortDirection.DESC);
        byte[] c = (byte[]) SortDirectionTransform.apply(new byte[]{0x61, 0x00, 0x00}, SortDirection.DESC);

        assertTrue(ByteArrayUtil.compareUnsigned(c, b) < 0);
        assertTrue(ByteArrayUtil.compareUnsigned(b, a) < 0);
    }
}
