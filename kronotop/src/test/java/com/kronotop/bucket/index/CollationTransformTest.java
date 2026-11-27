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

import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CollationTransformTest {
    private static final Collation COLLATION = Collation.create("en", 1, null, null,
            null, null, null, null, null);

    private final CollatorCache cache = new CollatorCache();

    @Test
    void shouldSortStringsByCollationOrder() {
        // Behavior: An English collation puts "apple" before "Banana", although plain byte order puts the capital
        // "B" first. The bytes the transform returns for "apple" sort before the bytes it returns for "Banana".
        byte[] apple = (byte[]) CollationTransform.apply("apple", COLLATION, cache);
        byte[] banana = (byte[]) CollationTransform.apply("Banana", COLLATION, cache);

        assertTrue(ByteArrayUtil.compareUnsigned(apple, banana) < 0);
    }

    @Test
    void shouldReturnSameBytesForStringsEqualUnderCollation() {
        // Behavior: An English collation at strength 1 ignores case, so "kronotop" and "KRONOTOP" are equal. The
        // transform returns the same bytes for both.
        byte[] lower = (byte[]) CollationTransform.apply("kronotop", COLLATION, cache);
        byte[] upper = (byte[]) CollationTransform.apply("KRONOTOP", COLLATION, cache);

        assertArrayEquals(lower, upper);
    }

    @Test
    void shouldReturnGivenValueWhenCollationIsNull() {
        // Behavior: Without a collation, the transform returns the string "kronotop" unchanged.
        assertEquals("kronotop", CollationTransform.apply("kronotop", null, cache));
    }

    @Test
    void shouldReturnGivenValueWhenValueIsNotString() {
        // Behavior: Collation applies only to strings, so the transform returns the integer 1988 and a Java null
        // unchanged.
        assertEquals(1988, CollationTransform.apply(1988, COLLATION, cache));
        assertNull(CollationTransform.apply(null, COLLATION, cache));
    }
}
