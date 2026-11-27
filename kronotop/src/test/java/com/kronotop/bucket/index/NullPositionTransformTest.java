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
import com.apple.foundationdb.tuple.Tuple;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.*;

class NullPositionTransformTest {
    private static final String INDEX_TYPE_TEST_DATA =
            "com.kronotop.bucket.index.SortDirectionTransformTest#indexTypeTestData";
    private static final byte[] NULL = new byte[]{0x00};
    private static final byte[] DESC_NULL = new byte[]{(byte) 0xFF, (byte) 0xFF};

    @ParameterizedTest(name = "{0}")
    @MethodSource(INDEX_TYPE_TEST_DATA)
    void shouldSortNullAfterEveryIndexTypeWhenAscAndNullsLast(BsonType bsonType, Object smaller, Object larger) {
        // Behavior: An ascending field wants nulls last, so the transform moves the null to the end. For each
        // supported index type, the moved null sorts after a smaller and a larger value of that type.
        byte[] applied = NullPositionTransform.apply(NULL, SortDirection.ASC, NullPosition.LAST);

        assertTrue(ByteArrayUtil.compareUnsigned(applied, Tuple.from(smaller).pack()) > 0);
        assertTrue(ByteArrayUtil.compareUnsigned(applied, Tuple.from(larger).pack()) > 0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource(INDEX_TYPE_TEST_DATA)
    void shouldSortNullBeforeEveryIndexTypeWhenDescAndNullsFirst(BsonType bsonType, Object smaller, Object larger) {
        // Behavior: A descending field wants nulls first, so the transform moves the null to the front. For each
        // supported index type, the moved null sorts before a smaller and a larger value of that type.
        byte[] applied = NullPositionTransform.apply(DESC_NULL, SortDirection.DESC, NullPosition.FIRST);
        byte[] smallerApplied = (byte[]) SortDirectionTransform.apply(smaller, SortDirection.DESC);
        byte[] largerApplied = (byte[]) SortDirectionTransform.apply(larger, SortDirection.DESC);

        assertTrue(ByteArrayUtil.compareUnsigned(applied, smallerApplied) < 0);
        assertTrue(ByteArrayUtil.compareUnsigned(applied, largerApplied) < 0);
    }

    @Test
    void shouldRevertNullWhenAscAndNullsLast() {
        // Behavior: An ascending field wants nulls last. The transform moves the null to the end, and revert with
        // the same options gives the original null back.
        byte[] applied = NullPositionTransform.apply(NULL, SortDirection.ASC, NullPosition.LAST);

        assertArrayEquals(NULL, NullPositionTransform.revert(applied, SortDirection.ASC, NullPosition.LAST));
    }

    @Test
    void shouldRevertNullWhenDescAndNullsFirst() {
        // Behavior: A descending field wants nulls first. The transform moves the null to the front, and revert
        // with the same options gives the original null back.
        byte[] applied = NullPositionTransform.apply(DESC_NULL, SortDirection.DESC, NullPosition.FIRST);

        assertArrayEquals(DESC_NULL, NullPositionTransform.revert(applied, SortDirection.DESC, NullPosition.FIRST));
    }

    @Test
    void shouldKeepNullAtDefaultPosition() {
        // Behavior: The null is already where the field wants it in two cases: ascending with nulls first, and
        // descending with nulls last. In both cases the transform returns the null unchanged.
        assertArrayEquals(NULL, NullPositionTransform.apply(NULL, SortDirection.ASC, NullPosition.FIRST));
        assertArrayEquals(DESC_NULL, NullPositionTransform.apply(DESC_NULL, SortDirection.DESC, NullPosition.LAST));
    }

    @Test
    void shouldKeepNonNullValue() {
        // Behavior: The string "kronotop" is not null, so apply and revert return it unchanged, both for an
        // ascending field with nulls last and for a descending field with nulls first.
        byte[] asc = Tuple.from("kronotop").pack();
        byte[] desc = (byte[]) SortDirectionTransform.apply("kronotop", SortDirection.DESC);

        assertArrayEquals(asc, NullPositionTransform.apply(asc, SortDirection.ASC, NullPosition.LAST));
        assertArrayEquals(asc, NullPositionTransform.revert(asc, SortDirection.ASC, NullPosition.LAST));
        assertArrayEquals(desc, NullPositionTransform.apply(desc, SortDirection.DESC, NullPosition.FIRST));
        assertArrayEquals(desc, NullPositionTransform.revert(desc, SortDirection.DESC, NullPosition.FIRST));
    }

    @Test
    void shouldReturnNullWhenValueIsNull() {
        // Behavior: When the value is a Java null, apply and revert both return null.
        assertNull(NullPositionTransform.apply(null, SortDirection.ASC, NullPosition.LAST));
        assertNull(NullPositionTransform.revert(null, SortDirection.ASC, NullPosition.LAST));
    }
}
