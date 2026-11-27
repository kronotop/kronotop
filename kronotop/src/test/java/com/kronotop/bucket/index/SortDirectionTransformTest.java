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
import com.apple.foundationdb.tuple.Tuple;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SortDirectionTransformTest {

    static Stream<Arguments> indexTypeTestData() {
        return Stream.of(
                Arguments.of(BsonType.INT32, 1988, 2026),
                Arguments.of(BsonType.INT64, 1988L, 2026L),
                Arguments.of(BsonType.DOUBLE, 19.88, 20.26),
                Arguments.of(BsonType.STRING, "apple", "banana"),
                Arguments.of(BsonType.BOOLEAN, false, true),
                Arguments.of(
                        BsonType.BINARY,
                        "apple".getBytes(StandardCharsets.UTF_8),
                        "banana".getBytes(StandardCharsets.UTF_8)
                ),
                Arguments.of(
                        BsonType.DATE_TIME,
                        Instant.parse("1988-01-01T00:00:00Z").toEpochMilli(),
                        Instant.parse("2026-01-01T00:00:00Z").toEpochMilli()
                ),
                Arguments.of(
                        BsonType.TIMESTAMP,
                        new BsonTimestamp(1988, 0).getValue(),
                        new BsonTimestamp(2026, 0).getValue()
                ),
                Arguments.of(
                        BsonType.OBJECT_ID,
                        new ObjectId(1988, 0).toByteArray(),
                        new ObjectId(2026, 0).toByteArray()
                )
        );
    }

    @Test
    void shouldReturnGivenValueWhenDirectionIsAsc() {
        // Behavior: Applies ASC to the integer 1988. The result is 1988.
        var value = 1988;

        assertEquals(value, SortDirectionTransform.apply(value, SortDirection.ASC));
    }

    @Test
    void shouldReturnGivenBytesWhenRevertDirectionIsAsc() {
        // Behavior: Reverts the bytes of "kronotop" with ASC. The result is the bytes of "kronotop".
        byte[] data = "kronotop".getBytes(StandardCharsets.UTF_8);

        assertArrayEquals("kronotop".getBytes(StandardCharsets.UTF_8), SortDirectionTransform.revert(data, SortDirection.ASC));
    }

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

    @ParameterizedTest(name = "{0}")
    @MethodSource("indexTypeTestData")
    void shouldReverseOrderAndRevertForEachIndexType(BsonType bsonType, Object smaller, Object larger) {
        // Behavior: For each supported index type, applies DESC to a smaller and a larger value. The result of
        // the larger value sorts before the result of the smaller one. Reverting each result with DESC and
        // unpacking it as a tuple gives the original value.
        byte[] smallerApplied = (byte[]) SortDirectionTransform.apply(smaller, SortDirection.DESC);
        byte[] largerApplied = (byte[]) SortDirectionTransform.apply(larger, SortDirection.DESC);

        assertTrue(
                ByteArrayUtil.compareUnsigned(largerApplied, smallerApplied) < 0,
                "Larger value should sort first for " + bsonType
        );

        byte[] smallerReverted = SortDirectionTransform.revert(smallerApplied, SortDirection.DESC);
        byte[] largerReverted = SortDirectionTransform.revert(largerApplied, SortDirection.DESC);

        assertEquals(Tuple.from(smaller), Tuple.fromBytes(smallerReverted));
        assertEquals(Tuple.from(larger), Tuple.fromBytes(largerReverted));
    }
}
