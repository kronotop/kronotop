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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class SelectorCalculatorTest {

    // --- extractIndexValueFromBqlValue: ObjectIdVal ---

    @Test
    void shouldExtractObjectIdFromObjectIdVal() {
        // Behavior: ObjectIdVal is extracted as byte[] (ObjectId bytes) so that
        // buildCursorTuple can distinguish primary vs secondary index using
        // IndexDefinition rather than instanceof checks on the value.
        ObjectId objectId = new ObjectId();
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new ObjectIdVal(objectId));

        assertInstanceOf(byte[].class, result);
        assertArrayEquals(objectId.toByteArray(), (byte[]) result);
    }

    // --- extractIndexValueFromBqlValue: other types for completeness ---

    @Test
    void shouldExtractStringFromStringVal() {
        // Behavior: StringVal extracts to a plain String.
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new StringVal("hello"));
        assertEquals("hello", result);
    }

    @Test
    void shouldExtractLongFromInt32Val() {
        // Behavior: Int32Val is normalized to long for consistent index storage.
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new Int32Val(42));
        assertInstanceOf(Long.class, result);
        assertEquals(42L, result);
    }

    @Test
    void shouldExtractLongFromInt64Val() {
        // Behavior: Int64Val extracts to long.
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new Int64Val(100L));
        assertEquals(100L, result);
    }

    @Test
    void shouldExtractDoubleFromDoubleVal() {
        // Behavior: DoubleVal extracts to double.
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new DoubleVal(3.14));
        assertEquals(3.14, result);
    }

    @Test
    void shouldExtractBooleanFromBooleanVal() {
        // Behavior: BooleanVal extracts to boolean.
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new BooleanVal(true));
        assertEquals(true, result);
    }

    @Test
    void shouldExtractLongFromDateTimeVal() {
        // Behavior: DateTimeVal extracts to long (epoch millis).
        long ts = 1703462400000L;
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new DateTimeVal(ts));
        assertEquals(ts, result);
    }

    @Test
    void shouldExtractLongFromTimestampVal() {
        // Behavior: TimestampVal extracts to long.
        long ts = 7215996951904567296L;
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new TimestampVal(ts));
        assertEquals(ts, result);
    }

    @Test
    void shouldExtractBigDecimalFromDecimal128Val() {
        // Behavior: Decimal128Val extracts to BigDecimal.
        BigDecimal bd = new BigDecimal("12345.6789");
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new Decimal128Val(bd));
        assertEquals(bd, result);
    }

    @Test
    void shouldExtractByteArrayFromBinaryVal() {
        // Behavior: BinaryVal extracts to byte[].
        byte[] data = {0x01, 0x02, 0x03};
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(new BinaryVal(data));
        assertArrayEquals(data, (byte[]) result);
    }

    @Test
    void shouldExtractNullFromNullVal() {
        // Behavior: NullVal extracts to null.
        Object result = SelectorCalculator.extractIndexValueFromBqlValue(NullVal.INSTANCE);
        assertNull(result);
    }

    @Test
    void shouldThrowForUnsupportedBqlValueType() {
        // Behavior: BqlValue types not supported for indexing (e.g., DocumentVal) throw IllegalArgumentException.
        DocumentVal unsupported = new DocumentVal(java.util.Map.of());
        assertThrows(IllegalArgumentException.class,
                () -> SelectorCalculator.extractIndexValueFromBqlValue(unsupported));
    }
}
