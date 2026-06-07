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

package com.kronotop.bucket;

import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class TypeBracketComparatorTest {

    private final Comparator<BsonValue> comparator = TypeBracketComparator.INSTANCE;

    // Type order tests

    @Test
    void shouldOrderNullBeforeInt32() {
        assertTrue(comparator.compare(BsonNull.VALUE, new BsonInt32(1)) < 0);
    }

    @Test
    void shouldCompareInt32AndInt64ByNumericValue() {
        // Behavior: INT32 and INT64 in the same numeric bracket are compared by actual numeric value.
        assertTrue(comparator.compare(new BsonInt32(100), new BsonInt64(50)) > 0);
        assertTrue(comparator.compare(new BsonInt32(10), new BsonInt64(50)) < 0);
    }

    @Test
    void shouldCompareInt32AndDoubleByNumericValue() {
        // Behavior: INT32 and DOUBLE in the same numeric bracket are compared by actual numeric value.
        assertTrue(comparator.compare(new BsonInt32(1), new BsonDouble(1.5)) < 0);
        assertTrue(comparator.compare(new BsonInt32(2), new BsonDouble(1.5)) > 0);
    }

    @Test
    void shouldCompareInt64AndDoubleByNumericValue() {
        // Behavior: INT64 and DOUBLE are promoted to DECIMAL128 for lossless comparison.
        assertTrue(comparator.compare(new BsonInt64(1L), new BsonDouble(1.5)) < 0);
        assertTrue(comparator.compare(new BsonInt64(2L), new BsonDouble(1.5)) > 0);
    }

    @Test
    void shouldCompareInt32AndDecimal128ByNumericValue() {
        // Behavior: INT32 and DECIMAL128 in the same numeric bracket are compared by actual numeric value.
        assertTrue(comparator.compare(new BsonInt32(10), new BsonDecimal128(new Decimal128(new BigDecimal("9.99")))) > 0);
        assertTrue(comparator.compare(new BsonInt32(9), new BsonDecimal128(new Decimal128(new BigDecimal("9.99")))) < 0);
    }

    @Test
    void shouldCompareInt64AndDecimal128ByNumericValue() {
        // Behavior: INT64 and DECIMAL128 in the same numeric bracket are compared by actual numeric value.
        assertTrue(comparator.compare(new BsonInt64(100L), new BsonDecimal128(new Decimal128(new BigDecimal("99.99")))) > 0);
        assertTrue(comparator.compare(new BsonInt64(99L), new BsonDecimal128(new Decimal128(new BigDecimal("99.99")))) < 0);
    }

    @Test
    void shouldCompareDoubleAndDecimal128ByNumericValue() {
        // Behavior: DOUBLE and DECIMAL128 in the same numeric bracket are compared by actual numeric value.
        assertTrue(comparator.compare(new BsonDouble(3.15), new BsonDecimal128(new Decimal128(new BigDecimal("3.14")))) > 0);
        assertTrue(comparator.compare(new BsonDouble(3.13), new BsonDecimal128(new Decimal128(new BigDecimal("3.14")))) < 0);
    }

    @Test
    void shouldTreatEqualCrossTypeNumericValuesAsEqual() {
        // Behavior: Numerically equal values of different BSON types compare as equal.
        assertEquals(0, comparator.compare(new BsonInt32(42), new BsonInt64(42L)));
        assertEquals(0, comparator.compare(new BsonInt32(42), new BsonDouble(42.0)));
        assertEquals(0, comparator.compare(new BsonInt64(42L), new BsonDecimal128(new Decimal128(new BigDecimal("42")))));
        assertEquals(0, comparator.compare(new BsonDouble(42.0), new BsonDecimal128(new Decimal128(new BigDecimal("42.0")))));
    }

    @Test
    void shouldOrderAnyNumericBeforeString() {
        // Behavior: All numeric types belong to bracket 1, STRING to bracket 2; any numeric sorts before any string.
        assertTrue(comparator.compare(new BsonInt32(999), new BsonString("a")) < 0);
        assertTrue(comparator.compare(new BsonInt64(999L), new BsonString("a")) < 0);
        assertTrue(comparator.compare(new BsonDouble(999.0), new BsonString("a")) < 0);
        assertTrue(comparator.compare(new BsonDecimal128(new Decimal128(999)), new BsonString("a")) < 0);
    }

    @Test
    void shouldOrderStringBeforeDocument() {
        assertTrue(comparator.compare(new BsonString("a"), new BsonDocument()) < 0);
    }

    @Test
    void shouldOrderDocumentBeforeArray() {
        assertTrue(comparator.compare(new BsonDocument(), new BsonArray()) < 0);
    }

    @Test
    void shouldOrderArrayBeforeBinary() {
        assertTrue(comparator.compare(new BsonArray(), new BsonBinary(new byte[]{1})) < 0);
    }

    @Test
    void shouldOrderBinaryBeforeObjectId() {
        // Behavior: Binary values should be ordered before ObjectId values in type bracketing.
        assertTrue(comparator.compare(new BsonBinary(new byte[]{1}), new BsonObjectId(new ObjectId())) < 0);
    }

    @Test
    void shouldOrderObjectIdBeforeBoolean() {
        // Behavior: ObjectId values should be ordered before Boolean values in type bracketing.
        assertTrue(comparator.compare(new BsonObjectId(new ObjectId()), new BsonBoolean(true)) < 0);
    }

    @Test
    void shouldOrderBooleanBeforeDateTime() {
        assertTrue(comparator.compare(new BsonBoolean(true), new BsonDateTime(1L)) < 0);
    }

    @Test
    void shouldOrderDateTimeBeforeTimestamp() {
        assertTrue(comparator.compare(new BsonDateTime(1L), new BsonTimestamp(1, 1)) < 0);
    }

    // Same-type comparison tests

    @Test
    void shouldCompareNullsAsEqual() {
        assertEquals(0, comparator.compare(BsonNull.VALUE, BsonNull.VALUE));
    }

    @Test
    void shouldTreatJavaNullAsBsonNull() {
        assertEquals(0, comparator.compare(null, null));
        assertEquals(0, comparator.compare(null, BsonNull.VALUE));
        assertEquals(0, comparator.compare(BsonNull.VALUE, null));
    }

    @Test
    void shouldCompareJavaNullBeforeOtherTypes() {
        assertTrue(comparator.compare(null, new BsonInt32(1)) < 0);
        assertTrue(comparator.compare(new BsonInt32(1), null) > 0);
    }

    @Test
    void shouldCompareArraysWithNullElements() {
        BsonArray a1 = new BsonArray();
        a1.add(null);

        BsonArray a2 = new BsonArray();
        a2.add(BsonNull.VALUE);

        assertEquals(0, comparator.compare(a1, a2));
    }

    @Test
    void shouldCompareArraysWithBothContainingNull() {
        BsonArray a1 = new BsonArray();
        a1.add(null);

        BsonArray a2 = new BsonArray();
        a2.add(null);

        assertEquals(0, comparator.compare(a1, a2));
    }

    @Test
    void shouldCompareInt32Values() {
        assertTrue(comparator.compare(new BsonInt32(1), new BsonInt32(2)) < 0);
        assertTrue(comparator.compare(new BsonInt32(2), new BsonInt32(1)) > 0);
        assertEquals(0, comparator.compare(new BsonInt32(5), new BsonInt32(5)));
    }

    @Test
    void shouldCompareInt64Values() {
        assertTrue(comparator.compare(new BsonInt64(1L), new BsonInt64(2L)) < 0);
        assertTrue(comparator.compare(new BsonInt64(2L), new BsonInt64(1L)) > 0);
        assertEquals(0, comparator.compare(new BsonInt64(5L), new BsonInt64(5L)));
    }

    @Test
    void shouldCompareDoubleValues() {
        assertTrue(comparator.compare(new BsonDouble(1.0), new BsonDouble(2.0)) < 0);
        assertTrue(comparator.compare(new BsonDouble(2.0), new BsonDouble(1.0)) > 0);
        assertEquals(0, comparator.compare(new BsonDouble(3.14), new BsonDouble(3.14)));
    }

    @Test
    void shouldCompareDecimal128Values() {
        BsonDecimal128 small = new BsonDecimal128(new Decimal128(new BigDecimal("1.0")));
        BsonDecimal128 large = new BsonDecimal128(new Decimal128(new BigDecimal("2.0")));
        BsonDecimal128 equal1 = new BsonDecimal128(new Decimal128(new BigDecimal("3.14")));
        BsonDecimal128 equal2 = new BsonDecimal128(new Decimal128(new BigDecimal("3.14")));

        assertTrue(comparator.compare(small, large) < 0);
        assertTrue(comparator.compare(large, small) > 0);
        assertEquals(0, comparator.compare(equal1, equal2));
    }

    @Test
    void shouldCompareStringValues() {
        assertTrue(comparator.compare(new BsonString("apple"), new BsonString("banana")) < 0);
        assertTrue(comparator.compare(new BsonString("banana"), new BsonString("apple")) > 0);
        assertEquals(0, comparator.compare(new BsonString("same"), new BsonString("same")));
    }

    @Test
    void shouldCompareBinaryValues() {
        assertTrue(comparator.compare(new BsonBinary(new byte[]{1}), new BsonBinary(new byte[]{2})) < 0);
        assertTrue(comparator.compare(new BsonBinary(new byte[]{2}), new BsonBinary(new byte[]{1})) > 0);
        assertEquals(0, comparator.compare(new BsonBinary(new byte[]{1, 2}), new BsonBinary(new byte[]{1, 2})));
    }

    @Test
    void shouldCompareObjectIdValues() {
        // Behavior: ObjectId values of the same type should be compared by their natural ordering.
        ObjectId id1 = new ObjectId("000000000000000000000001");
        ObjectId id2 = new ObjectId("000000000000000000000002");
        ObjectId id3 = new ObjectId("000000000000000000000001");

        assertTrue(comparator.compare(new BsonObjectId(id1), new BsonObjectId(id2)) < 0);
        assertTrue(comparator.compare(new BsonObjectId(id2), new BsonObjectId(id1)) > 0);
        assertEquals(0, comparator.compare(new BsonObjectId(id1), new BsonObjectId(id3)));
    }

    @Test
    void shouldCompareBooleanValues() {
        assertTrue(comparator.compare(new BsonBoolean(false), new BsonBoolean(true)) < 0);
        assertTrue(comparator.compare(new BsonBoolean(true), new BsonBoolean(false)) > 0);
        assertEquals(0, comparator.compare(new BsonBoolean(true), new BsonBoolean(true)));
    }

    @Test
    void shouldCompareDateTimeValues() {
        assertTrue(comparator.compare(new BsonDateTime(1000L), new BsonDateTime(2000L)) < 0);
        assertTrue(comparator.compare(new BsonDateTime(2000L), new BsonDateTime(1000L)) > 0);
        assertEquals(0, comparator.compare(new BsonDateTime(1000L), new BsonDateTime(1000L)));
    }

    @Test
    void shouldCompareTimestampValues() {
        assertTrue(comparator.compare(new BsonTimestamp(1, 1), new BsonTimestamp(2, 1)) < 0);
        assertTrue(comparator.compare(new BsonTimestamp(2, 1), new BsonTimestamp(1, 1)) > 0);
        assertEquals(0, comparator.compare(new BsonTimestamp(1, 1), new BsonTimestamp(1, 1)));
    }

    // Array comparison tests

    @Test
    void shouldCompareEmptyArraysAsEqual() {
        assertEquals(0, comparator.compare(new BsonArray(), new BsonArray()));
    }

    @Test
    void shouldCompareArraysByElements() {
        BsonArray a1 = new BsonArray();
        a1.add(new BsonInt32(1));

        BsonArray a2 = new BsonArray();
        a2.add(new BsonInt32(2));

        assertTrue(comparator.compare(a1, a2) < 0);
    }

    @Test
    void shouldCompareShorterArrayAsSmaller() {
        BsonArray shorter = new BsonArray();
        shorter.add(new BsonInt32(1));

        BsonArray longer = new BsonArray();
        longer.add(new BsonInt32(1));
        longer.add(new BsonInt32(2));

        assertTrue(comparator.compare(shorter, longer) < 0);
    }

    // Document comparison tests

    @Test
    void shouldCompareEmptyDocumentsAsEqual() {
        assertEquals(0, comparator.compare(new BsonDocument(), new BsonDocument()));
    }

    @Test
    void shouldCompareDocumentsByFields() {
        BsonDocument d1 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument d2 = new BsonDocument("a", new BsonInt32(2));

        assertTrue(comparator.compare(d1, d2) < 0);
    }

    @Test
    void shouldCompareDocumentsByFieldNames() {
        BsonDocument d1 = new BsonDocument("a", new BsonInt32(1));
        BsonDocument d2 = new BsonDocument("b", new BsonInt32(1));

        assertTrue(comparator.compare(d1, d2) < 0);
    }

    @Test
    void shouldCompareSmallerDocumentAsSmaller() {
        BsonDocument smaller = new BsonDocument("a", new BsonInt32(1));
        BsonDocument larger = new BsonDocument("a", new BsonInt32(1)).append("b", new BsonInt32(2));

        assertTrue(comparator.compare(smaller, larger) < 0);
    }

    // Unsupported type test

    @Test
    void shouldThrowExceptionForUnsupportedType() {
        // Behavior: Unsupported BSON types should cause an IllegalArgumentException.
        BsonValue unsupported = new BsonRegularExpression(".*");
        BsonValue supported = new BsonInt32(1);

        assertThrows(IllegalArgumentException.class, () -> comparator.compare(unsupported, supported));
    }
}
