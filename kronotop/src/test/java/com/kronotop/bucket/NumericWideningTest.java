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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class NumericWideningTest {

    // --- canWiden tests ---

    @Test
    void shouldAllowInt32ToInt64Widening() {
        // Behavior: INT32 can be losslessly widened to INT64
        assertTrue(NumericWidening.canWiden(BsonType.INT32, BsonType.INT64));
    }

    @Test
    void shouldAllowInt32ToDoubleWidening() {
        // Behavior: INT32 can be losslessly widened to DOUBLE (all 32-bit ints fit in 53-bit mantissa)
        assertTrue(NumericWidening.canWiden(BsonType.INT32, BsonType.DOUBLE));
    }

    @Test
    void shouldAllowInt32ToDecimal128Widening() {
        // Behavior: INT32 can be losslessly widened to DECIMAL128
        assertTrue(NumericWidening.canWiden(BsonType.INT32, BsonType.DECIMAL128));
    }

    @Test
    void shouldAllowInt64ToDecimal128Widening() {
        // Behavior: INT64 can be losslessly widened to DECIMAL128
        assertTrue(NumericWidening.canWiden(BsonType.INT64, BsonType.DECIMAL128));
    }

    @Test
    void shouldAllowDoubleToDecimal128Widening() {
        // Behavior: DOUBLE can be losslessly widened to DECIMAL128
        assertTrue(NumericWidening.canWiden(BsonType.DOUBLE, BsonType.DECIMAL128));
    }

    @Test
    void shouldForbidInt64ToDoubleWidening() {
        // Behavior: INT64 to DOUBLE is lossy (64-bit integers exceed double's 53-bit mantissa)
        assertFalse(NumericWidening.canWiden(BsonType.INT64, BsonType.DOUBLE));
    }

    @Test
    void shouldAllowSameTypeWidening() {
        // Behavior: same-type widening is identity, always allowed
        assertTrue(NumericWidening.canWiden(BsonType.INT32, BsonType.INT32));
        assertTrue(NumericWidening.canWiden(BsonType.INT64, BsonType.INT64));
        assertTrue(NumericWidening.canWiden(BsonType.DOUBLE, BsonType.DOUBLE));
        assertTrue(NumericWidening.canWiden(BsonType.DECIMAL128, BsonType.DECIMAL128));
    }

    @Test
    void shouldForbidNonNumericWidening() {
        // Behavior: non-numeric types cannot be widened to or from numeric types
        assertFalse(NumericWidening.canWiden(BsonType.STRING, BsonType.INT32));
        assertFalse(NumericWidening.canWiden(BsonType.INT32, BsonType.STRING));
        assertFalse(NumericWidening.canWiden(BsonType.BOOLEAN, BsonType.INT64));
        assertFalse(NumericWidening.canWiden(BsonType.STRING, BsonType.DOUBLE));
    }

    @Test
    void shouldForbidNarrowingConversions() {
        // Behavior: narrowing (wider to narrower) is never allowed
        assertFalse(NumericWidening.canWiden(BsonType.INT64, BsonType.INT32));
        assertFalse(NumericWidening.canWiden(BsonType.DOUBLE, BsonType.INT32));
        assertFalse(NumericWidening.canWiden(BsonType.DECIMAL128, BsonType.INT32));
        assertFalse(NumericWidening.canWiden(BsonType.DECIMAL128, BsonType.INT64));
        assertFalse(NumericWidening.canWiden(BsonType.DECIMAL128, BsonType.DOUBLE));
        assertFalse(NumericWidening.canWiden(BsonType.DOUBLE, BsonType.INT64));
    }

    // --- widenValue tests ---

    @Test
    void shouldWidenInt32ToInt64Value() {
        // Behavior: INT32 value 42 widened to INT64 produces Long 42L
        Object result = NumericWidening.widenValue(new BsonInt32(42), BsonType.INT64);
        assertInstanceOf(Long.class, result);
        assertEquals(42L, result);
    }

    @Test
    void shouldWidenInt32ToDoubleValue() {
        // Behavior: INT32 value 42 widened to DOUBLE produces Double 42.0
        Object result = NumericWidening.widenValue(new BsonInt32(42), BsonType.DOUBLE);
        assertInstanceOf(Double.class, result);
        assertEquals(42.0, result);
    }

    @Test
    void shouldWidenInt32ToDecimal128Value() {
        // Behavior: INT32 value 42 widened to DECIMAL128 produces BigDecimal 42
        Object result = NumericWidening.widenValue(new BsonInt32(42), BsonType.DECIMAL128);
        assertInstanceOf(BigDecimal.class, result);
        assertEquals(BigDecimal.valueOf(42), result);
    }

    @Test
    void shouldWidenInt64ToDecimal128Value() {
        // Behavior: INT64 value widened to DECIMAL128 produces BigDecimal
        Object result = NumericWidening.widenValue(new BsonInt64(123456789L), BsonType.DECIMAL128);
        assertInstanceOf(BigDecimal.class, result);
        assertEquals(BigDecimal.valueOf(123456789L), result);
    }

    @Test
    void shouldWidenDoubleToDecimal128Value() {
        // Behavior: DOUBLE value widened to DECIMAL128 produces BigDecimal
        Object result = NumericWidening.widenValue(new BsonDouble(3.14), BsonType.DECIMAL128);
        assertInstanceOf(BigDecimal.class, result);
        assertEquals(BigDecimal.valueOf(3.14), result);
    }

    @Test
    void shouldReturnNullForForbiddenWidening() {
        // Behavior: widenValue returns null for INT64 to DOUBLE (forbidden)
        assertNull(NumericWidening.widenValue(new BsonInt64(42L), BsonType.DOUBLE));
    }

    @Test
    void shouldReturnNullForNonNumericWidening() {
        // Behavior: widenValue returns null for non-numeric types
        assertNull(NumericWidening.widenValue(new BsonString("hello"), BsonType.INT32));
    }

    @Test
    void shouldPreserveInt32MaxValueWhenWideningToInt64() {
        // Behavior: Integer.MAX_VALUE is preserved exactly when widened to INT64
        Object result = NumericWidening.widenValue(new BsonInt32(Integer.MAX_VALUE), BsonType.INT64);
        assertEquals((long) Integer.MAX_VALUE, result);
    }

    @Test
    void shouldPreserveInt32MinValueWhenWideningToInt64() {
        // Behavior: Integer.MIN_VALUE is preserved exactly when widened to INT64
        Object result = NumericWidening.widenValue(new BsonInt32(Integer.MIN_VALUE), BsonType.INT64);
        assertEquals((long) Integer.MIN_VALUE, result);
    }

    @Test
    void shouldPreserveInt32MaxValueWhenWideningToDouble() {
        // Behavior: Integer.MAX_VALUE is preserved exactly when widened to DOUBLE
        Object result = NumericWidening.widenValue(new BsonInt32(Integer.MAX_VALUE), BsonType.DOUBLE);
        assertEquals((double) Integer.MAX_VALUE, result);
    }

    @Test
    void shouldPreserveLongMaxValueWhenWideningToDecimal128() {
        // Behavior: Long.MAX_VALUE is preserved exactly when widened to DECIMAL128
        Object result = NumericWidening.widenValue(new BsonInt64(Long.MAX_VALUE), BsonType.DECIMAL128);
        assertEquals(BigDecimal.valueOf(Long.MAX_VALUE), result);
    }

    @Test
    void shouldWidenSameTypeViaToObject() {
        // Behavior: same-type widening delegates to BSONUtil.toObject
        Object result = NumericWidening.widenValue(new BsonInt32(42), BsonType.INT32);
        assertInstanceOf(Integer.class, result);
        assertEquals(42, result);
    }

    // --- commonType tests ---

    @Test
    void shouldReturnInt64ForInt32AndInt64() {
        // Behavior: common type for INT32 and INT64 is INT64
        assertEquals(BsonType.INT64, NumericWidening.commonType(BsonType.INT32, BsonType.INT64));
        assertEquals(BsonType.INT64, NumericWidening.commonType(BsonType.INT64, BsonType.INT32));
    }

    @Test
    void shouldReturnDoubleForInt32AndDouble() {
        // Behavior: common type for INT32 and DOUBLE is DOUBLE
        assertEquals(BsonType.DOUBLE, NumericWidening.commonType(BsonType.INT32, BsonType.DOUBLE));
        assertEquals(BsonType.DOUBLE, NumericWidening.commonType(BsonType.DOUBLE, BsonType.INT32));
    }

    @Test
    void shouldReturnDecimal128ForInt32AndDecimal128() {
        // Behavior: common type for INT32 and DECIMAL128 is DECIMAL128
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.INT32, BsonType.DECIMAL128));
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.DECIMAL128, BsonType.INT32));
    }

    @Test
    void shouldReturnDecimal128ForInt64AndDouble() {
        // Behavior: common type for INT64 and DOUBLE is DECIMAL128 (INT64->DOUBLE is lossy)
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.INT64, BsonType.DOUBLE));
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.DOUBLE, BsonType.INT64));
    }

    @Test
    void shouldReturnDecimal128ForInt64AndDecimal128() {
        // Behavior: common type for INT64 and DECIMAL128 is DECIMAL128
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.INT64, BsonType.DECIMAL128));
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.DECIMAL128, BsonType.INT64));
    }

    @Test
    void shouldReturnDecimal128ForDoubleAndDecimal128() {
        // Behavior: common type for DOUBLE and DECIMAL128 is DECIMAL128
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.DOUBLE, BsonType.DECIMAL128));
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.DECIMAL128, BsonType.DOUBLE));
    }

    @Test
    void shouldReturnSameTypeForIdenticalNumericTypes() {
        // Behavior: common type of a type with itself is that type
        assertEquals(BsonType.INT32, NumericWidening.commonType(BsonType.INT32, BsonType.INT32));
        assertEquals(BsonType.INT64, NumericWidening.commonType(BsonType.INT64, BsonType.INT64));
        assertEquals(BsonType.DOUBLE, NumericWidening.commonType(BsonType.DOUBLE, BsonType.DOUBLE));
        assertEquals(BsonType.DECIMAL128, NumericWidening.commonType(BsonType.DECIMAL128, BsonType.DECIMAL128));
    }

    @Test
    void shouldReturnNullForNonNumericCommonType() {
        // Behavior: commonType returns null when either type is non-numeric
        assertNull(NumericWidening.commonType(BsonType.STRING, BsonType.INT32));
        assertNull(NumericWidening.commonType(BsonType.INT32, BsonType.STRING));
        assertNull(NumericWidening.commonType(BsonType.BOOLEAN, BsonType.DOUBLE));
    }

    // --- isNumericBsonType tests ---

    @Test
    void shouldIdentifyNumericTypes() {
        // Behavior: INT32, INT64, DOUBLE, DECIMAL128 are numeric; others are not
        assertTrue(NumericWidening.isNumericBsonType(BsonType.INT32));
        assertTrue(NumericWidening.isNumericBsonType(BsonType.INT64));
        assertTrue(NumericWidening.isNumericBsonType(BsonType.DOUBLE));
        assertTrue(NumericWidening.isNumericBsonType(BsonType.DECIMAL128));

        assertFalse(NumericWidening.isNumericBsonType(BsonType.STRING));
        assertFalse(NumericWidening.isNumericBsonType(BsonType.BOOLEAN));
        assertFalse(NumericWidening.isNumericBsonType(BsonType.NULL));
        assertFalse(NumericWidening.isNumericBsonType(BsonType.BINARY));
        assertFalse(NumericWidening.isNumericBsonType(BsonType.DATE_TIME));
    }

    // --- promoteToComparable tests ---

    @Test
    void shouldPromoteInt32ToInt64() {
        // Behavior: Integer promoted to INT64 produces Long
        Comparable<?> result = NumericWidening.promoteToComparable(42, BsonType.INT32, BsonType.INT64);
        assertInstanceOf(Long.class, result);
        assertEquals(42L, result);
    }

    @Test
    void shouldPromoteInt32ToDouble() {
        // Behavior: Integer promoted to DOUBLE produces Double
        Comparable<?> result = NumericWidening.promoteToComparable(42, BsonType.INT32, BsonType.DOUBLE);
        assertInstanceOf(Double.class, result);
        assertEquals(42.0, result);
    }

    @Test
    void shouldPromoteInt32ToDecimal128() {
        // Behavior: Integer promoted to DECIMAL128 produces BigDecimal
        Comparable<?> result = NumericWidening.promoteToComparable(42, BsonType.INT32, BsonType.DECIMAL128);
        assertInstanceOf(BigDecimal.class, result);
        assertEquals(BigDecimal.valueOf(42), result);
    }

    @Test
    void shouldPromoteInt64ToDecimal128() {
        // Behavior: Long promoted to DECIMAL128 produces BigDecimal
        Comparable<?> result = NumericWidening.promoteToComparable(100L, BsonType.INT64, BsonType.DECIMAL128);
        assertInstanceOf(BigDecimal.class, result);
        assertEquals(BigDecimal.valueOf(100L), result);
    }

    @Test
    void shouldPromoteDoubleToDecimal128() {
        // Behavior: Double promoted to DECIMAL128 produces BigDecimal
        Comparable<?> result = NumericWidening.promoteToComparable(3.14, BsonType.DOUBLE, BsonType.DECIMAL128);
        assertInstanceOf(BigDecimal.class, result);
        assertEquals(BigDecimal.valueOf(3.14), result);
    }

    @Test
    void shouldReturnNullForInvalidPromotion() {
        // Behavior: promoting INT64 to DOUBLE returns null (forbidden path)
        assertNull(NumericWidening.promoteToComparable(42L, BsonType.INT64, BsonType.DOUBLE));
    }

    // --- extractNumericValue tests ---

    @Test
    void shouldExtractInt32Value() {
        // Behavior: extracts Integer from BsonInt32
        assertEquals(42, NumericWidening.extractNumericValue(new BsonInt32(42)));
    }

    @Test
    void shouldExtractInt64Value() {
        // Behavior: extracts Long from BsonInt64
        assertEquals(42L, NumericWidening.extractNumericValue(new BsonInt64(42L)));
    }

    @Test
    void shouldExtractDoubleValue() {
        // Behavior: extracts Double from BsonDouble
        assertEquals(3.14, NumericWidening.extractNumericValue(new BsonDouble(3.14)));
    }

    @Test
    void shouldExtractDecimal128Value() {
        // Behavior: extracts BigDecimal from BsonDecimal128
        assertEquals(
                new BigDecimal("42.5"),
                NumericWidening.extractNumericValue(new BsonDecimal128(Decimal128.parse("42.5")))
        );
    }

    @Test
    void shouldReturnNullForNonNumericExtraction() {
        // Behavior: extractNumericValue returns null for non-numeric types
        assertNull(NumericWidening.extractNumericValue(new BsonString("hello")));
    }
}
