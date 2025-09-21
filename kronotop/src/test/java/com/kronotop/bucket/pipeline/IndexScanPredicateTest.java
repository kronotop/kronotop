/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.Operator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class IndexScanPredicateTest {

    static Stream<Arguments> provideBqlValueTestCases() {
        return Stream.of(
                // StringVal
                Arguments.of(
                        new StringVal("test"),
                        new StringVal("test"),
                        new StringVal("different")
                ),
                // Int32Val
                Arguments.of(
                        new Int32Val(42),
                        new Int32Val(42),
                        new Int32Val(24)
                ),
                // Int64Val
                Arguments.of(
                        new Int64Val(42L),
                        new Int64Val(42L),
                        new Int64Val(24L)
                ),
                // DoubleVal
                Arguments.of(
                        new DoubleVal(42.5),
                        new DoubleVal(42.5),
                        new DoubleVal(24.5)
                ),
                // Decimal128Val
                Arguments.of(
                        new Decimal128Val(new BigDecimal("42.5")),
                        new Decimal128Val(new BigDecimal("42.5")),
                        new Decimal128Val(new BigDecimal("24.5"))
                ),
                // BooleanVal
                Arguments.of(
                        new BooleanVal(true),
                        new BooleanVal(true),
                        new BooleanVal(false)
                ),
                // BinaryVal
                Arguments.of(
                        new BinaryVal(new byte[]{1, 2, 3}),
                        new BinaryVal(new byte[]{1, 2, 3}),
                        new BinaryVal(new byte[]{4, 5, 6})
                ),
                // TimestampVal
                Arguments.of(
                        new TimestampVal(1234567890L),
                        new TimestampVal(1234567890L),
                        new TimestampVal(9876543210L)
                ),
                // DateTimeVal
                Arguments.of(
                        new DateTimeVal(1234567890L),
                        new DateTimeVal(1234567890L),
                        new DateTimeVal(9876543210L)
                ),
                // VersionstampVal
                Arguments.of(
                        new VersionstampVal(Versionstamp.incomplete(1)),
                        new VersionstampVal(Versionstamp.incomplete(1)),
                        new VersionstampVal(Versionstamp.incomplete(2))
                )
        );
    }

    @ParameterizedTest
    @EnumSource(value = Operator.class, names = {"EQ", "GT", "GTE", "LT", "LTE"})
    void testNonNeOperatorsReturnTrue(Operator operator) {
        // Given
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", operator, new StringVal("test"));
        BqlValue anyValue = new StringVal("different");

        // When
        boolean result = predicate.test(anyValue);

        // Then
        assertTrue(result, "Non-NE operators should always return true");
    }

    @ParameterizedTest
    @MethodSource("provideBqlValueTestCases")
    void testNeOperatorWithMatchingTypes(BqlValue operand, BqlValue matchingValue, BqlValue differentValue) {
        // Given
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.NE, operand);

        // When & Then - same values should return false (not different)
        assertFalse(predicate.test(matchingValue), "NE should return false for equal values");

        // When & Then - different values should return true
        assertTrue(predicate.test(differentValue), "NE should return true for different values");
    }

    @ParameterizedTest
    @MethodSource("provideBqlValueTestCases")
    void testNeOperatorWithNonMatchingTypes(BqlValue operand, BqlValue matchingValue, BqlValue differentValue) {
        // Given
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.NE, operand);

        // When & Then - different types should return false
        BqlValue differentType = new StringVal("different_type");
        if (!(operand instanceof StringVal)) {
            assertFalse(predicate.test(differentType), "NE should return false for different types");
        }
    }

    @Test
    void testNeOperatorWithNullValues() {
        // Given
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.NE, NullVal.INSTANCE);

        // When & Then - null vs null should return false
        assertFalse(predicate.test(NullVal.INSTANCE), "NE should return false for null == null");

        // When & Then - null vs non-null should return true
        assertTrue(predicate.test(new StringVal("test")), "NE should return true for null != non-null");
    }

    @Test
    void testNeOperatorWithComplexTypes() {
        // Array test
        List<BqlValue> list1 = List.of(new StringVal("a"), new StringVal("b"));
        List<BqlValue> list2 = List.of(new StringVal("a"), new StringVal("b"));
        List<BqlValue> list3 = List.of(new StringVal("c"), new StringVal("d"));

        IndexScanPredicate arrayPredicate = new IndexScanPredicate(1, "field", Operator.NE, new ArrayVal(list1));
        assertFalse(arrayPredicate.test(new ArrayVal(list2)), "Equal arrays should return false for NE");
        assertTrue(arrayPredicate.test(new ArrayVal(list3)), "Different arrays should return true for NE");

        // Document test
        Map<String, BqlValue> doc1 = Map.of("key1", new StringVal("value1"));
        Map<String, BqlValue> doc2 = Map.of("key1", new StringVal("value1"));
        Map<String, BqlValue> doc3 = Map.of("key1", new StringVal("value2"));

        IndexScanPredicate docPredicate = new IndexScanPredicate(1, "field", Operator.NE, new DocumentVal(doc1));
        assertFalse(docPredicate.test(new DocumentVal(doc2)), "Equal documents should return false for NE");
        assertTrue(docPredicate.test(new DocumentVal(doc3)), "Different documents should return true for NE");
    }

    @Test
    void testUnsupportedOperandType() {
        // Given an unsupported operand type
        Object unsupportedOperand = "raw string instead of BqlValue";
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.NE, unsupportedOperand);

        // When & Then
        assertThrows(IllegalStateException.class, () -> predicate.test(new StringVal("test")),
                "Should throw IllegalStateException for unsupported operand types");
    }
}