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
import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PredicateEvaluatorTest {

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    private static ByteBuffer toBuffer(BsonDocument doc) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            CODEC.encode(new BsonBinaryWriter(buffer), doc, EncoderContext.builder().build());
            return ByteBuffer.wrap(buffer.toByteArray());
        }
    }

    // ==================== evaluateComparison Tests ====================

    @Nested
    @DisplayName("String Comparisons")
    class StringComparisons {

        @Test
        void shouldHandleEquality() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", "test"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", "other"));
        }

        @Test
        void shouldHandleInequality() {
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, "test", "test"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, "test", "other"));
        }

        @Test
        void shouldHandleOrdering() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, "z", "a"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, "a", "z"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, "z", "z"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, "a", "z"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, "a", "a"));
        }
    }

    @Nested
    @DisplayName("Byte Array Comparisons")
    class ByteArrayComparisons {

        @Test
        void shouldHandleEquality() {
            byte[] a = {1, 2, 3};
            byte[] b = {1, 2, 3};
            byte[] c = {1, 2, 4};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldHandleOrdering() {
            byte[] small = {1, 2, 3};
            byte[] large = {1, 2, 4};
            byte[] equal = {1, 2, 3};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, small, large));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, large, small));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, small, equal));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, large, small));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, small, equal));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, small, large));
        }

        @Test
        void shouldHandleInOperator() {
            byte[] target = {1, 2, 3};
            byte[] match = {1, 2, 3};
            List<Object> list = List.of(match, new byte[]{4, 5, 6});

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.IN, target, list));
        }

        @Test
        void shouldHandleNinOperator() {
            byte[] target = {7, 8, 9};
            List<Object> list = List.of(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NIN, target, list));
        }
    }

    @Nested
    @DisplayName("Array Equality Comparisons")
    class ArrayEqualityComparisons {

        @Test
        void shouldCompareIntArraysForEquality() {
            int[] a = {1, 2, 3};
            int[] b = {1, 2, 3};
            int[] c = {1, 2, 4};
            int[] d = {3, 2, 1};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, d)); // different order
        }

        @Test
        void shouldCompareIntArraysForInequality() {
            int[] a = {1, 2, 3};
            int[] b = {1, 2, 3};
            int[] c = {1, 2, 4};

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, a, b));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, a, c));
        }

        @Test
        void shouldCompareLongArraysForEquality() {
            long[] a = {100L, 200L};
            long[] b = {100L, 200L};
            long[] c = {200L, 100L};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldCompareDoubleArraysForEquality() {
            double[] a = {1.1, 2.2};
            double[] b = {1.1, 2.2};
            double[] c = {2.2, 1.1};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldCompareObjectArraysForEquality() {
            String[] a = {"apple", "banana"};
            String[] b = {"apple", "banana"};
            String[] c = {"banana", "apple"};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldCompareListsForEquality() {
            List<String> a = List.of("apple", "banana");
            List<String> b = List.of("apple", "banana");
            List<String> c = List.of("banana", "apple");
            List<String> d = List.of("apple");

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c)); // different order
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, d)); // different size
        }

        @Test
        void shouldCompareNestedListsForEquality() {
            List<List<Integer>> a = List.of(List.of(1, 2), List.of(3, 4));
            List<List<Integer>> b = List.of(List.of(1, 2), List.of(3, 4));
            List<List<Integer>> c = List.of(List.of(1, 2), List.of(4, 3));

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldReturnFalseForDifferentArrayTypes() {
            int[] intArray = {1, 2, 3};
            long[] longArray = {1L, 2L, 3L};

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, intArray, longArray));
        }
    }

    @Nested
    @DisplayName("ArrayVal Comparisons (valuesEqual)")
    class ArrayValComparisons {

        @Test
        void shouldCompareSimpleArrayVals() {
            ArrayVal a = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal b = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal c = new ArrayVal(List.of(new StringVal("apple"), new StringVal("cherry")));

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldRespectOrderInArrayVals() {
            ArrayVal a = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal b = new ArrayVal(List.of(new StringVal("banana"), new StringVal("apple")));

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
        }

        @Test
        void shouldCompareDifferentSizedArrayVals() {
            ArrayVal a = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal b = new ArrayVal(List.of(new StringVal("apple")));

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
        }

        @Test
        void shouldCompareEmptyArrayVals() {
            ArrayVal a = new ArrayVal(List.of());
            ArrayVal b = new ArrayVal(List.of());

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
        }

        @Test
        void shouldCompareNestedArrayVals() {
            ArrayVal inner1 = new ArrayVal(List.of(new Int32Val(1), new Int32Val(2)));
            ArrayVal inner2 = new ArrayVal(List.of(new Int32Val(3), new Int32Val(4)));
            ArrayVal a = new ArrayVal(List.of(inner1, inner2));

            ArrayVal inner3 = new ArrayVal(List.of(new Int32Val(1), new Int32Val(2)));
            ArrayVal inner4 = new ArrayVal(List.of(new Int32Val(3), new Int32Val(4)));
            ArrayVal b = new ArrayVal(List.of(inner3, inner4));

            ArrayVal inner5 = new ArrayVal(List.of(new Int32Val(1), new Int32Val(2)));
            ArrayVal inner6 = new ArrayVal(List.of(new Int32Val(3), new Int32Val(5))); // different
            ArrayVal c = new ArrayVal(List.of(inner5, inner6));

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldCompareArrayValsWithMixedTypes() {
            ArrayVal a = new ArrayVal(List.of(new StringVal("test"), new Int32Val(42), new BooleanVal(true)));
            ArrayVal b = new ArrayVal(List.of(new StringVal("test"), new Int32Val(42), new BooleanVal(true)));
            ArrayVal c = new ArrayVal(List.of(new StringVal("test"), new Int32Val(42), new BooleanVal(false)));

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldHandleNeOperatorForArrayVals() {
            ArrayVal a = new ArrayVal(List.of(new StringVal("apple")));
            ArrayVal b = new ArrayVal(List.of(new StringVal("apple")));
            ArrayVal c = new ArrayVal(List.of(new StringVal("banana")));

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, a, b));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, a, c));
        }
    }

    @Nested
    @DisplayName("Integer Comparisons")
    class IntegerComparisons {

        @Test
        void shouldHandleEquality() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, 42, 42));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, 42, 43));
        }

        @Test
        void shouldHandleOrdering() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, 10, 5));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, 5, 10));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, 5, 10));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LT, 10, 5));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, 10, 10));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, 10, 5));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GTE, 5, 10));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, 10, 10));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, 5, 10));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LTE, 10, 5));
        }
    }

    @Nested
    @DisplayName("Long Comparisons")
    class LongComparisons {

        @Test
        void shouldHandleOrdering() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, 200L, 100L));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, 100L, 200L));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, 100L, 200L));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LT, 200L, 100L));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, 200L, 200L));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, 200L, 100L));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GTE, 100L, 200L));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, 100L, 100L));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, 100L, 200L));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LTE, 200L, 100L));
        }
    }

    @Nested
    @DisplayName("Double Comparisons")
    class DoubleComparisons {

        @Test
        void shouldHandleOrdering() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, 3.14, 2.71));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, 2.71, 3.14));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, 2.71, 3.14));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LT, 3.14, 2.71));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, 3.14, 3.14));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, 3.14, 2.71));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GTE, 2.71, 3.14));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, 2.71, 2.71));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, 2.71, 3.14));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LTE, 3.14, 2.71));
        }
    }

    @Nested
    @DisplayName("BigDecimal Comparisons")
    class BigDecimalComparisons {

        @Test
        void shouldHandleOrdering() {
            BigDecimal small = new BigDecimal("100.00");
            BigDecimal large = new BigDecimal("200.00");
            BigDecimal equal = new BigDecimal("100.00");

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, large, small));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, small, large));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, small, large));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LT, large, small));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, small, equal));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, large, small));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GTE, small, large));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, small, equal));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, small, large));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LTE, large, small));
        }
    }

    @Nested
    @DisplayName("Versionstamp Comparisons")
    class VersionstampComparisons {

        @Test
        void shouldHandleOrdering() {
            byte[] bytes1 = new byte[12];
            byte[] bytes2 = new byte[12];
            byte[] bytes3 = new byte[12];
            bytes1[0] = 1;
            bytes2[0] = 2;
            bytes3[0] = 1;

            Versionstamp small = Versionstamp.fromBytes(bytes1);
            Versionstamp large = Versionstamp.fromBytes(bytes2);
            Versionstamp equal = Versionstamp.fromBytes(bytes3);

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, small, large));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LT, large, small));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, large, small));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, small, large));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, small, equal));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, large, small));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GTE, small, large));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, small, equal));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, small, large));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LTE, large, small));
        }
    }

    @Nested
    @DisplayName("Null Handling")
    class NullHandling {

        @Test
        void shouldHandleBothNull() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, null, null));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, null, null));
        }

        @Test
        void shouldHandleOneNull() {
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", null));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, "test", null));
        }

        @Test
        void shouldReturnFalseForOrderingWithNull() {
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, null, "test"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, "test", null));
        }
    }

    @Nested
    @DisplayName("IN/NIN Operators")
    class InNinOperators {

        @Test
        void shouldHandleIn() {
            List<Object> list = List.of("apple", "banana");
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.IN, "apple", list));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.IN, "grape", list));
        }

        @Test
        void shouldHandleNin() {
            List<Object> list = List.of("apple", "banana");
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NIN, "grape", list));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NIN, "apple", list));
        }

        @Test
        void shouldReturnFalseForNonListOperand() {
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.IN, "test", "not-a-list"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NIN, "test", "not-a-list"));
        }
    }

    @Nested
    @DisplayName("Error Cases")
    class ErrorCases {

        @Test
        void shouldThrowForUnsupportedOperator() {
            assertThrows(UnsupportedOperationException.class, () ->
                    PredicateEvaluator.evaluateComparison(Operator.SIZE, "test", "test"));
        }

        @Test
        void shouldReturnFalseForNonComparableType() {
            Object a = new Object();
            Object b = new Object();
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, a, b));
        }

        @Test
        void shouldReturnFalseForTypeMismatch() {
            // EQ with different types
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, "string", 123));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, 100, 100L));

            // NE with different types
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, "string", 123));

            // Ordering with different types
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, "string", 123));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LT, 100, "100"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GTE, 100L, 100));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LTE, 3.14, 3));
        }
    }

    // ==================== testResidualPredicate Tests ====================

    @Nested
    @DisplayName("testResidualPredicate - String Fields")
    class ResidualPredicateStringTests {

        @Test
        void shouldMatchStringEquality() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, new StringVal("John"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldNotMatchStringEquality() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, new StringVal("Jane"));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleStringGtComparison() {
            BsonDocument doc = new BsonDocument("name", new BsonString("zebra"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.GT, new StringVal("apple"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleStringLtComparison() {
            BsonDocument doc = new BsonDocument("name", new BsonString("apple"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.LT, new StringVal("zebra"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleStringGteComparison() {
            BsonDocument doc = new BsonDocument("name", new BsonString("banana"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.GTE, new StringVal("banana")), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.GTE, new StringVal("apple")), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.GTE, new StringVal("cherry")), buffer.duplicate()));
        }

        @Test
        void shouldHandleStringLteComparison() {
            BsonDocument doc = new BsonDocument("name", new BsonString("banana"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.LTE, new StringVal("banana")), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.LTE, new StringVal("cherry")), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.LTE, new StringVal("apple")), buffer.duplicate()));
        }

        @Test
        void shouldHandleStringNeComparison() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.NE, new StringVal("Jane")), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.NE, new StringVal("John")), buffer.duplicate()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Numeric Fields")
    class ResidualPredicateNumericTests {

        @Test
        void shouldCompareInt32Field() {
            BsonDocument doc = new BsonDocument("age", new BsonInt32(25));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, new Int32Val(20));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareInt64Field() {
            BsonDocument doc = new BsonDocument("timestamp", new BsonInt64(1234567890L));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "timestamp", Operator.LTE, new Int64Val(2000000000L));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareDoubleField() {
            BsonDocument doc = new BsonDocument("price", new BsonDouble(19.99));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "price", Operator.LT, new DoubleVal(20.0));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareDecimal128Field() {
            BsonDocument doc = new BsonDocument("amount", new BsonDecimal128(new Decimal128(new BigDecimal("100.50"))));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "amount", Operator.GTE, new Decimal128Val(new BigDecimal("100.00")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareInt64AllOperators() {
            BsonDocument doc = new BsonDocument("value", new BsonInt64(1000000L));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int64Val(1000000L)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int64Val(999999L)), buffer.duplicate()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new Int64Val(999999L)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new Int64Val(1000000L)), buffer.duplicate()));
            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, new Int64Val(500000L)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, new Int64Val(2000000L)), buffer.duplicate()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Int64Val(1000000L)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Int64Val(500000L)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Int64Val(2000000L)), buffer.duplicate()));
            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, new Int64Val(2000000L)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, new Int64Val(500000L)), buffer.duplicate()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Int64Val(1000000L)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Int64Val(2000000L)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Int64Val(500000L)), buffer.duplicate()));
        }

        @Test
        void shouldCompareDecimal128AllOperators() {
            BsonDocument doc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("100.50"))));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Decimal128Val(new BigDecimal("100.50"))), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Decimal128Val(new BigDecimal("100.49"))), buffer.duplicate()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new Decimal128Val(new BigDecimal("100.49"))), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new Decimal128Val(new BigDecimal("100.50"))), buffer.duplicate()));
            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, new Decimal128Val(new BigDecimal("50.00"))), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, new Decimal128Val(new BigDecimal("200.00"))), buffer.duplicate()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Decimal128Val(new BigDecimal("100.50"))), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Decimal128Val(new BigDecimal("50.00"))), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Decimal128Val(new BigDecimal("200.00"))), buffer.duplicate()));
            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, new Decimal128Val(new BigDecimal("200.00"))), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, new Decimal128Val(new BigDecimal("50.00"))), buffer.duplicate()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Decimal128Val(new BigDecimal("100.50"))), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Decimal128Val(new BigDecimal("200.00"))), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Decimal128Val(new BigDecimal("50.00"))), buffer.duplicate()));
        }

        @Test
        void shouldCompareInt32AllOperators() {
            BsonDocument doc = new BsonDocument("value", new BsonInt32(50));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int32Val(50)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int32Val(51)), buffer.duplicate()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new Int32Val(51)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new Int32Val(50)), buffer.duplicate()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Int32Val(50)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Int32Val(40)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new Int32Val(60)), buffer.duplicate()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Int32Val(50)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Int32Val(60)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new Int32Val(40)), buffer.duplicate()));
            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, new Int32Val(60)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, new Int32Val(50)), buffer.duplicate()));
        }

        @Test
        void shouldCompareDoubleAllOperators() {
            BsonDocument doc = new BsonDocument("value", new BsonDouble(3.14));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new DoubleVal(3.14)), buffer.duplicate()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new DoubleVal(2.71)), buffer.duplicate()));
            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, new DoubleVal(2.0)), buffer.duplicate()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new DoubleVal(3.14)), buffer.duplicate()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new DoubleVal(3.14)), buffer.duplicate()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Boolean Fields")
    class ResidualPredicateBooleanTests {

        @Test
        void shouldCompareBooleanField() {
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "active", Operator.EQ, new BooleanVal(true));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareBooleanFieldNe() {
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "active", Operator.NE, new BooleanVal(false));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForUnsupportedBooleanOperators() {
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            // GT, GTE, LT, LTE are not supported for booleans - should return false
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.GT, new BooleanVal(false)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.GTE, new BooleanVal(false)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.LT, new BooleanVal(true)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.LTE, new BooleanVal(true)), buffer.duplicate()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Null Fields")
    class ResidualPredicateNullTests {

        @Test
        void shouldCompareNullField() {
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "optional", Operator.EQ, NullVal.INSTANCE);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnTrueForMissingFieldWithEqNull() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "nonexistent", Operator.EQ, NullVal.INSTANCE);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleExistsOperator() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EXISTS, NullVal.INSTANCE);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForNullWithExists() {
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "optional", Operator.EXISTS, NullVal.INSTANCE);
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldEvaluateNullComparisonOperators() {
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            // GT: null is not > null
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.GT, NullVal.INSTANCE), buffer.duplicate()));
            // GTE: null >= null
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.GTE, NullVal.INSTANCE), buffer.duplicate()));
            // LT: null is not < null
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.LT, NullVal.INSTANCE), buffer.duplicate()));
            // LTE: null <= null
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.LTE, NullVal.INSTANCE), buffer.duplicate()));
        }

        @Test
        void shouldHandleNullNeComparison() {
            BsonDocument docWithNull = new BsonDocument("optional", new BsonNull());
            ByteBuffer bufferWithNull = toBuffer(docWithNull);

            // NE on null field with NullVal should return false (they are equal)
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.NE, NullVal.INSTANCE), bufferWithNull));

            // NE on non-null field with NullVal should return true
            BsonDocument docWithValue = new BsonDocument("optional", new BsonString("value"));
            ByteBuffer bufferWithValue = toBuffer(docWithValue);
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.NE, NullVal.INSTANCE), bufferWithValue));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Binary Fields")
    class ResidualPredicateBinaryTests {

        @Test
        void shouldCompareBinaryField() {
            byte[] data = {1, 2, 3, 4, 5};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "data", Operator.EQ, new BinaryVal(data));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareBinaryFieldNe() {
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.NE, new BinaryVal(new byte[]{4, 5, 6})), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.NE, new BinaryVal(new byte[]{1, 2, 3})), buffer.duplicate()));
        }

        @Test
        void shouldCompareBinaryFieldOrdering() {
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GT, new BinaryVal(new byte[]{1, 2, 2})), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GT, new BinaryVal(new byte[]{1, 2, 4})), buffer.duplicate()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GTE, new BinaryVal(new byte[]{1, 2, 3})), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GTE, new BinaryVal(new byte[]{1, 2, 2})), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GTE, new BinaryVal(new byte[]{1, 2, 4})), buffer.duplicate()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LT, new BinaryVal(new byte[]{1, 2, 4})), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LT, new BinaryVal(new byte[]{1, 2, 2})), buffer.duplicate()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LTE, new BinaryVal(new byte[]{1, 2, 3})), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LTE, new BinaryVal(new byte[]{1, 2, 4})), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LTE, new BinaryVal(new byte[]{1, 2, 2})), buffer.duplicate()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Timestamp/DateTime Fields")
    class ResidualPredicateTimestampTests {

        @Test
        void shouldCompareTimestampField() {
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "created", Operator.EQ, new TimestampVal(ts.getValue()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareDateTimeField() {
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "eventTime", Operator.EQ, new DateTimeVal(dateTime));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareTimestampFieldAllOperators() {
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);
            long value = ts.getValue();

            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.NE, new TimestampVal(value + 1)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.NE, new TimestampVal(value)), buffer.duplicate()));

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GT, new TimestampVal(value - 1)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GT, new TimestampVal(value)), buffer.duplicate()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GTE, new TimestampVal(value)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GTE, new TimestampVal(value - 1)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GTE, new TimestampVal(value + 1)), buffer.duplicate()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LT, new TimestampVal(value + 1)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LT, new TimestampVal(value)), buffer.duplicate()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LTE, new TimestampVal(value)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LTE, new TimestampVal(value + 1)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LTE, new TimestampVal(value - 1)), buffer.duplicate()));
        }

        @Test
        void shouldCompareDateTimeFieldAllOperators() {
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.NE, new DateTimeVal(dateTime + 1000)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.NE, new DateTimeVal(dateTime)), buffer.duplicate()));

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GT, new DateTimeVal(dateTime - 1000)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GT, new DateTimeVal(dateTime)), buffer.duplicate()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GTE, new DateTimeVal(dateTime)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GTE, new DateTimeVal(dateTime - 1000)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GTE, new DateTimeVal(dateTime + 1000)), buffer.duplicate()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LT, new DateTimeVal(dateTime + 1000)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LT, new DateTimeVal(dateTime)), buffer.duplicate()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LTE, new DateTimeVal(dateTime)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LTE, new DateTimeVal(dateTime + 1000)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LTE, new DateTimeVal(dateTime - 1000)), buffer.duplicate()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Versionstamp Fields")
    class ResidualPredicateVersionstampTests {

        @Test
        void shouldCompareVersionstampField() {
            byte[] bytes = new byte[12];
            Arrays.fill(bytes, (byte) 1);
            Versionstamp vs = Versionstamp.fromBytes(bytes);

            BsonDocument doc = new BsonDocument("version", new BsonBinary(BsonBinarySubType.BINARY, bytes));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "version", Operator.EQ, new VersionstampVal(vs));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareVersionstampFieldWithOrdering() {
            byte[] bytes1 = new byte[12];
            byte[] bytes2 = new byte[12];
            bytes1[0] = 1;
            bytes2[0] = 2;

            BsonDocument doc = new BsonDocument("version", new BsonBinary(BsonBinarySubType.BINARY, bytes1));
            ByteBuffer buffer = toBuffer(doc);

            Versionstamp expected = Versionstamp.fromBytes(bytes2);
            ResidualPredicate predicate = new ResidualPredicate(1, "version", Operator.LT, new VersionstampVal(expected));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldCompareVersionstampFieldAllOperators() {
            byte[] bytesSmall = new byte[12];
            byte[] bytesMedium = new byte[12];
            byte[] bytesLarge = new byte[12];
            bytesSmall[0] = 1;
            bytesMedium[0] = 2;
            bytesLarge[0] = 3;

            BsonDocument doc = new BsonDocument("version", new BsonBinary(BsonBinarySubType.BINARY, bytesMedium));
            ByteBuffer buffer = toBuffer(doc);

            Versionstamp small = Versionstamp.fromBytes(bytesSmall);
            Versionstamp medium = Versionstamp.fromBytes(bytesMedium);
            Versionstamp large = Versionstamp.fromBytes(bytesLarge);

            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.NE, new VersionstampVal(small)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.NE, new VersionstampVal(medium)), buffer.duplicate()));

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GT, new VersionstampVal(small)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GT, new VersionstampVal(large)), buffer.duplicate()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GTE, new VersionstampVal(medium)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GTE, new VersionstampVal(small)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GTE, new VersionstampVal(large)), buffer.duplicate()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LT, new VersionstampVal(large)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LT, new VersionstampVal(small)), buffer.duplicate()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LTE, new VersionstampVal(medium)), buffer.duplicate()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LTE, new VersionstampVal(large)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LTE, new VersionstampVal(small)), buffer.duplicate()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - IN/NIN Operations")
    class ResidualPredicateInNinTests {

        @Test
        void shouldHandleInOperationWithListOperand() {
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("electronics"), new StringVal("books"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.IN, categories);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleNinOperationWithListOperand() {
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("books"), new StringVal("clothing"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.NIN, categories);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForInOperationWhenValueNotInList() {
            BsonDocument doc = new BsonDocument("category", new BsonString("toys"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("electronics"), new StringVal("books"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.IN, categories);
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForNinOperationWhenValueInList() {
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("electronics"), new StringVal("books"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.NIN, categories);
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Array Fields")
    class ResidualPredicateArrayTests {

        @Test
        void shouldHandleArraySizeOperation() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("item1"),
                    new BsonString("item2"),
                    new BsonString("item3")
            ));
            BsonDocument doc = new BsonDocument("items", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedSize = List.of(new Int32Val(3));
            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, new ArrayVal(expectedSize));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleEmptyArraySizeOperation() {
            BsonDocument doc = new BsonDocument("items", new BsonArray());
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedSize = List.of(new Int32Val(0));
            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, new ArrayVal(expectedSize));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleArrayAllOperation() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana"),
                    new BsonString("cherry")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"), new StringVal("banana"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.ALL, new ArrayVal(expectedValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForAllWhenNotAllValuesExist() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"), new StringVal("cherry"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.ALL, new ArrayVal(expectedValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForNonArrayFieldWithSizeOperation() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedSize = List.of(new Int32Val(1));
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.SIZE, new ArrayVal(expectedSize));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleArrayNinOperationWhenNoMatch() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("cherry"), new StringVal("grape"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.NIN, new ArrayVal(expectedValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldHandleArrayNinOperationWhenMatch() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"), new StringVal("grape"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.NIN, new ArrayVal(expectedValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Edge Cases")
    class ResidualPredicateEdgeCases {

        @Test
        void shouldReturnFalseForNonExistentField() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, new Int32Val(20));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForFieldTypeMismatch() {
            BsonDocument doc = new BsonDocument("age", new BsonString("not-a-number"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, new Int32Val(20));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForDocumentValOperand() {
            BsonDocument doc = new BsonDocument("nested", new BsonDocument("key", new BsonString("value")));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "nested", Operator.EQ, new DocumentVal(java.util.Map.of()));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        void shouldReturnFalseForInt32FieldWithStringOperand() {
            BsonDocument doc = new BsonDocument("age", new BsonInt32(25));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "age", Operator.EQ, new StringVal("25")), buffer));
        }

        @Test
        void shouldReturnFalseForStringFieldWithInt32Operand() {
            BsonDocument doc = new BsonDocument("value", new BsonString("100"));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int32Val(100)), buffer));
        }

        @Test
        void shouldReturnFalseForInt32FieldWithInt64Operand() {
            BsonDocument doc = new BsonDocument("value", new BsonInt32(100));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int64Val(100L)), buffer));
        }

        @Test
        void shouldReturnFalseForDoubleFieldWithInt32Operand() {
            BsonDocument doc = new BsonDocument("value", new BsonDouble(100.0));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int32Val(100)), buffer));
        }

        @Test
        void shouldReturnFalseForBooleanFieldWithStringOperand() {
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.EQ, new StringVal("true")), buffer));
        }

        @Test
        void shouldReturnFalseForBinaryFieldWithStringOperand() {
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.EQ, new StringVal("binary")), buffer));
        }

        @Test
        void shouldReturnFalseForInt64FieldWithInt32Operand() {
            BsonDocument doc = new BsonDocument("value", new BsonInt64(100L));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int32Val(100)), buffer));
        }

        @Test
        void shouldReturnFalseForDoubleFieldWithDecimal128Operand() {
            BsonDocument doc = new BsonDocument("value", new BsonDouble(100.5));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Decimal128Val(new BigDecimal("100.5"))), buffer));
        }

        @Test
        void shouldReturnFalseForDecimal128FieldWithDoubleOperand() {
            BsonDocument doc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("100.5"))));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new DoubleVal(100.5)), buffer));
        }

        @Test
        void shouldReturnFalseForTimestampFieldWithInt64Operand() {
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.EQ, new Int64Val(ts.getValue())), buffer));
        }

        @Test
        void shouldReturnFalseForDateTimeFieldWithInt64Operand() {
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.EQ, new Int64Val(dateTime)), buffer));
        }

        @Test
        void shouldReturnFalseForDateTimeFieldWithTimestampOperand() {
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.EQ, new TimestampVal(dateTime)), buffer));
        }

        @Test
        void shouldReturnFalseForTimestampFieldWithDateTimeOperand() {
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.EQ, new DateTimeVal(ts.getValue())), buffer));
        }

        @Test
        void shouldReturnFalseForNullFieldWithNonNullOperand() {
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.EQ, new StringVal("value")), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.EQ, new Int32Val(0)), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.EQ, new BooleanVal(false)), buffer.duplicate()));
        }

        @Test
        void shouldReturnFalseForStringFieldWithDoubleOperand() {
            BsonDocument doc = new BsonDocument("value", new BsonString("3.14"));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new DoubleVal(3.14)), buffer));
        }

        @Test
        void shouldReturnFalseForBooleanFieldWithInt32Operand() {
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.EQ, new Int32Val(1)), buffer));
        }

        @Test
        void shouldReturnFalseForInt32FieldWithBooleanOperand() {
            BsonDocument doc = new BsonDocument("value", new BsonInt32(1));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new BooleanVal(true)), buffer));
        }

        @Test
        void shouldReturnFalseForInt64FieldWithDoubleOperand() {
            BsonDocument doc = new BsonDocument("value", new BsonInt64(100L));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new DoubleVal(100.0)), buffer));
        }

        @Test
        void shouldReturnFalseForDoubleFieldWithInt64Operand() {
            BsonDocument doc = new BsonDocument("value", new BsonDouble(100.0));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int64Val(100L)), buffer));
        }

        @Test
        void shouldReturnFalseForArrayFieldWithNonArrayOperandForEquality() {
            BsonArray array = new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2)));
            BsonDocument doc = new BsonDocument("items", array);
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "items", Operator.EQ, new StringVal("[1, 2]")), buffer.duplicate()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "items", Operator.EQ, new Int32Val(2)), buffer.duplicate()));
        }

        @Test
        void shouldReturnFalseForBinaryFieldWithBooleanOperand() {
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.EQ, new BooleanVal(true)), buffer));
        }

        @Test
        void shouldReturnFalseForDecimal128FieldWithInt32Operand() {
            BsonDocument doc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("100"))));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, new Int32Val(100)), buffer));
        }

        @Test
        void shouldReturnFalseForTypeMismatchWithOrderingOperators() {
            BsonDocument doc = new BsonDocument("value", new BsonInt32(50));
            ByteBuffer buffer = toBuffer(doc);

            // GT with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, new StringVal("40")), buffer.duplicate()));
            // GTE with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, new StringVal("50")), buffer.duplicate()));
            // LT with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, new StringVal("60")), buffer.duplicate()));
            // LTE with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, new StringVal("50")), buffer.duplicate()));
            // NE with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, new StringVal("50")), buffer.duplicate()));
        }
    }
}
