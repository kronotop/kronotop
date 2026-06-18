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

import com.apple.foundationdb.tuple.Versionstamp;
import com.ibm.icu.text.Collator;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.bson.BasicOutputBuffer;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PredicateEvaluatorTest {

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    private static Operand lit(BqlValue v) {
        return new Operand.Literal(v);
    }

    private static ByteBuffer toBuffer(BsonDocument doc) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            CODEC.encode(new BsonBinaryWriter(buffer), doc, EncoderContext.builder().build());
            return ByteBuffer.wrap(buffer.toByteArray());
        }
    }

    private static DocumentView toView(ByteBuffer buffer) {
        DocumentView view = new DocumentView();
        view.reset(null, buffer);
        return view;
    }


    // ==================== evaluateComparison Tests ====================

    @Nested
    @DisplayName("String Comparisons")
    class StringComparisons {

        @Test
        void shouldHandleEquality() {
            // Behavior: EQ returns true for identical strings, false otherwise.
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", "test"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", "other"));
        }

        @Test
        void shouldHandleInequality() {
            // Behavior: NE returns false for identical strings, true otherwise.
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, "test", "test"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, "test", "other"));
        }

        @Test
        void shouldHandleOrdering() {
            // Behavior: GT, GTE, LT, LTE compare strings lexicographically.
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
            // Behavior: EQ compares byte arrays element-by-element for equality.
            byte[] a = {1, 2, 3};
            byte[] b = {1, 2, 3};
            byte[] c = {1, 2, 4};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldHandleOrdering() {
            // Behavior: Byte arrays are compared lexicographically byte-by-byte.
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
            // Behavior: IN returns true if the byte array matches any array in the list.
            byte[] target = {1, 2, 3};
            byte[] match = {1, 2, 3};
            List<Object> list = List.of(match, new byte[]{4, 5, 6});

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.IN, target, list));
        }

        @Test
        void shouldHandleNinOperator() {
            // Behavior: NIN returns true if the byte array matches no array in the list.
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
            // Behavior: Int arrays are compared element-by-element; order matters.
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
            // Behavior: NE returns false for identical int arrays, true otherwise.
            int[] a = {1, 2, 3};
            int[] b = {1, 2, 3};
            int[] c = {1, 2, 4};

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, a, b));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, a, c));
        }

        @Test
        void shouldCompareLongArraysForEquality() {
            // Behavior: Long arrays are compared element-by-element; order matters.
            long[] a = {100L, 200L};
            long[] b = {100L, 200L};
            long[] c = {200L, 100L};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldCompareDoubleArraysForEquality() {
            // Behavior: Double arrays are compared element-by-element; order matters.
            double[] a = {1.1, 2.2};
            double[] b = {1.1, 2.2};
            double[] c = {2.2, 1.1};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldCompareObjectArraysForEquality() {
            // Behavior: Object arrays are compared element-by-element using equals; order matters.
            String[] a = {"apple", "banana"};
            String[] b = {"apple", "banana"};
            String[] c = {"banana", "apple"};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldCompareListsForEquality() {
            // Behavior: Lists are compared by content and order; size and order must match.
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
            // Behavior: Nested lists are compared recursively; inner list order matters.
            List<List<Integer>> a = List.of(List.of(1, 2), List.of(3, 4));
            List<List<Integer>> b = List.of(List.of(1, 2), List.of(3, 4));
            List<List<Integer>> c = List.of(List.of(1, 2), List.of(4, 3));

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldReturnFalseForDifferentArrayTypes() {
            // Behavior: Arrays of different primitive types are not equal even with same values.
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
            // Behavior: ArrayVal objects are compared by their element values in order.
            ArrayVal a = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal b = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal c = new ArrayVal(List.of(new StringVal("apple"), new StringVal("cherry")));

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldRespectOrderInArrayVals() {
            // Behavior: ArrayVal equality is order-sensitive; different order means not equal.
            ArrayVal a = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal b = new ArrayVal(List.of(new StringVal("banana"), new StringVal("apple")));

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
        }

        @Test
        void shouldCompareDifferentSizedArrayVals() {
            // Behavior: ArrayVal objects with different sizes are not equal.
            ArrayVal a = new ArrayVal(List.of(new StringVal("apple"), new StringVal("banana")));
            ArrayVal b = new ArrayVal(List.of(new StringVal("apple")));

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
        }

        @Test
        void shouldCompareEmptyArrayVals() {
            // Behavior: Two empty ArrayVal objects are equal.
            ArrayVal a = new ArrayVal(List.of());
            ArrayVal b = new ArrayVal(List.of());

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
        }

        @Test
        void shouldCompareNestedArrayVals() {
            // Behavior: Nested ArrayVal objects are compared recursively element-by-element.
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
            // Behavior: ArrayVal with mixed element types compares each element by type and value.
            ArrayVal a = new ArrayVal(List.of(new StringVal("test"), new Int32Val(42), new BooleanVal(true)));
            ArrayVal b = new ArrayVal(List.of(new StringVal("test"), new Int32Val(42), new BooleanVal(true)));
            ArrayVal c = new ArrayVal(List.of(new StringVal("test"), new Int32Val(42), new BooleanVal(false)));

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, a, b));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, a, c));
        }

        @Test
        void shouldHandleNeOperatorForArrayVals() {
            // Behavior: NE returns false for identical ArrayVal, true when different.
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
            // Behavior: EQ returns true for identical integers, false otherwise.
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, 42, 42));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, 42, 43));
        }

        @Test
        void shouldHandleOrdering() {
            // Behavior: GT, GTE, LT, LTE compare integers numerically.
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
            // Behavior: GT, GTE, LT, LTE compare long values numerically.
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
            // Behavior: GT, GTE, LT, LTE compare double values numerically.
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
            // Behavior: GT, GTE, LT, LTE compare BigDecimal values numerically.
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
            // Behavior: Versionstamps are compared byte-by-byte lexicographically.
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
    @DisplayName("ObjectId Comparisons")
    class ObjectIdComparisons {

        @Test
        void shouldHandleEquality() {
            // Behavior: EQ returns true for identical ObjectIds, false for different ones.
            ObjectId id1 = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
            ObjectId id2 = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
            ObjectId id3 = new ObjectId(new byte[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9});

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, id1, id2));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, id1, id3));
        }

        @Test
        void shouldHandleInequality() {
            // Behavior: NE returns false for identical ObjectIds, true for different ones.
            ObjectId id1 = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
            ObjectId id2 = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
            ObjectId id3 = new ObjectId(new byte[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9});

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, id1, id2));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, id1, id3));
        }

        @Test
        void shouldHandleOrdering() {
            // Behavior: ObjectIds are compared using their natural Comparable ordering.
            byte[] bytes1 = new byte[12];
            byte[] bytes2 = new byte[12];
            byte[] bytes3 = new byte[12];
            bytes1[0] = 1;
            bytes2[0] = 2;
            bytes3[0] = 1;

            ObjectId small = new ObjectId(bytes1);
            ObjectId large = new ObjectId(bytes2);
            ObjectId equal = new ObjectId(bytes3);

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
            // Behavior: Two nulls are equal; EQ returns true, NE returns false.
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, null, null));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, null, null));
        }

        @Test
        void shouldHandleOneNull() {
            // Behavior: One null and one non-null are not equal; EQ returns false, NE returns true.
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", null));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, "test", null));
        }

        @Test
        void shouldReturnFalseForOrderingWithNull() {
            // Behavior: Ordering comparisons (GT, LT, etc.) with null always return false.
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, null, "test"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, "test", null));
        }
    }

    @Nested
    @DisplayName("IN/NIN Operators")
    class InNinOperators {

        @Test
        void shouldHandleIn() {
            // Behavior: IN returns true if the value is in the list, false otherwise.
            List<Object> list = List.of("apple", "banana");
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.IN, "apple", list));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.IN, "grape", list));
        }

        @Test
        void shouldHandleNin() {
            // Behavior: NIN returns true if the value is not in the list, false if it is.
            List<Object> list = List.of("apple", "banana");
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NIN, "grape", list));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NIN, "apple", list));
        }

        @Test
        void shouldReturnFalseForNonListOperand() {
            // Behavior: IN returns false and NIN returns true when operand is not a list.
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.IN, "test", "not-a-list"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NIN, "test", "not-a-list"));
        }
    }

    @Nested
    @DisplayName("Error Cases")
    class ErrorCases {

        @Test
        void shouldThrowForUnsupportedOperator() {
            // Behavior: SIZE operator is not supported in evaluateComparison and throws exception.
            assertThrows(UnsupportedOperationException.class, () ->
                    PredicateEvaluator.evaluateComparison(Operator.SIZE, "test", "test"));
        }

        @Test
        void shouldReturnFalseForNonComparableType() {
            // Behavior: Non-comparable types return false for ordering comparisons.
            Object a = new Object();
            Object b = new Object();
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, a, b));
        }

        @Test
        void shouldReturnFalseForTypeMismatch() {
            // Behavior: Comparisons between different types always return false.
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
            // Behavior: EQ matches when document string field equals the operand.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, lit(new StringVal("John")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchStringEquality() {
            // Behavior: EQ does not match when document string field differs from operand.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, lit(new StringVal("Jane")));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleStringGtComparison() {
            // Behavior: GT matches when document string is lexicographically greater than operand.
            BsonDocument doc = new BsonDocument("name", new BsonString("zebra"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.GT, lit(new StringVal("apple")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleStringLtComparison() {
            // Behavior: LT matches when document string is lexicographically less than operand.
            BsonDocument doc = new BsonDocument("name", new BsonString("apple"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.LT, lit(new StringVal("zebra")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleStringGteComparison() {
            // Behavior: GTE matches when document string is greater than or equal to operand.
            BsonDocument doc = new BsonDocument("name", new BsonString("banana"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.GTE, lit(new StringVal("banana"))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.GTE, lit(new StringVal("apple"))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.GTE, lit(new StringVal("cherry"))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldHandleStringLteComparison() {
            // Behavior: LTE matches when document string is less than or equal to operand.
            BsonDocument doc = new BsonDocument("name", new BsonString("banana"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.LTE, lit(new StringVal("banana"))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.LTE, lit(new StringVal("cherry"))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.LTE, lit(new StringVal("apple"))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldHandleStringNeComparison() {
            // Behavior: NE matches when document string differs from operand.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.NE, lit(new StringVal("Jane"))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "name", Operator.NE, lit(new StringVal("John"))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Regex Fields")
    class ResidualPredicateRegexTests {

        private ResidualPredicate regex(String selector, String pattern, String options) {
            return new ResidualPredicate(1, selector, Operator.REGEX, lit(new RegexVal(pattern, options)));
        }

        @Test
        void shouldMatchStringFieldWithRegex() {
            // Behavior: $regex matches when the string field matches the pattern.
            BsonDocument doc = new BsonDocument("name", new BsonString("Alice"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    regex("name", "^Al", ""), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchStringFieldWhenRegexDiffers() {
            // Behavior: $regex does not match when the string field does not match the pattern.
            BsonDocument doc = new BsonDocument("name", new BsonString("Bob"));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    regex("name", "^Al", ""), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchStringFieldCaseInsensitivelyWithIOption() {
            // Behavior: the i option makes $regex matching case-insensitive.
            BsonDocument doc = new BsonDocument("name", new BsonString("ALICE"));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    regex("name", "^alice$", "i"), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchRegexAgainstNonStringField() {
            // Behavior: $regex never matches a non-string field (no type coercion).
            BsonDocument doc = new BsonDocument("name", new BsonInt32(42));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    regex("name", "42", ""), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchRegexAgainstArrayElement() {
            // Behavior: $regex on an array field matches if any string element matches the pattern.
            BsonArray tags = new BsonArray(Arrays.asList(
                    new BsonString("java"),
                    new BsonString("python")
            ));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    regex("tags", "^py", ""), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchRegexWhenNoArrayElementMatches() {
            // Behavior: $regex on an array field returns false when no string element matches.
            BsonArray tags = new BsonArray(Arrays.asList(
                    new BsonString("java"),
                    new BsonString("go")
            ));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    regex("tags", "^py", ""), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldRespectMultilineOption() {
            // Behavior: the m option makes ^ and $ match line boundaries, changing the result.
            BsonDocument doc = new BsonDocument("text", new BsonString("first line\nsecond line"));

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    regex("text", "^second", "m"), toView(toBuffer(doc)), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    regex("text", "^second", ""), toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldRespectDotallOption() {
            // Behavior: the s option makes . match newlines, changing the result.
            BsonDocument doc = new BsonDocument("text", new BsonString("a\nb"));

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    regex("text", "a.b", "s"), toView(toBuffer(doc)), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    regex("text", "a.b", ""), toView(toBuffer(doc)), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Regex in $in/$nin/$all")
    class ResidualPredicateRegexListTests {

        private ResidualPredicate listPred(String selector, Operator op, BqlValue... values) {
            return new ResidualPredicate(1, selector, op, new Operand.LiteralList(List.of(values)));
        }

        private boolean test(ResidualPredicate pred, BsonDocument doc) {
            return PredicateEvaluator.testResidualPredicate(pred, toView(toBuffer(doc)), Collections.emptyList());
        }

        @Test
        void shouldMatchScalarFieldWithRegexElementInIn() {
            // Behavior: $in with a regex element matches a string field whose value matches the pattern.
            BsonDocument doc = new BsonDocument("name", new BsonString("Alice"));
            assertTrue(test(listPred("name", Operator.IN, new RegexVal("^Al", "")), doc));
        }

        @Test
        void shouldNotMatchScalarFieldWhenRegexElementDiffers() {
            // Behavior: $in with a regex element does not match when the pattern fails.
            BsonDocument doc = new BsonDocument("name", new BsonString("Bob"));
            assertFalse(test(listPred("name", Operator.IN, new RegexVal("^Al", "")), doc));
        }

        @Test
        void shouldMatchScalarFieldViaEitherRegexOrLiteralInIn() {
            // Behavior: $in mixing a regex and a literal matches when either alternative holds.
            assertTrue(test(listPred("name", Operator.IN, new RegexVal("^Al", ""), new StringVal("Bob")),
                    new BsonDocument("name", new BsonString("Bob"))));
            assertTrue(test(listPred("name", Operator.IN, new RegexVal("^Al", ""), new StringVal("Bob")),
                    new BsonDocument("name", new BsonString("Alice"))));
        }

        @Test
        void shouldMatchCaseInsensitiveRegexElementInIn() {
            // Behavior: the i option applies to a regex element inside $in.
            BsonDocument doc = new BsonDocument("name", new BsonString("ALICE"));
            assertTrue(test(listPred("name", Operator.IN, new RegexVal("^alice$", "i")), doc));
        }

        @Test
        void shouldNotMatchRegexElementAgainstNonStringField() {
            // Behavior: a regex element never matches a non-string field (no type coercion).
            BsonDocument doc = new BsonDocument("code", new BsonInt32(42));
            assertFalse(test(listPred("code", Operator.IN, new RegexVal("42", "")), doc));
        }

        @Test
        void shouldMatchArrayFieldWhenAnyElementMatchesRegexElementInIn() {
            // Behavior: $in with a regex element matches an array field if any string element matches.
            BsonDocument doc = new BsonDocument("tags",
                    new BsonArray(Arrays.asList(new BsonString("java"), new BsonString("python"))));
            assertTrue(test(listPred("tags", Operator.IN, new RegexVal("^py", "")), doc));
        }

        @Test
        void shouldNegateRegexElementWithNin() {
            // Behavior: $nin with a regex element excludes documents whose field matches the pattern.
            assertFalse(test(listPred("name", Operator.NIN, new RegexVal("^Al", "")),
                    new BsonDocument("name", new BsonString("Alice"))));
            assertTrue(test(listPred("name", Operator.NIN, new RegexVal("^Al", "")),
                    new BsonDocument("name", new BsonString("Bob"))));
        }

        @Test
        void shouldRequireEveryRegexElementToMatchInAll() {
            // Behavior: $all with regex elements requires each pattern to match some string array element.
            BsonDocument matching = new BsonDocument("tags",
                    new BsonArray(Arrays.asList(new BsonString("alpha"), new BsonString("beta"))));
            assertTrue(test(listPred("tags", Operator.ALL, new RegexVal("^al", ""), new RegexVal("^be", "")), matching));

            BsonDocument partial = new BsonDocument("tags",
                    new BsonArray(Arrays.asList(new BsonString("alpha"), new BsonString("gamma"))));
            assertFalse(test(listPred("tags", Operator.ALL, new RegexVal("^al", ""), new RegexVal("^be", "")), partial));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Numeric Fields")
    class ResidualPredicateNumericTests {

        @Test
        void shouldCompareInt32Field() {
            // Behavior: GT on Int32 field matches when the document value is greater than operand.
            BsonDocument doc = new BsonDocument("age", new BsonInt32(25));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, lit(new Int32Val(20)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt64Field() {
            // Behavior: LTE on the Int64 field matches when the document value is less than or equal to operand.
            BsonDocument doc = new BsonDocument("timestamp", new BsonInt64(1234567890L));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "timestamp", Operator.LTE, lit(new Int64Val(2000000000L)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareDoubleField() {
            // Behavior: LT on Double field matches when document value is less than operand.
            BsonDocument doc = new BsonDocument("price", new BsonDouble(19.99));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "price", Operator.LT, lit(new DoubleVal(20.0)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareDecimal128Field() {
            // Behavior: GTE on Decimal128 field matches when document value is greater than or equal to operand.
            BsonDocument doc = new BsonDocument("amount", new BsonDecimal128(new Decimal128(new BigDecimal("100.50"))));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "amount", Operator.GTE, lit(new Decimal128Val(new BigDecimal("100.00"))));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt64AllOperators() {
            // Behavior: All comparison operators work correctly on Int64 fields.
            BsonDocument doc = new BsonDocument("value", new BsonInt64(1000000L));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val(1000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val(999999L))), toView(buffer.duplicate()), Collections.emptyList()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new Int64Val(999999L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new Int64Val(1000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(500000L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(2000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int64Val(1000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int64Val(500000L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int64Val(2000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val(2000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val(500000L))), toView(buffer.duplicate()), Collections.emptyList()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(1000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(2000000L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(500000L))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareDecimal128AllOperators() {
            // Behavior: All comparison operators work correctly on Decimal128 fields.
            BsonDocument doc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("100.50"))));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Decimal128Val(new BigDecimal("100.50")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Decimal128Val(new BigDecimal("100.49")))), toView(buffer.duplicate()), Collections.emptyList()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new Decimal128Val(new BigDecimal("100.49")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new Decimal128Val(new BigDecimal("100.50")))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("50.00")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("200.00")))), toView(buffer.duplicate()), Collections.emptyList()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("100.50")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("50.00")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("200.00")))), toView(buffer.duplicate()), Collections.emptyList()));
            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("200.00")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("50.00")))), toView(buffer.duplicate()), Collections.emptyList()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("100.50")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("200.00")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("50.00")))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt32AllOperators() {
            // Behavior: All comparison operators work correctly on Int32 fields.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(50));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int32Val(50))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int32Val(51))), toView(buffer.duplicate()), Collections.emptyList()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new Int32Val(51))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new Int32Val(50))), toView(buffer.duplicate()), Collections.emptyList()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int32Val(50))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int32Val(40))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int32Val(60))), toView(buffer.duplicate()), Collections.emptyList()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(50))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(60))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(40))), toView(buffer.duplicate()), Collections.emptyList()));
            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int32Val(60))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int32Val(50))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareDoubleAllOperators() {
            // Behavior: All comparison operators work correctly on Double fields.
            BsonDocument doc = new BsonDocument("value", new BsonDouble(3.14));
            ByteBuffer buffer = toBuffer(doc);

            // EQ
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal(3.14))), toView(buffer.duplicate()), Collections.emptyList()));
            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new DoubleVal(2.71))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal(2.0))), toView(buffer.duplicate()), Collections.emptyList()));
            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new DoubleVal(3.14))), toView(buffer.duplicate()), Collections.emptyList()));
            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new DoubleVal(3.14))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        // --- Cross-type ordering operators (numeric widening) ---

        @Test
        void shouldCompareInt32WithInt64AllOrderingOperators() {
            // Behavior: All ordering operators work across INT32 and INT64 via numeric widening (common type: INT64).

            // INT32 field with INT64 operand
            BsonDocument doc = new BsonDocument("value", new BsonInt32(50));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(40L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(60L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int64Val(50L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int64Val(60L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val(60L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val(40L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(50L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(40L))), toView(buffer.duplicate()), Collections.emptyList()));

            // INT64 field with INT32 operand (reverse direction)
            BsonDocument reverseDoc = new BsonDocument("value", new BsonInt64(50L));
            ByteBuffer reverseBuffer = toBuffer(reverseDoc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int32Val(40))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int32Val(60))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int32Val(50))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int32Val(60))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(50))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(40))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt32WithDoubleAllOrderingOperators() {
            // Behavior: All ordering operators work across INT32 and DOUBLE via numeric widening (common type: DOUBLE).

            // INT32 field with DOUBLE operand
            BsonDocument doc = new BsonDocument("value", new BsonInt32(50));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal(40.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal(60.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new DoubleVal(50.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new DoubleVal(60.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new DoubleVal(60.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new DoubleVal(40.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new DoubleVal(50.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new DoubleVal(40.0))), toView(buffer.duplicate()), Collections.emptyList()));

            // DOUBLE field with INT32 operand (reverse direction)
            BsonDocument reverseDoc = new BsonDocument("value", new BsonDouble(50.0));
            ByteBuffer reverseBuffer = toBuffer(reverseDoc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int32Val(40))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int32Val(60))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int32Val(50))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int32Val(60))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(50))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(40))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt32WithDecimal128AllOrderingOperators() {
            // Behavior: All ordering operators work across INT32 and DECIMAL128 via numeric widening (common type: DECIMAL128).

            // INT32 field with DECIMAL128 operand
            BsonDocument doc = new BsonDocument("value", new BsonInt32(50));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("40")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("60")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("50")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("60")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("60")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("40")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("50")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("40")))), toView(buffer.duplicate()), Collections.emptyList()));

            // DECIMAL128 field with INT32 operand (reverse direction)
            BsonDocument reverseDoc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("50"))));
            ByteBuffer reverseBuffer = toBuffer(reverseDoc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int32Val(40))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int32Val(60))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int32Val(50))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int32Val(60))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(50))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int32Val(40))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt64WithDoubleAllOrderingOperators() {
            // Behavior: All ordering operators work across INT64 and DOUBLE via numeric widening.
            // Both sides promote to DECIMAL128 because INT64->DOUBLE is lossy (exceeds 53-bit mantissa).

            // INT64 field with DOUBLE operand
            BsonDocument doc = new BsonDocument("value", new BsonInt64(50L));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal(40.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal(60.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new DoubleVal(50.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new DoubleVal(60.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new DoubleVal(60.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new DoubleVal(40.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new DoubleVal(50.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new DoubleVal(40.0))), toView(buffer.duplicate()), Collections.emptyList()));

            // DOUBLE field with INT64 operand (reverse direction)
            BsonDocument reverseDoc = new BsonDocument("value", new BsonDouble(50.0));
            ByteBuffer reverseBuffer = toBuffer(reverseDoc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(40L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(60L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int64Val(50L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val(60L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(50L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(40L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt64WithDecimal128AllOrderingOperators() {
            // Behavior: All ordering operators work across INT64 and DECIMAL128 via numeric widening (common type: DECIMAL128).

            // INT64 field with DECIMAL128 operand
            BsonDocument doc = new BsonDocument("value", new BsonInt64(50L));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("40")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("60")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("50")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("60")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("60")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("40")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("50")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("40")))), toView(buffer.duplicate()), Collections.emptyList()));

            // DECIMAL128 field with INT64 operand (reverse direction)
            BsonDocument reverseDoc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("50"))));
            ByteBuffer reverseBuffer = toBuffer(reverseDoc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(40L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val(60L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Int64Val(50L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val(60L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(50L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Int64Val(40L))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareDoubleWithDecimal128AllOrderingOperators() {
            // Behavior: All ordering operators work across DOUBLE and DECIMAL128 via numeric widening (common type: DECIMAL128).

            // DOUBLE field with DECIMAL128 operand
            BsonDocument doc = new BsonDocument("value", new BsonDouble(50.5));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("40.5")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Decimal128Val(new BigDecimal("60.5")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("50.5")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new Decimal128Val(new BigDecimal("60.5")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("60.5")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Decimal128Val(new BigDecimal("40.5")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("50.5")))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new Decimal128Val(new BigDecimal("40.5")))), toView(buffer.duplicate()), Collections.emptyList()));

            // DECIMAL128 field with DOUBLE operand (reverse direction)
            BsonDocument reverseDoc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("50.5"))));
            ByteBuffer reverseBuffer = toBuffer(reverseDoc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal(40.5))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal(60.5))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new DoubleVal(50.5))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new DoubleVal(60.5))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new DoubleVal(50.5))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new DoubleVal(40.5))), toView(reverseBuffer.duplicate()), Collections.emptyList()));
        }

        // --- Boundary tests ---

        @Test
        void shouldCompareInt32MaxValueWithInt64Operand() {
            // Behavior: INT32 MAX_VALUE widens to INT64 without overflow.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(Integer.MAX_VALUE));
            ByteBuffer buffer = toBuffer(doc);

            // EQ with same value as INT64
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val((long) Integer.MAX_VALUE))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT with MAX_VALUE+1 should be false (field is less)
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val((long) Integer.MAX_VALUE + 1))), toView(buffer.duplicate()), Collections.emptyList()));
            // LT with MAX_VALUE+1 should be true
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val((long) Integer.MAX_VALUE + 1))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareInt32MinValueWithInt64Operand() {
            // Behavior: INT32 MIN_VALUE widens to INT64 without overflow.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(Integer.MIN_VALUE));
            ByteBuffer buffer = toBuffer(doc);

            // EQ with same value as INT64
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val((long) Integer.MIN_VALUE))), toView(buffer.duplicate()), Collections.emptyList()));
            // LT with MIN_VALUE-1 should be false (field is greater)
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val((long) Integer.MIN_VALUE - 1))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT with MIN_VALUE-1 should be true
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new Int64Val((long) Integer.MIN_VALUE - 1))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareLargeInt64WithDoubleOperand() {
            // Behavior: (1L << 53) + 1 is the first integer not exactly representable as double.
            // Widening to DECIMAL128 preserves precision that a direct INT64->DOUBLE cast would lose.
            long preciseValue = (1L << 53) + 1;
            BsonDocument doc = new BsonDocument("value", new BsonInt64(preciseValue));
            ByteBuffer buffer = toBuffer(doc);

            // The double cast loses precision: (double) preciseValue collapses to (1L << 53)
            // After widening both to DECIMAL128, they should NOT be equal
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal((double) preciseValue))), toView(buffer.duplicate()), Collections.emptyList()));
            // The INT64 value is greater than the truncated double
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new DoubleVal((double) preciseValue))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareNegativeNumbersAcrossTypes() {
            // Behavior: Negative numbers widen correctly across all numeric types.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(-10));
            ByteBuffer buffer = toBuffer(doc);

            // INT32 -10 matches INT64 -10
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val(-10L))), toView(buffer.duplicate()), Collections.emptyList()));
            // INT32 -10 matches DOUBLE -10.0
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal(-10.0))), toView(buffer.duplicate()), Collections.emptyList()));
            // INT32 -10 matches DECIMAL128 -10
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Decimal128Val(new BigDecimal("-10")))), toView(buffer.duplicate()), Collections.emptyList()));
            // INT32 -10 < INT64 -5
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new Int64Val(-5L))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareZeroAcrossTypes() {
            // Behavior: Zero widens correctly across all numeric types.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(0));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val(0L))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal(0.0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Decimal128Val(BigDecimal.ZERO))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Boolean Fields")
    class ResidualPredicateBooleanTests {

        @Test
        void shouldCompareBooleanField() {
            // Behavior: EQ on Boolean field matches when document value equals operand.
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "active", Operator.EQ, lit(new BooleanVal(true)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareBooleanFieldNe() {
            // Behavior: NE on Boolean field matches when document value differs from operand.
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "active", Operator.NE, lit(new BooleanVal(false)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForUnsupportedBooleanOperators() {
            // Behavior: GT, GTE, LT, LTE are not supported for booleans and return false.
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            // GT, GTE, LT, LTE are not supported for booleans - should return false
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.GT, lit(new BooleanVal(false))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.GTE, lit(new BooleanVal(false))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.LT, lit(new BooleanVal(true))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.LTE, lit(new BooleanVal(true))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Null Fields")
    class ResidualPredicateNullTests {

        @Test
        void shouldCompareNullField() {
            // Behavior: EQ with NullVal matches document fields that are explicitly null.
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "optional", Operator.EQ, lit(NullVal.INSTANCE));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnTrueForMissingFieldWithEqNull() {
            // Behavior: EQ with NullVal also matches missing fields (treated as null).
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "nonexistent", Operator.EQ, lit(NullVal.INSTANCE));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleExistsOperator() {
            // Behavior: EXISTS true matches when the field is present in the document.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EXISTS, lit(new BooleanVal(true)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnTrueForNullFieldWithExists() {
            // Behavior: EXISTS true matches when field exists even if value is null.
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "optional", Operator.EXISTS, lit(new BooleanVal(true)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleNullNeComparison() {
            // Behavior: NE with NullVal returns false for null fields, true for non-null fields.
            BsonDocument docWithNull = new BsonDocument("optional", new BsonNull());
            ByteBuffer bufferWithNull = toBuffer(docWithNull);

            // NE on null field with NullVal should return false (they are equal)
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.NE, lit(NullVal.INSTANCE)), toView(bufferWithNull), Collections.emptyList()));

            // NE on non-null field with NullVal should return true
            BsonDocument docWithValue = new BsonDocument("optional", new BsonString("value"));
            ByteBuffer bufferWithValue = toBuffer(docWithValue);
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.NE, lit(NullVal.INSTANCE)), toView(bufferWithValue), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForExistingFieldWithExistsFalse() {
            // Behavior: EXISTS false returns false when the field exists.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EXISTS, lit(new BooleanVal(false)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnTrueForMissingFieldWithExistsFalse() {
            // Behavior: EXISTS false returns true when the field is missing.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "nonexistent", Operator.EXISTS, lit(new BooleanVal(false)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForNullFieldWithExistsFalse() {
            // Behavior: EXISTS false returns false for null field since the field key exists.
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "optional", Operator.EXISTS, lit(new BooleanVal(false)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnTrueForMissingFieldWithNeNonNull() {
            // Behavior: NE with non-null operand returns true for missing fields.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            // Missing field with NE and non-null operand should return true
            ResidualPredicate predicate = new ResidualPredicate(1, "nonexistent", Operator.NE, lit(new StringVal("value")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Binary Fields")
    class ResidualPredicateBinaryTests {

        @Test
        void shouldCompareBinaryField() {
            // Behavior: EQ on Binary field matches when byte arrays are identical.
            byte[] data = {1, 2, 3, 4, 5};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "data", Operator.EQ, lit(new BinaryVal(data)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareBinaryFieldNe() {
            // Behavior: NE on Binary field matches when byte arrays differ.
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.NE, lit(new BinaryVal(new byte[]{4, 5, 6}))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.NE, lit(new BinaryVal(new byte[]{1, 2, 3}))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareBinaryFieldOrdering() {
            // Behavior: Binary fields support ordering comparisons using byte-by-byte comparison.
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GT, lit(new BinaryVal(new byte[]{1, 2, 2}))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GT, lit(new BinaryVal(new byte[]{1, 2, 4}))), toView(buffer.duplicate()), Collections.emptyList()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GTE, lit(new BinaryVal(new byte[]{1, 2, 3}))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GTE, lit(new BinaryVal(new byte[]{1, 2, 2}))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.GTE, lit(new BinaryVal(new byte[]{1, 2, 4}))), toView(buffer.duplicate()), Collections.emptyList()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LT, lit(new BinaryVal(new byte[]{1, 2, 4}))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LT, lit(new BinaryVal(new byte[]{1, 2, 2}))), toView(buffer.duplicate()), Collections.emptyList()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LTE, lit(new BinaryVal(new byte[]{1, 2, 3}))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LTE, lit(new BinaryVal(new byte[]{1, 2, 4}))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.LTE, lit(new BinaryVal(new byte[]{1, 2, 2}))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Timestamp/DateTime Fields")
    class ResidualPredicateTimestampTests {

        @Test
        void shouldCompareTimestampField() {
            // Behavior: EQ on Timestamp field matches when values are identical.
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "created", Operator.EQ, lit(new TimestampVal(ts.getValue())));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareDateTimeField() {
            // Behavior: EQ on DateTime field matches when millisecond values are identical.
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "eventTime", Operator.EQ, lit(new DateTimeVal(dateTime)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareTimestampFieldAllOperators() {
            // Behavior: All comparison operators work correctly on Timestamp fields.
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);
            long value = ts.getValue();

            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.NE, lit(new TimestampVal(value + 1))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.NE, lit(new TimestampVal(value))), toView(buffer.duplicate()), Collections.emptyList()));

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GT, lit(new TimestampVal(value - 1))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GT, lit(new TimestampVal(value))), toView(buffer.duplicate()), Collections.emptyList()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GTE, lit(new TimestampVal(value))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GTE, lit(new TimestampVal(value - 1))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.GTE, lit(new TimestampVal(value + 1))), toView(buffer.duplicate()), Collections.emptyList()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LT, lit(new TimestampVal(value + 1))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LT, lit(new TimestampVal(value))), toView(buffer.duplicate()), Collections.emptyList()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LTE, lit(new TimestampVal(value))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LTE, lit(new TimestampVal(value + 1))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.LTE, lit(new TimestampVal(value - 1))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldCompareDateTimeFieldAllOperators() {
            // Behavior: All comparison operators work correctly on DateTime fields.
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.NE, lit(new DateTimeVal(dateTime + 1000))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.NE, lit(new DateTimeVal(dateTime))), toView(buffer.duplicate()), Collections.emptyList()));

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GT, lit(new DateTimeVal(dateTime - 1000))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GT, lit(new DateTimeVal(dateTime))), toView(buffer.duplicate()), Collections.emptyList()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GTE, lit(new DateTimeVal(dateTime))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GTE, lit(new DateTimeVal(dateTime - 1000))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.GTE, lit(new DateTimeVal(dateTime + 1000))), toView(buffer.duplicate()), Collections.emptyList()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LT, lit(new DateTimeVal(dateTime + 1000))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LT, lit(new DateTimeVal(dateTime))), toView(buffer.duplicate()), Collections.emptyList()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LTE, lit(new DateTimeVal(dateTime))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LTE, lit(new DateTimeVal(dateTime + 1000))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.LTE, lit(new DateTimeVal(dateTime - 1000))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Versionstamp Fields")
    class ResidualPredicateVersionstampTests {

        @Test
        void shouldCompareVersionstampField() {
            // Behavior: EQ on Versionstamp field matches when byte values are identical.
            byte[] bytes = new byte[12];
            Arrays.fill(bytes, (byte) 1);
            Versionstamp vs = Versionstamp.fromBytes(bytes);

            BsonDocument doc = new BsonDocument("version", new BsonBinary(BsonBinarySubType.BINARY, bytes));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "version", Operator.EQ, lit(new VersionstampVal(vs)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareVersionstampFieldWithOrdering() {
            // Behavior: Versionstamp fields support ordering comparisons byte-by-byte.
            byte[] bytes1 = new byte[12];
            byte[] bytes2 = new byte[12];
            bytes1[0] = 1;
            bytes2[0] = 2;

            BsonDocument doc = new BsonDocument("version", new BsonBinary(BsonBinarySubType.BINARY, bytes1));
            ByteBuffer buffer = toBuffer(doc);

            Versionstamp expected = Versionstamp.fromBytes(bytes2);
            ResidualPredicate predicate = new ResidualPredicate(1, "version", Operator.LT, lit(new VersionstampVal(expected)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareVersionstampFieldAllOperators() {
            // Behavior: All comparison operators work correctly on Versionstamp fields.
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
                    new ResidualPredicate(1, "version", Operator.NE, lit(new VersionstampVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.NE, lit(new VersionstampVal(medium))), toView(buffer.duplicate()), Collections.emptyList()));

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GT, lit(new VersionstampVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GT, lit(new VersionstampVal(large))), toView(buffer.duplicate()), Collections.emptyList()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GTE, lit(new VersionstampVal(medium))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GTE, lit(new VersionstampVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.GTE, lit(new VersionstampVal(large))), toView(buffer.duplicate()), Collections.emptyList()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LT, lit(new VersionstampVal(large))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LT, lit(new VersionstampVal(small))), toView(buffer.duplicate()), Collections.emptyList()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LTE, lit(new VersionstampVal(medium))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LTE, lit(new VersionstampVal(large))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "version", Operator.LTE, lit(new VersionstampVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - ObjectId")
    class ResidualPredicateObjectIdTests {

        @Test
        void shouldCompareObjectIdField() {
            // Behavior: EQ on ObjectId field matches when values are identical.
            ObjectId objectId = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});

            BsonDocument doc = new BsonDocument("refId", new BsonObjectId(objectId));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "refId", Operator.EQ, lit(new ObjectIdVal(objectId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareObjectIdFieldWithOrdering() {
            // Behavior: ObjectId fields support ordering comparisons.
            byte[] bytes1 = new byte[12];
            byte[] bytes2 = new byte[12];
            bytes1[0] = 1;
            bytes2[0] = 2;

            ObjectId smallId = new ObjectId(bytes1);
            ObjectId largeId = new ObjectId(bytes2);

            BsonDocument doc = new BsonDocument("refId", new BsonObjectId(smallId));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "refId", Operator.LT, lit(new ObjectIdVal(largeId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldCompareObjectIdFieldAllOperators() {
            // Behavior: All comparison operators work correctly on ObjectId fields.
            byte[] bytesSmall = new byte[12];
            byte[] bytesMedium = new byte[12];
            byte[] bytesLarge = new byte[12];
            bytesSmall[0] = 1;
            bytesMedium[0] = 2;
            bytesLarge[0] = 3;

            ObjectId small = new ObjectId(bytesSmall);
            ObjectId medium = new ObjectId(bytesMedium);
            ObjectId large = new ObjectId(bytesLarge);

            BsonDocument doc = new BsonDocument("refId", new BsonObjectId(medium));
            ByteBuffer buffer = toBuffer(doc);

            // NE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.NE, lit(new ObjectIdVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.NE, lit(new ObjectIdVal(medium))), toView(buffer.duplicate()), Collections.emptyList()));

            // GT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.GT, lit(new ObjectIdVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.GT, lit(new ObjectIdVal(large))), toView(buffer.duplicate()), Collections.emptyList()));

            // GTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.GTE, lit(new ObjectIdVal(medium))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.GTE, lit(new ObjectIdVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.GTE, lit(new ObjectIdVal(large))), toView(buffer.duplicate()), Collections.emptyList()));

            // LT
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.LT, lit(new ObjectIdVal(large))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.LT, lit(new ObjectIdVal(small))), toView(buffer.duplicate()), Collections.emptyList()));

            // LTE
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.LTE, lit(new ObjectIdVal(medium))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.LTE, lit(new ObjectIdVal(large))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "refId", Operator.LTE, lit(new ObjectIdVal(small))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForTypeMismatch() {
            // Behavior: ObjectId field compared with StringVal returns false for EQ.
            ObjectId objectId = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});

            BsonDocument doc = new BsonDocument("refId", new BsonObjectId(objectId));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "refId", Operator.EQ, lit(new StringVal("not-an-objectid")));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleInWithObjectId() {
            // Behavior: IN operator matches when ObjectId field value is in the operand list.
            ObjectId id1 = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
            ObjectId id2 = new ObjectId(new byte[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9});

            BsonDocument doc = new BsonDocument("refId", new BsonObjectId(id1));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> ids = List.of(new ObjectIdVal(id1), new ObjectIdVal(id2));
            ResidualPredicate predicate = new ResidualPredicate(1, "refId", Operator.IN, new Operand.LiteralList(ids));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleNinWithObjectId() {
            // Behavior: NIN operator excludes when ObjectId field value is in the operand list.
            ObjectId id1 = new ObjectId(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
            ObjectId id2 = new ObjectId(new byte[]{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9});

            BsonDocument doc = new BsonDocument("refId", new BsonObjectId(id1));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> ids = List.of(new ObjectIdVal(id1), new ObjectIdVal(id2));
            ResidualPredicate predicate = new ResidualPredicate(1, "refId", Operator.NIN, new Operand.LiteralList(ids));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - IN/NIN Operations")
    class ResidualPredicateInNinTests {

        @Test
        void shouldHandleInOperationWithListOperand() {
            // Behavior: IN matches when scalar field value is in the operand list.
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("electronics"), new StringVal("books"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.IN, new Operand.LiteralList(categories));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleNinOperationWithListOperand() {
            // Behavior: NIN matches when scalar field value is not in the operand list.
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("books"), new StringVal("clothing"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.NIN, new Operand.LiteralList(categories));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForInOperationWhenValueNotInList() {
            // Behavior: IN returns false when scalar field value is not in the operand list.
            BsonDocument doc = new BsonDocument("category", new BsonString("toys"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("electronics"), new StringVal("books"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.IN, new Operand.LiteralList(categories));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForNinOperationWhenValueInList() {
            // Behavior: NIN returns false when scalar field value is in the operand list.
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> categories = List.of(new StringVal("electronics"), new StringVal("books"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.NIN, new Operand.LiteralList(categories));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        // Array field matching tests

        @Test
        void shouldMatchArrayFieldWithInWhenElementInList() {
            // Behavior: IN on array field matches if any array element is in the operand list.
            BsonArray tags = new BsonArray(Arrays.asList(
                    new BsonString("java"),
                    new BsonString("python"),
                    new BsonString("go")
            ));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> searchTags = List.of(new StringVal("python"), new StringVal("rust"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.IN, new Operand.LiteralList(searchTags));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayFieldWithInWhenNoElementInList() {
            // Behavior: IN on array field returns false if no array element is in the operand list.
            BsonArray tags = new BsonArray(Arrays.asList(
                    new BsonString("ruby"),
                    new BsonString("elixir")
            ));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> searchTags = List.of(new StringVal("python"), new StringVal("rust"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.IN, new Operand.LiteralList(searchTags));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayFieldWithNinWhenElementInList() {
            // Behavior: NIN on array field returns false if any array element is in the operand list.
            BsonArray tags = new BsonArray(Arrays.asList(
                    new BsonString("java"),
                    new BsonString("python")
            ));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> searchTags = List.of(new StringVal("python"), new StringVal("rust"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.NIN, new Operand.LiteralList(searchTags));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayFieldWithNinWhenNoElementInList() {
            // Behavior: NIN on array field matches if no array element is in the operand list.
            BsonArray tags = new BsonArray(Arrays.asList(
                    new BsonString("ruby"),
                    new BsonString("elixir")
            ));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> searchTags = List.of(new StringVal("python"), new StringVal("rust"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.NIN, new Operand.LiteralList(searchTags));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        // Null field handling tests

        @Test
        void shouldMatchNullFieldWithInContainingNullVal() {
            // Behavior: IN matches null field when operand list contains NullVal.
            BsonDocument doc = new BsonDocument("status", BsonNull.VALUE);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> values = List.of(new StringVal("active"), NullVal.INSTANCE);
            ResidualPredicate predicate = new ResidualPredicate(1, "status", Operator.IN, new Operand.LiteralList(values));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchNullFieldWithInNotContainingNullVal() {
            // Behavior: IN returns false for null field when operand list doesn't contain NullVal.
            BsonDocument doc = new BsonDocument("status", BsonNull.VALUE);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> values = List.of(new StringVal("active"), new StringVal("inactive"));
            ResidualPredicate predicate = new ResidualPredicate(1, "status", Operator.IN, new Operand.LiteralList(values));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchNullFieldWithNinContainingNullVal() {
            // Behavior: NIN returns false for null field when operand list contains NullVal.
            BsonDocument doc = new BsonDocument("status", BsonNull.VALUE);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> values = List.of(new StringVal("active"), NullVal.INSTANCE);
            ResidualPredicate predicate = new ResidualPredicate(1, "status", Operator.NIN, new Operand.LiteralList(values));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchNullFieldWithNinNotContainingNullVal() {
            // Behavior: NIN matches null field when operand list doesn't contain NullVal.
            BsonDocument doc = new BsonDocument("status", BsonNull.VALUE);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> values = List.of(new StringVal("active"), new StringVal("inactive"));
            ResidualPredicate predicate = new ResidualPredicate(1, "status", Operator.NIN, new Operand.LiteralList(values));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchMissingFieldWithInContainingNullVal() {
            // Behavior: IN matches missing field when operand list contains NullVal.
            BsonDocument doc = new BsonDocument("other", new BsonString("value"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> values = List.of(new StringVal("active"), NullVal.INSTANCE);
            ResidualPredicate predicate = new ResidualPredicate(1, "status", Operator.IN, new Operand.LiteralList(values));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchMissingFieldWithNinContainingNullVal() {
            // Behavior: NIN returns false for missing field when operand list contains NullVal.
            BsonDocument doc = new BsonDocument("other", new BsonString("value"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> values = List.of(new StringVal("active"), NullVal.INSTANCE);
            ResidualPredicate predicate = new ResidualPredicate(1, "status", Operator.NIN, new Operand.LiteralList(values));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        // Nested array tests - matching nested arrays as top-level elements

        @Test
        void shouldMatchNestedArrayAsTopLevelElementWithIn() {
            // Behavior: IN matches when nested array is a top-level element in the document array.
            // Document: {'tags': [['A', 'B'], 'C']}
            BsonArray nestedArray = new BsonArray(Arrays.asList(new BsonString("A"), new BsonString("B")));
            BsonArray tags = new BsonArray(Arrays.asList(nestedArray, new BsonString("C")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $in: [['A', 'B']] - should match because ['A', 'B'] is a top-level element
            List<BqlValue> searchValues = List.of(new ArrayVal(List.of(new StringVal("A"), new StringVal("B"))));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.IN, new Operand.LiteralList(searchValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchNestedArrayWithInWhenNotPresent() {
            // Behavior: IN returns false when searched nested array is not a top-level element.
            // Document: {'tags': ['X', 'Y']}
            BsonArray tags = new BsonArray(Arrays.asList(new BsonString("X"), new BsonString("Y")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $in: [['A', 'B']] - should NOT match because ['A', 'B'] is not a top-level element
            List<BqlValue> searchValues = List.of(new ArrayVal(List.of(new StringVal("A"), new StringVal("B"))));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.IN, new Operand.LiteralList(searchValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldExcludeNestedArrayAsTopLevelElementWithNin() {
            // Behavior: NIN returns false when nested array is a top-level element.
            // Document: {'tags': [['A', 'B'], 'C']}
            BsonArray nestedArray = new BsonArray(Arrays.asList(new BsonString("A"), new BsonString("B")));
            BsonArray tags = new BsonArray(Arrays.asList(nestedArray, new BsonString("C")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $nin: [['A', 'B']] - should NOT match because ['A', 'B'] IS a top-level element
            List<BqlValue> searchValues = List.of(new ArrayVal(List.of(new StringVal("A"), new StringVal("B"))));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.NIN, new Operand.LiteralList(searchValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchWithNinWhenNestedArrayNotPresent() {
            // Behavior: NIN matches when nested array is not a top-level element.
            // Document: {'tags': ['X', 'Y']}
            BsonArray tags = new BsonArray(Arrays.asList(new BsonString("X"), new BsonString("Y")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $nin: [['A', 'B']] - should match because ['A', 'B'] is NOT a top-level element
            List<BqlValue> searchValues = List.of(new ArrayVal(List.of(new StringVal("A"), new StringVal("B"))));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.NIN, new Operand.LiteralList(searchValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchScalarInsideNestedArrayWithIn() {
            // Behavior: IN does not descend into nested arrays; only top-level elements are checked.
            // Document: {'tags': [['A', 'B'], 'C']} - 'A' is inside nested array, not at top level
            BsonArray nestedArray = new BsonArray(Arrays.asList(new BsonString("A"), new BsonString("B")));
            BsonArray tags = new BsonArray(Arrays.asList(nestedArray, new BsonString("C")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $in: ['A'] - should NOT match because 'A' is not a top-level element
            List<BqlValue> searchValues = List.of(new StringVal("A"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.IN, new Operand.LiteralList(searchValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchScalarInsideNestedArrayWithNin() {
            // Behavior: NIN does not descend into nested arrays; only top-level elements are checked.
            // Document: {'tags': [['A', 'B'], 'C']} - 'A' is inside nested array, not at top level
            BsonArray nestedArray = new BsonArray(Arrays.asList(new BsonString("A"), new BsonString("B")));
            BsonArray tags = new BsonArray(Arrays.asList(nestedArray, new BsonString("C")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $nin: ['A'] - should match because 'A' is NOT a top-level element
            List<BqlValue> searchValues = List.of(new StringVal("A"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.NIN, new Operand.LiteralList(searchValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchTopLevelScalarInMixedArrayWithIn() {
            // Behavior: IN matches when scalar is a top-level element in mixed array.
            // Document: {'tags': [['A', 'B'], 'C']} - 'C' is at top level
            BsonArray nestedArray = new BsonArray(Arrays.asList(new BsonString("A"), new BsonString("B")));
            BsonArray tags = new BsonArray(Arrays.asList(nestedArray, new BsonString("C")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $in: ['C'] - should match because 'C' IS a top-level element
            List<BqlValue> searchValues = List.of(new StringVal("C"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.IN, new Operand.LiteralList(searchValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldExcludeTopLevelScalarInMixedArrayWithNin() {
            // Behavior: NIN returns false when scalar is a top-level element in mixed array.
            // Document: {'tags': [['A', 'B'], 'C']} - 'C' is at top level
            BsonArray nestedArray = new BsonArray(Arrays.asList(new BsonString("A"), new BsonString("B")));
            BsonArray tags = new BsonArray(Arrays.asList(nestedArray, new BsonString("C")));
            BsonDocument doc = new BsonDocument("tags", tags);
            ByteBuffer buffer = toBuffer(doc);

            // Query: $nin: ['C'] - should NOT match because 'C' IS a top-level element
            List<BqlValue> searchValues = List.of(new StringVal("C"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.NIN, new Operand.LiteralList(searchValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Array Fields")
    class ResidualPredicateArrayTests {

        @Test
        void shouldHandleArraySizeOperation() {
            // Behavior: SIZE matches when array length equals the operand.
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("item1"),
                    new BsonString("item2"),
                    new BsonString("item3")
            ));
            BsonDocument doc = new BsonDocument("items", array);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, lit(new Int32Val(3)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForSizeMismatch() {
            // Behavior: SIZE returns false when array length doesn't match operand.
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("item1"),
                    new BsonString("item2")
            ));
            BsonDocument doc = new BsonDocument("items", array);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, lit(new Int32Val(3)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleEmptyArraySizeOperation() {
            // Behavior: SIZE matches empty array when operand is 0.
            BsonDocument doc = new BsonDocument("items", new BsonArray());
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, lit(new Int32Val(0)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleArrayAllOperation() {
            // Behavior: ALL matches when array contains all elements in operand list.
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana"),
                    new BsonString("cherry")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"), new StringVal("banana"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.ALL, new Operand.LiteralList(expectedValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForAllWhenNotAllValuesExist() {
            // Behavior: ALL returns false when array is missing any element from operand list.
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"), new StringVal("cherry"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.ALL, new Operand.LiteralList(expectedValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForAllOnNonArrayField() {
            // Behavior: ALL returns false when field is not an array.
            BsonDocument doc = new BsonDocument("name", new BsonString("apple"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"));
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.ALL, new Operand.LiteralList(expectedValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForAllOnMissingField() {
            // Behavior: ALL returns false when field is missing.
            BsonDocument doc = new BsonDocument("other", new BsonString("value"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.ALL, new Operand.LiteralList(expectedValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForSizeOnNonArrayField() {
            // Behavior: SIZE returns false when field is not an array.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.SIZE, lit(new Int32Val(1)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForSizeOnMissingField() {
            // Behavior: SIZE returns false when field is missing.
            BsonDocument doc = new BsonDocument("other", new BsonString("value"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, lit(new Int32Val(0)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleArrayNinOperationWhenNoMatch() {
            // Behavior: NIN with ArrayVal matches when no array element is in the operand array.
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("cherry"), new StringVal("grape"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.NIN, new Operand.LiteralList(expectedValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleArrayNinOperationWhenMatch() {
            // Behavior: NIN with ArrayVal returns false when any array element is in the operand array.
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> expectedValues = List.of(new StringVal("apple"), new StringVal("grape"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.NIN, new Operand.LiteralList(expectedValues));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        // ==================== Array Comparison Operators ====================

        @Test
        void shouldMatchArrayWithEqWhenAnyElementEquals() {
            // Behavior: EQ on array field matches if ANY element equals the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(100),
                    new BsonInt32(90)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.EQ, lit(new Int32Val(100)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayWithEqWhenNoElementEquals() {
            // Behavior: EQ on array field does not match if no element equals the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(82),
                    new BsonInt32(80)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.EQ, lit(new Int32Val(100)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithNeWhenNoElementEquals() {
            // Behavior: NE on array field matches if NO element equals the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(82),
                    new BsonInt32(80)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.NE, lit(new Int32Val(100)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayWithNeWhenAnyElementEquals() {
            // Behavior: NE on array field does not match if ANY element equals the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(100),
                    new BsonInt32(90)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.NE, lit(new Int32Val(100)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithGtWhenAnyElementGreater() {
            // Behavior: GT on array field matches if ANY element is greater than the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(88),
                    new BsonInt32(90),
                    new BsonInt32(92)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.GT, lit(new Int32Val(85)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayWithGtWhenNoElementGreater() {
            // Behavior: GT on array field does not match if no element is greater than the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(80),
                    new BsonInt32(82),
                    new BsonInt32(85)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.GT, lit(new Int32Val(85)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithGteWhenAnyElementGreaterOrEqual() {
            // Behavior: GTE on array field matches if ANY element is greater than or equal to the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(82),
                    new BsonInt32(80)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.GTE, lit(new Int32Val(85)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayWithGteWhenNoElementGreaterOrEqual() {
            // Behavior: GTE on array field does not match if no element is greater than or equal to the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(80),
                    new BsonInt32(82),
                    new BsonInt32(84)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.GTE, lit(new Int32Val(85)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithLtWhenAnyElementLess() {
            // Behavior: LT on array field matches if ANY element is less than the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(82),
                    new BsonInt32(80)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.LT, lit(new Int32Val(85)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayWithLtWhenNoElementLess() {
            // Behavior: LT on array field does not match if no element is less than the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(88),
                    new BsonInt32(90),
                    new BsonInt32(92)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.LT, lit(new Int32Val(85)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithLteWhenAnyElementLessOrEqual() {
            // Behavior: LTE on array field matches if ANY element is less than or equal to the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(100),
                    new BsonInt32(90)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.LTE, lit(new Int32Val(85)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayWithLteWhenNoElementLessOrEqual() {
            // Behavior: LTE on array field does not match if no element is less than or equal to the value.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(88),
                    new BsonInt32(90),
                    new BsonInt32(92)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.LTE, lit(new Int32Val(85)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithStringComparison() {
            // Behavior: Comparison operators work with string arrays.
            BsonArray names = new BsonArray(Arrays.asList(
                    new BsonString("alice"),
                    new BsonString("bob"),
                    new BsonString("charlie")
            ));
            BsonDocument doc = new BsonDocument("names", names);
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "names", Operator.EQ, lit(new StringVal("bob"))), toView(buffer), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "names", Operator.GT, lit(new StringVal("bob"))), toView(buffer), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "names", Operator.LT, lit(new StringVal("bob"))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchArrayWithTypeMismatch() {
            // Behavior: Comparison operators return false when array element types don't match operand type.
            BsonArray grades = new BsonArray(Arrays.asList(
                    new BsonInt32(85),
                    new BsonInt32(90)
            ));
            BsonDocument doc = new BsonDocument("grades", grades);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "grades", Operator.EQ, lit(new StringVal("85")));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleEmptyArrayForComparisonOperators() {
            // Behavior: Empty arrays never match comparison operators (no elements to satisfy).
            BsonDocument doc = new BsonDocument("grades", new BsonArray());
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "grades", Operator.EQ, lit(new Int32Val(85))), toView(buffer), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "grades", Operator.GT, lit(new Int32Val(85))), toView(buffer), Collections.emptyList()));
            // NE on empty array: no element equals value, so should match
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "grades", Operator.NE, lit(new Int32Val(85))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithDoubleComparison() {
            // Behavior: Comparison operators work with double arrays.
            BsonArray scores = new BsonArray(Arrays.asList(
                    new BsonDouble(3.5),
                    new BsonDouble(4.0),
                    new BsonDouble(3.8)
            ));
            BsonDocument doc = new BsonDocument("scores", scores);
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.EQ, lit(new DoubleVal(4.0))), toView(buffer), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.GT, lit(new DoubleVal(3.9))), toView(buffer), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.LT, lit(new DoubleVal(3.6))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchArrayWithInt64Comparison() {
            // Behavior: Comparison operators work with Int64 arrays.
            BsonArray ids = new BsonArray(Arrays.asList(
                    new BsonInt64(1000000000L),
                    new BsonInt64(2000000000L),
                    new BsonInt64(3000000000L)
            ));
            BsonDocument doc = new BsonDocument("ids", ids);
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "ids", Operator.EQ, lit(new Int64Val(2000000000L))), toView(buffer), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "ids", Operator.GT, lit(new Int64Val(2500000000L))), toView(buffer), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "ids", Operator.LT, lit(new Int64Val(1500000000L))), toView(buffer), Collections.emptyList()));
        }

        // --- Array + numeric widening ---

        @Test
        void shouldMatchInt32ArrayWithInt64Operand() {
            // Behavior: Array elements widen from INT32 to INT64 for cross-type comparison.
            BsonArray scores = new BsonArray(Arrays.asList(
                    new BsonInt32(10), new BsonInt32(20), new BsonInt32(30)));
            BsonDocument doc = new BsonDocument("scores", scores);
            ByteBuffer buffer = toBuffer(doc);

            // EQ: 20 == 20L
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.EQ, lit(new Int64Val(20L))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT: 30 > 25L
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.GT, lit(new Int64Val(25L))), toView(buffer.duplicate()), Collections.emptyList()));
            // LT: 10 < 15L
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.LT, lit(new Int64Val(15L))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt32ArrayWithDoubleOperand() {
            // Behavior: Array elements widen from INT32 to DOUBLE for cross-type comparison.
            BsonArray scores = new BsonArray(Arrays.asList(
                    new BsonInt32(10), new BsonInt32(20), new BsonInt32(30)));
            BsonDocument doc = new BsonDocument("scores", scores);
            ByteBuffer buffer = toBuffer(doc);

            // EQ: 20 == 20.0
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.EQ, lit(new DoubleVal(20.0))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT: 30 > 25.0
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "scores", Operator.GT, lit(new DoubleVal(25.0))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt64ArrayWithDoubleOperandViaDecimal128() {
            // Behavior: Array INT64 elements and DOUBLE operand both promote to DECIMAL128 for lossless comparison
            // (INT64->DOUBLE is lossy, so the common type is DECIMAL128).
            BsonArray ids = new BsonArray(Arrays.asList(
                    new BsonInt64(100L), new BsonInt64(200L), new BsonInt64(300L)));
            BsonDocument doc = new BsonDocument("ids", ids);
            ByteBuffer buffer = toBuffer(doc);

            // EQ: 200L == 200.0 (both promoted to DECIMAL128)
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "ids", Operator.EQ, lit(new DoubleVal(200.0))), toView(buffer.duplicate()), Collections.emptyList()));
            // GT: 300L > 250.0
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "ids", Operator.GT, lit(new DoubleVal(250.0))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Edge Cases")
    class ResidualPredicateEdgeCases {

        @Test
        void shouldReturnFalseForNonExistentField() {
            // Behavior: Comparison on missing field returns false (except for NE and EQ with null).
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, lit(new Int32Val(20)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForFieldTypeMismatch() {
            // Behavior: Comparison returns false when field type doesn't match operand type.
            BsonDocument doc = new BsonDocument("age", new BsonString("not-a-number"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, lit(new Int32Val(20)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForDocumentValOperand() {
            // Behavior: DocumentVal operand is not supported and returns false.
            BsonDocument doc = new BsonDocument("nested", new BsonDocument("key", new BsonString("value")));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "nested", Operator.EQ, lit(new DocumentVal(java.util.Map.of())));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForInt32FieldWithStringOperand() {
            // Behavior: Type mismatch between Int32 field and String operand returns false.
            BsonDocument doc = new BsonDocument("age", new BsonInt32(25));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "age", Operator.EQ, lit(new StringVal("25"))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForStringFieldWithInt32Operand() {
            // Behavior: Type mismatch between String field and Int32 operand returns false.
            BsonDocument doc = new BsonDocument("value", new BsonString("100"));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int32Val(100))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt32FieldWithInt64Operand() {
            // Behavior: Numeric widening allows Int32 field to match Int64 operand with the same value.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(100));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val(100L))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchDoubleFieldWithInt32Operand() {
            // Behavior: Numeric widening allows Double field to match Int32 operand with the same value.
            BsonDocument doc = new BsonDocument("value", new BsonDouble(100.0));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int32Val(100))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForBooleanFieldWithStringOperand() {
            // Behavior: Type mismatch between Boolean field and String operand returns false.
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.EQ, lit(new StringVal("true"))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForBinaryFieldWithStringOperand() {
            // Behavior: Type mismatch between Binary field and String operand returns false.
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.EQ, lit(new StringVal("binary"))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt64FieldWithInt32Operand() {
            // Behavior: Numeric widening allows Int64 field to match Int32 operand with the same value.
            BsonDocument doc = new BsonDocument("value", new BsonInt64(100L));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int32Val(100))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchDoubleFieldWithDecimal128Operand() {
            // Behavior: Numeric widening allows Double field to match Decimal128 operand with the same value.
            BsonDocument doc = new BsonDocument("value", new BsonDouble(100.5));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Decimal128Val(new BigDecimal("100.5")))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchDecimal128FieldWithDoubleOperand() {
            // Behavior: Numeric widening allows Decimal128 field to match Double operand with the same value.
            BsonDocument doc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("100.5"))));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal(100.5))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForTimestampFieldWithInt64Operand() {
            // Behavior: Type mismatch between Timestamp field and Int64 operand returns false.
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.EQ, lit(new Int64Val(ts.getValue()))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForDateTimeFieldWithInt64Operand() {
            // Behavior: Type mismatch between DateTime field and Int64 operand returns false.
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.EQ, lit(new Int64Val(dateTime))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForDateTimeFieldWithTimestampOperand() {
            // Behavior: Type mismatch between DateTime field and Timestamp operand returns false.
            long dateTime = 1609459200000L;
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "eventTime", Operator.EQ, lit(new TimestampVal(dateTime))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForTimestampFieldWithDateTimeOperand() {
            // Behavior: Type mismatch between Timestamp field and DateTime operand returns false.
            BsonTimestamp ts = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", ts);
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "created", Operator.EQ, lit(new DateTimeVal(ts.getValue()))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForNullFieldWithNonNullOperand() {
            // Behavior: Null field with non-null operand returns false for EQ.
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.EQ, lit(new StringVal("value"))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.EQ, lit(new Int32Val(0))), toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "optional", Operator.EQ, lit(new BooleanVal(false))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForStringFieldWithDoubleOperand() {
            // Behavior: Type mismatch between String field and Double operand returns false.
            BsonDocument doc = new BsonDocument("value", new BsonString("3.14"));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal(3.14))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForBooleanFieldWithInt32Operand() {
            // Behavior: Type mismatch between Boolean field and Int32 operand returns false.
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "active", Operator.EQ, lit(new Int32Val(1))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForInt32FieldWithBooleanOperand() {
            // Behavior: Type mismatch between Int32 field and Boolean operand returns false.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(1));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new BooleanVal(true))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt64FieldWithDoubleOperand() {
            // Behavior: Numeric widening promotes both INT64 and DOUBLE to DECIMAL128 via commonType for comparison.
            BsonDocument doc = new BsonDocument("value", new BsonInt64(100L));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal(100.0))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchDoubleFieldWithInt64Operand() {
            // Behavior: Numeric widening promotes both DOUBLE and INT64 to DECIMAL128 via commonType for comparison.
            BsonDocument doc = new BsonDocument("value", new BsonDouble(100.0));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val(100L))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForArrayFieldWithTypeMismatchOperand() {
            // Behavior: EQ on array returns false when an operand type doesn't match any element.
            // String "[1, 2]" does not match any int element in the array.
            BsonArray array = new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2)));
            BsonDocument doc = new BsonDocument("items", array);
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "items", Operator.EQ, lit(new StringVal("[1, 2]"))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnTrueForArrayFieldWhenAnyElementMatchesEq() {
            // Behavior: EQ on array returns true when ANY element equals the operand.
            BsonArray array = new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt32(2)));
            BsonDocument doc = new BsonDocument("items", array);
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "items", Operator.EQ, lit(new Int32Val(2))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForBinaryFieldWithBooleanOperand() {
            // Behavior: Type mismatch between Binary field and Boolean operand returns false.
            byte[] data = {1, 2, 3};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, data));
            ByteBuffer buffer = toBuffer(doc);

            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "data", Operator.EQ, lit(new BooleanVal(true))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchDecimal128FieldWithInt32Operand() {
            // Behavior: Numeric widening allows Decimal128 field to match Int32 operand with the same value.
            BsonDocument doc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("100"))));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int32Val(100))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt32FieldWithDoubleOperand() {
            // Behavior: Numeric widening allows Int32 field to match Double operand with the same value (INT32 -> DOUBLE).
            BsonDocument doc = new BsonDocument("value", new BsonInt32(100));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new DoubleVal(100.0))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt32FieldWithDecimal128Operand() {
            // Behavior: Numeric widening allows Int32 field to match Decimal128 operand with the same value (INT32 -> DECIMAL128).
            BsonDocument doc = new BsonDocument("value", new BsonInt32(100));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Decimal128Val(new BigDecimal("100")))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt64FieldWithDecimal128Operand() {
            // Behavior: Numeric widening allows Int64 field to match Decimal128 operand with the same value (INT64 -> DECIMAL128).
            BsonDocument doc = new BsonDocument("value", new BsonInt64(100L));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Decimal128Val(new BigDecimal("100")))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchDecimal128FieldWithInt64Operand() {
            // Behavior: Numeric widening allows Decimal128 field to match Int64 operand with the same value (INT64 -> DECIMAL128).
            BsonDocument doc = new BsonDocument("value", new BsonDecimal128(new Decimal128(new BigDecimal("100"))));
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.EQ, lit(new Int64Val(100L))), toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForTypeMismatchWithOrderingOperators() {
            // Behavior: All ordering operators return false on type mismatch.
            BsonDocument doc = new BsonDocument("value", new BsonInt32(50));
            ByteBuffer buffer = toBuffer(doc);

            // GT with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GT, lit(new StringVal("40"))), toView(buffer.duplicate()), Collections.emptyList()));
            // GTE with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.GTE, lit(new StringVal("50"))), toView(buffer.duplicate()), Collections.emptyList()));
            // LT with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LT, lit(new StringVal("60"))), toView(buffer.duplicate()), Collections.emptyList()));
            // LTE with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.LTE, lit(new StringVal("50"))), toView(buffer.duplicate()), Collections.emptyList()));
            // NE with string operand
            assertFalse(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "value", Operator.NE, lit(new StringVal("50"))), toView(buffer.duplicate()), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - Nested Fields")
    class ResidualPredicateNestedFieldTests {

        @Test
        void shouldMatchNestedStringField() {
            // Behavior: Dot notation accesses nested fields for comparison.
            BsonDocument nested = new BsonDocument("name", new BsonString("John"));
            BsonDocument doc = new BsonDocument("user", nested);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "user.name", Operator.EQ, lit(new StringVal("John")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchNestedInt32Field() {
            // Behavior: Dot notation works for nested numeric fields.
            BsonDocument nested = new BsonDocument("age", new BsonInt32(30));
            BsonDocument doc = new BsonDocument("user", nested);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "user.age", Operator.EQ, lit(new Int32Val(30)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchDeeplyNestedField() {
            // Behavior: Dot notation supports arbitrary nesting depth.
            BsonDocument city = new BsonDocument("name", new BsonString("London"));
            BsonDocument address = new BsonDocument("city", city);
            BsonDocument user = new BsonDocument("address", address);
            BsonDocument doc = new BsonDocument("data", user);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "data.address.city.name", Operator.EQ, lit(new StringVal("London")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForNonExistentNestedField() {
            // Behavior: Missing nested field returns false for EQ with non-null.
            BsonDocument nested = new BsonDocument("name", new BsonString("John"));
            BsonDocument doc = new BsonDocument("user", nested);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "user.email", Operator.EQ, lit(new StringVal("john@example.com")));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleNestedFieldWithNullValue() {
            // Behavior: Nested null fields are handled correctly with EQ/NE and NullVal.
            BsonDocument nested = new BsonDocument("optional", new BsonNull());
            BsonDocument doc = new BsonDocument("user", nested);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate eqPredicate = new ResidualPredicate(1, "user.optional", Operator.EQ, lit(NullVal.INSTANCE));
            assertTrue(PredicateEvaluator.testResidualPredicate(eqPredicate, toView(buffer), Collections.emptyList()));

            ResidualPredicate nePredicate = new ResidualPredicate(1, "user.optional", Operator.NE, lit(NullVal.INSTANCE));
            assertFalse(PredicateEvaluator.testResidualPredicate(nePredicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleNestedFieldComparisonOperators() {
            // Behavior: All comparison operators work on nested fields.
            BsonDocument nested = new BsonDocument("score", new BsonInt32(85));
            BsonDocument doc = new BsonDocument("result", nested);
            ByteBuffer buffer = toBuffer(doc);

            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "result.score", Operator.GT, lit(new Int32Val(80))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "result.score", Operator.GTE, lit(new Int32Val(85))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "result.score", Operator.LT, lit(new Int32Val(90))), toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(
                    new ResidualPredicate(1, "result.score", Operator.LTE, lit(new Int32Val(85))), toView(buffer.duplicate()), Collections.emptyList()));
        }

        @Test
        void shouldHandleNestedFieldExistsOperator() {
            // Behavior: EXISTS operator works on nested fields.
            BsonDocument nested = new BsonDocument("name", new BsonString("John"));
            BsonDocument doc = new BsonDocument("user", nested);
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "user.name", Operator.EXISTS, lit(new BooleanVal(true)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleNestedFieldInOperator() {
            // Behavior: IN operator works on nested fields.
            BsonDocument nested = new BsonDocument("status", new BsonString("active"));
            BsonDocument doc = new BsonDocument("account", nested);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> statuses = List.of(new StringVal("active"), new StringVal("pending"));
            ResidualPredicate predicate = new ResidualPredicate(1, "account.status", Operator.IN, new Operand.LiteralList(statuses));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleNestedFieldNinOperator() {
            // Behavior: NIN operator works on nested fields.
            BsonDocument nested = new BsonDocument("status", new BsonString("active"));
            BsonDocument doc = new BsonDocument("account", nested);
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> statuses = List.of(new StringVal("suspended"), new StringVal("deleted"));
            ResidualPredicate predicate = new ResidualPredicate(1, "account.status", Operator.NIN, new Operand.LiteralList(statuses));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("evaluateBsonValue with NullVal")
    class EvaluateBsonValueNullTests {

        @Test
        void shouldReturnTrueForNullValueWithEqNullVal() {
            // Behavior: BsonNull with EQ and NullVal operand returns true.
            assertTrue(PredicateEvaluator.evaluateBsonValue(BsonNull.VALUE, Operator.EQ, NullVal.INSTANCE));
        }

        @Test
        void shouldReturnFalseForNonNullValueWithEqNullVal() {
            // Behavior: Non-null value with EQ and NullVal operand returns false.
            assertFalse(PredicateEvaluator.evaluateBsonValue(new BsonInt32(5), Operator.EQ, NullVal.INSTANCE));
            assertFalse(PredicateEvaluator.evaluateBsonValue(new BsonString("test"), Operator.EQ, NullVal.INSTANCE));
        }

        @Test
        void shouldReturnFalseForNullValueWithNeNullVal() {
            // Behavior: BsonNull with NE and NullVal operand returns false (null equals null).
            assertFalse(PredicateEvaluator.evaluateBsonValue(BsonNull.VALUE, Operator.NE, NullVal.INSTANCE));
        }

        @Test
        void shouldReturnTrueForNonNullValueWithNeNullVal() {
            // Behavior: Non-null value with NE and NullVal operand returns true.
            assertTrue(PredicateEvaluator.evaluateBsonValue(new BsonInt32(5), Operator.NE, NullVal.INSTANCE));
            assertTrue(PredicateEvaluator.evaluateBsonValue(new BsonString("test"), Operator.NE, NullVal.INSTANCE));
        }

        @Test
        void shouldReturnFalseForOrderingOperatorsWithNullVal() {
            // Behavior: Ordering operators (GT, GTE, LT, LTE) with NullVal always return false.
            assertFalse(PredicateEvaluator.evaluateBsonValue(BsonNull.VALUE, Operator.GT, NullVal.INSTANCE));
            assertFalse(PredicateEvaluator.evaluateBsonValue(BsonNull.VALUE, Operator.GTE, NullVal.INSTANCE));
            assertFalse(PredicateEvaluator.evaluateBsonValue(BsonNull.VALUE, Operator.LT, NullVal.INSTANCE));
            assertFalse(PredicateEvaluator.evaluateBsonValue(BsonNull.VALUE, Operator.LTE, NullVal.INSTANCE));
            assertFalse(PredicateEvaluator.evaluateBsonValue(new BsonInt32(5), Operator.GT, NullVal.INSTANCE));
        }
    }

    // ==================== Parameterized Predicate Tests ====================

    @Nested
    @DisplayName("testResidualPredicate - Parameterized Execution")
    class ParameterizedPredicateTests {

        private static Operand param(int index) {
            return new Operand.Param(new ParamRef(index));
        }

        private static Operand paramList(int... indices) {
            List<ParamRef> refs = Arrays.stream(indices).mapToObj(ParamRef::new).toList();
            return new Operand.ParamList(refs);
        }

        @Test
        void shouldEvaluateParamOperandWithEq() {
            // Behavior: Param(0) resolves from parameters list and matches with EQ operator.
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, param(0));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), List.of(new StringVal("John"))));
        }

        @Test
        void shouldEvaluateParamOperandWithGt() {
            // Behavior: Param(0) resolves from parameters list and matches with GT operator.
            BsonDocument doc = new BsonDocument("age", new BsonInt32(30));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, param(0));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), List.of(new Int32Val(25))));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), List.of(new Int32Val(35))));
        }

        @Test
        void shouldEvaluateNonZeroParamIndex() {
            // Behavior: Param(1) resolves to the second element in the parameters list.
            BsonDocument doc = new BsonDocument("status", new BsonString("active"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "status", Operator.EQ, param(1));
            List<BqlValue> params = List.of(new StringVal("ignored"), new StringVal("active"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), params));
        }

        @Test
        void shouldEvaluateParamListWithIn() {
            // Behavior: ParamList resolves each parameter and matches with IN operator.
            BsonDocument doc = new BsonDocument("color", new BsonString("blue"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "color", Operator.IN, paramList(0, 1, 2));
            List<BqlValue> params = List.of(new StringVal("red"), new StringVal("blue"), new StringVal("green"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), params));
        }

        @Test
        void shouldEvaluateParamListWithNin() {
            // Behavior: ParamList resolves each parameter and excludes with NIN operator.
            BsonDocument doc = new BsonDocument("color", new BsonString("yellow"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "color", Operator.NIN, paramList(0, 1));
            List<BqlValue> params = List.of(new StringVal("red"), new StringVal("blue"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), params));

            // Should return false when value is in the list
            BsonDocument doc2 = new BsonDocument("color", new BsonString("red"));
            ByteBuffer buffer2 = toBuffer(doc2);
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer2), params));
        }

        @Test
        void shouldEvaluateParamListWithAll() {
            // Behavior: ParamList resolves each parameter and matches array containing all values.
            BsonDocument doc = new BsonDocument("tags", new BsonArray(List.of(
                    new BsonString("java"),
                    new BsonString("python"),
                    new BsonString("go")
            )));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.ALL, paramList(0, 1));
            List<BqlValue> params = List.of(new StringVal("java"), new StringVal("python"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), params));

            // Should return false when array doesn't contain all values
            List<BqlValue> params2 = List.of(new StringVal("java"), new StringVal("rust"));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), params2));
        }
    }

    @Nested
    @DisplayName("testResidualPredicate - _id")
    class ResidualPredicateIdTests {

        @Test
        void shouldMatchIdWithEq() {
            // Behavior: EQ on _id matches when document ObjectId equals operand.
            ObjectId docId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.EQ, lit(new ObjectIdVal(docId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchIdWithEqWhenDifferent() {
            // Behavior: EQ on _id returns false when document ObjectId differs from operand.
            ObjectId docId = new ObjectId();
            ObjectId otherId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.EQ, lit(new ObjectIdVal(otherId)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithNe() {
            // Behavior: NE on _id matches when document ObjectId differs from operand.
            ObjectId docId = new ObjectId();
            ObjectId otherId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.NE, lit(new ObjectIdVal(otherId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchIdWithNeWhenEqual() {
            // Behavior: NE on _id returns false when document ObjectId equals operand.
            ObjectId docId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.NE, lit(new ObjectIdVal(docId)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithGt() {
            // Behavior: GT on _id matches when document ObjectId is greater than operand.
            byte[] smallBytes = new byte[12];
            byte[] largeBytes = new byte[12];
            smallBytes[0] = 1;
            largeBytes[0] = 2;
            ObjectId smallId = new ObjectId(smallBytes);
            ObjectId largeId = new ObjectId(largeBytes);

            BsonDocument largeDoc = new BsonDocument("_id", new BsonObjectId(largeId)).append("name", new BsonString("test"));
            BsonDocument smallDoc = new BsonDocument("_id", new BsonObjectId(smallId)).append("name", new BsonString("test"));

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.GT, lit(new ObjectIdVal(smallId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(largeDoc)), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(smallDoc)), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithGte() {
            // Behavior: GTE on _id matches when document ObjectId is greater than or equal to operand.
            byte[] smallBytes = new byte[12];
            byte[] largeBytes = new byte[12];
            smallBytes[0] = 1;
            largeBytes[0] = 2;
            ObjectId smallId = new ObjectId(smallBytes);
            ObjectId largeId = new ObjectId(largeBytes);

            BsonDocument largeDoc = new BsonDocument("_id", new BsonObjectId(largeId)).append("name", new BsonString("test"));
            BsonDocument smallDoc = new BsonDocument("_id", new BsonObjectId(smallId)).append("name", new BsonString("test"));

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.GTE, lit(new ObjectIdVal(smallId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(largeDoc)), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(smallDoc)), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithLt() {
            // Behavior: LT on _id matches when document ObjectId is less than operand.
            byte[] smallBytes = new byte[12];
            byte[] largeBytes = new byte[12];
            smallBytes[0] = 1;
            largeBytes[0] = 2;
            ObjectId smallId = new ObjectId(smallBytes);
            ObjectId largeId = new ObjectId(largeBytes);

            BsonDocument smallDoc = new BsonDocument("_id", new BsonObjectId(smallId)).append("name", new BsonString("test"));
            BsonDocument largeDoc = new BsonDocument("_id", new BsonObjectId(largeId)).append("name", new BsonString("test"));

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.LT, lit(new ObjectIdVal(largeId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(smallDoc)), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(largeDoc)), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithLte() {
            // Behavior: LTE on _id matches when document ObjectId is less than or equal to operand.
            byte[] smallBytes = new byte[12];
            byte[] largeBytes = new byte[12];
            smallBytes[0] = 1;
            largeBytes[0] = 2;
            ObjectId smallId = new ObjectId(smallBytes);
            ObjectId largeId = new ObjectId(largeBytes);

            BsonDocument smallDoc = new BsonDocument("_id", new BsonObjectId(smallId)).append("name", new BsonString("test"));
            BsonDocument largeDoc = new BsonDocument("_id", new BsonObjectId(largeId)).append("name", new BsonString("test"));

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.LTE, lit(new ObjectIdVal(largeId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(smallDoc)), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(toBuffer(largeDoc)), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithExistsTrue() {
            // Behavior: EXISTS true on _id returns true when document has _id field.
            ObjectId docId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.EXISTS, lit(new BooleanVal(true)));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithExistsFalse() {
            // Behavior: EXISTS false on _id returns false when document has _id field.
            ObjectId docId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.EXISTS, lit(new BooleanVal(false)));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleMissingId() {
            // Behavior: Missing _id field behaves like any missing field — EQ returns false, NE returns true.
            BsonDocument doc = new BsonDocument("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);
            ObjectId operandId = new ObjectId();

            ResidualPredicate eqPredicate = new ResidualPredicate(1, "_id", Operator.EQ, lit(new ObjectIdVal(operandId)));
            assertFalse(PredicateEvaluator.testResidualPredicate(eqPredicate, toView(buffer), Collections.emptyList()));

            ResidualPredicate nePredicate = new ResidualPredicate(1, "_id", Operator.NE, lit(new ObjectIdVal(operandId)));
            assertTrue(PredicateEvaluator.testResidualPredicate(nePredicate, toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldHandleExistsWithMissingId() {
            // Behavior: EXISTS on missing _id returns false for true, true for false.
            BsonDocument doc = new BsonDocument("name", new BsonString("test"));

            ResidualPredicate existsTruePredicate = new ResidualPredicate(1, "_id", Operator.EXISTS, lit(new BooleanVal(true)));
            assertFalse(PredicateEvaluator.testResidualPredicate(existsTruePredicate, toView(toBuffer(doc)), Collections.emptyList()));

            ResidualPredicate existsFalsePredicate = new ResidualPredicate(1, "_id", Operator.EXISTS, lit(new BooleanVal(false)));
            assertTrue(PredicateEvaluator.testResidualPredicate(existsFalsePredicate, toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithIn() {
            // Behavior: IN on _id matches when document ObjectId is in the operand list.
            ObjectId docId = new ObjectId();
            ObjectId otherId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> ids = List.of(new ObjectIdVal(docId), new ObjectIdVal(otherId));
            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.IN, new Operand.LiteralList(ids));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchIdWithInWhenNotInList() {
            // Behavior: IN on _id returns false when document ObjectId is not in the operand list.
            ObjectId docId = new ObjectId();
            ObjectId id1 = new ObjectId();
            ObjectId id2 = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> ids = List.of(new ObjectIdVal(id1), new ObjectIdVal(id2));
            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.IN, new Operand.LiteralList(ids));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldMatchIdWithNin() {
            // Behavior: NIN on _id matches when document ObjectId is not in the operand list.
            ObjectId docId = new ObjectId();
            ObjectId id1 = new ObjectId();
            ObjectId id2 = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> ids = List.of(new ObjectIdVal(id1), new ObjectIdVal(id2));
            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.NIN, new Operand.LiteralList(ids));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchIdWithNinWhenInList() {
            // Behavior: NIN on _id returns false when document ObjectId is in the operand list.
            ObjectId docId = new ObjectId();
            ObjectId otherId = new ObjectId();
            BsonDocument doc = new BsonDocument("_id", new BsonObjectId(docId)).append("name", new BsonString("test"));
            ByteBuffer buffer = toBuffer(doc);

            List<BqlValue> ids = List.of(new ObjectIdVal(docId), new ObjectIdVal(otherId));
            ResidualPredicate predicate = new ResidualPredicate(1, "_id", Operator.NIN, new Operand.LiteralList(ids));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }
    }

    // ==================== Collation-aware String Comparisons ====================

    @Nested
    @DisplayName("Collation-aware String Comparisons")
    class CollationAwareStringComparisons {

        private static Collator collator(String locale, int strength) {
            Collation spec = Collation.create(
                    locale, strength, null, null, null, null, null, null, null);
            return new CollatorCache().acquire(spec);
        }

        @Test
        void shouldEqualCaseInsensitiveWithPrimaryStrength() {
            // Behavior: with PRIMARY strength, case differences are ignored
            BsonDocument doc = new BsonDocument("name", new BsonString("cafe"));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, lit(new StringVal("CAFE")));

            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldNotEqualWithTertiaryStrength() {
            // Behavior: with TERTIARY strength, case differences matter
            BsonDocument doc = new BsonDocument("name", new BsonString("cafe"));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, lit(new StringVal("CAFE")));

            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList(), collator("en", 3)));
        }

        @Test
        void shouldCompareAccentInsensitiveWithPrimaryStrength() {
            // Behavior: with PRIMARY strength, accent differences are ignored
            BsonDocument doc = new BsonDocument("name", new BsonString("cafe"));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, lit(new StringVal("caf\u00e9")));

            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldFallBackToBinaryWhenNoCollation() {
            // Behavior: with null collator, binary ordering is used (uppercase < lowercase in ASCII)
            BsonDocument doc = new BsonDocument("name", new BsonString("b"));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.GT, lit(new StringVal("B")));

            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList()));
        }

        @Test
        void shouldHandleGtWithCollation() {
            // Behavior: GT uses collator for locale-aware ordering
            BsonDocument doc = new BsonDocument("name", new BsonString("b"));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.GT, lit(new StringVal("a")));

            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer), Collections.emptyList(), collator("en", 3)));
        }

        @Test
        void shouldHandleLteWithCollation() {
            // Behavior: LTE uses collator for locale-aware ordering
            BsonDocument doc = new BsonDocument("name", new BsonString("a"));
            ByteBuffer buffer = toBuffer(doc);

            ResidualPredicate eqPredicate = new ResidualPredicate(1, "name", Operator.LTE, lit(new StringVal("a")));
            assertTrue(PredicateEvaluator.testResidualPredicate(eqPredicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 3)));

            ResidualPredicate ltPredicate = new ResidualPredicate(2, "name", Operator.LTE, lit(new StringVal("b")));
            assertTrue(PredicateEvaluator.testResidualPredicate(ltPredicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 3)));
        }

        @Test
        void shouldHandleNeWithCollation() {
            // Behavior: NE with case-insensitive collation treats "cafe" and "CAFE" as equal
            BsonDocument doc = new BsonDocument("name", new BsonString("cafe"));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.NE, lit(new StringVal("CAFE")));

            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldHandleInWithCollation() {
            // Behavior: IN operator uses collation for string matching
            BsonDocument doc = new BsonDocument("name", new BsonString("cafe"));
            ByteBuffer buffer = toBuffer(doc);
            List<BqlValue> values = List.of(new StringVal("CAFE"), new StringVal("tea"));
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.IN, new Operand.LiteralList(values));

            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldHandleNinWithCollation() {
            // Behavior: NIN with case-insensitive collation: "cafe" matches "CAFE", so NIN returns false
            BsonDocument doc = new BsonDocument("name", new BsonString("cafe"));
            ByteBuffer buffer = toBuffer(doc);
            List<BqlValue> values = List.of(new StringVal("CAFE"));
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.NIN, new Operand.LiteralList(values));

            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldHandleAllWithCollation() {
            // Behavior: ALL operator uses collation for string matching
            BsonDocument doc = new BsonDocument("tags", new BsonArray(List.of(new BsonString("Cafe"), new BsonString("Tea"))));
            ByteBuffer buffer = toBuffer(doc);
            List<BqlValue> values = List.of(new StringVal("cafe"), new StringVal("tea"));
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.ALL, new Operand.LiteralList(values));

            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldHandleArrayElementWithCollation() {
            // Behavior: array field element matching uses collation
            BsonDocument doc = new BsonDocument("tags", new BsonArray(List.of(new BsonString("cafe"))));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "tags", Operator.EQ, lit(new StringVal("CAFE")));

            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldNotAffectIntegerComparisons() {
            // Behavior: collator has no effect on non-string comparisons
            BsonDocument doc = new BsonDocument("age", new BsonInt32(25));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.EQ, lit(new Int32Val(25)));

            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldNotAffectBooleanComparisons() {
            // Behavior: collator has no effect on boolean comparisons
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = toBuffer(doc);
            ResidualPredicate predicate = new ResidualPredicate(1, "active", Operator.EQ, lit(new BooleanVal(true)));

            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldRespectTurkishDotlessI() {
            // Behavior: Turkish locale treats I/ı and İ/i as separate character pairs, unlike English
            BsonDocument doc = new BsonDocument("city", new BsonString("istanbul"));
            ByteBuffer buffer = toBuffer(doc);

            // In Turkish: "i" and "İ" are the same base character (dotted pair)
            ResidualPredicate dottedI = new ResidualPredicate(1, "city", Operator.EQ, lit(new StringVal("\u0130stanbul")));
            assertTrue(PredicateEvaluator.testResidualPredicate(dottedI, toView(buffer.duplicate()), Collections.emptyList(), collator("tr", 1)));

            // In Turkish: "i" and "I" are DIFFERENT base characters (I pairs with ı, not i)
            ResidualPredicate dotlessI = new ResidualPredicate(2, "city", Operator.EQ, lit(new StringVal("Istanbul")));
            assertFalse(PredicateEvaluator.testResidualPredicate(dotlessI, toView(buffer.duplicate()), Collections.emptyList(), collator("tr", 1)));

            // In English: "i" and "I" ARE the same base character
            assertTrue(PredicateEvaluator.testResidualPredicate(dotlessI, toView(buffer.duplicate()), Collections.emptyList(), collator("en", 1)));
        }

        @Test
        void shouldApplyCollationToListElementComparison() {
            // Behavior: collator is applied to each element when comparing two Lists of Strings
            List<String> actual = List.of("cafe", "resume");
            List<String> expected = List.of("CAFE", "RESUME");

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, actual, expected));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, actual, expected, collator("en", 1)));
        }

        @Test
        void shouldApplyCollationToNestedListComparison() {
            // Behavior: collator propagates through nested list comparison
            List<List<String>> actual = List.of(List.of("cafe", "latte"));
            List<List<String>> expected = List.of(List.of("CAFE", "LATTE"));

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, actual, expected));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, actual, expected, collator("en", 1)));
        }

        @Test
        void shouldApplyCollationToListInequalityComparison() {
            // Behavior: NE with collation considers case-equivalent lists as equal
            List<String> actual = List.of("cafe");
            List<String> expected = List.of("CAFE");

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, actual, expected));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, actual, expected, collator("en", 1)));
        }
    }
}
