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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PredicateEvaluatorTest {

    private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();

    private static byte[] bsonDocumentToBytes(BsonDocument document) {
        try (BasicOutputBuffer buffer = new BasicOutputBuffer()) {
            BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
            BSON_DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().build());
            return buffer.toByteArray();
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})
    @DisplayName("Special double values comparison")
    void shouldHandleSpecialDoubleValuesComparison(double specialValue) {
        BsonDocument doc = new BsonDocument("value", new BsonDouble(specialValue));
        ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

        ResidualPredicate predicate = new ResidualPredicate(1, "value", Operator.EQ, new DoubleVal(specialValue));
        boolean result = PredicateEvaluator.testResidualPredicate(predicate, buffer);

        if (Double.isNaN(specialValue)) {
            // NaN == NaN should be true for our BQL equality
            assertTrue(result);
        } else {
            assertTrue(result);
        }
    }

    @Nested
    @DisplayName("Generic Comparison Tests")
    class GenericComparisonTests {

        @Test
        @DisplayName("String equality comparison")
        void shouldHandleStringEqualityComparison() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", "test"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, "test", "different"));
        }

        @Test
        @DisplayName("String inequality comparison")
        void shouldHandleStringInequalityComparison() {
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, "test", "test"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, "test", "different"));
        }

        @Test
        @DisplayName("String ordering comparisons")
        void shouldHandleStringOrderingComparisons() {
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GT, "z", "a"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, "a", "z"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, "z", "z"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, "a", "z"));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.LT, "z", "a"));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, "a", "a"));
        }

        @Test
        @DisplayName("Byte array equality comparison")
        void shouldHandleByteArrayEqualityComparison() {
            byte[] bytes1 = {1, 2, 3};
            byte[] bytes2 = {1, 2, 3};
            byte[] bytes3 = {1, 2, 4};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.EQ, bytes1, bytes2));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.EQ, bytes1, bytes3));
        }

        @Test
        @DisplayName("Byte array inequality comparison")
        void shouldHandleByteArrayInequalityComparison() {
            byte[] bytes1 = {1, 2, 3};
            byte[] bytes2 = {1, 2, 3};
            byte[] bytes3 = {1, 2, 4};

            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NE, bytes1, bytes2));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NE, bytes1, bytes3));
        }

        @Test
        @DisplayName("Byte array ordering comparisons")
        void shouldHandleByteArrayOrderingComparisons() {
            byte[] bytes1 = {1, 2, 3};
            byte[] bytes2 = {1, 2, 4};

            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LT, bytes1, bytes2));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.GT, bytes1, bytes2));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.LTE, bytes1, bytes1));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.GTE, bytes1, bytes1));
        }

        @Test
        @DisplayName("IN operator with list")
        void shouldHandleInOperatorWithList() {
            List<Object> values = List.of("apple", "banana", "cherry");
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.IN, "apple", values));
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.IN, "grape", values));
        }

        @Test
        @DisplayName("NIN operator with list")
        void shouldHandleNinOperatorWithList() {
            List<Object> values = List.of("apple", "banana", "cherry");
            assertFalse(PredicateEvaluator.evaluateComparison(Operator.NIN, "apple", values));
            assertTrue(PredicateEvaluator.evaluateComparison(Operator.NIN, "grape", values));
        }

        @Test
        @DisplayName("Unsupported operator throws exception")
        void shouldThrowExceptionForUnsupportedOperator() {
            assertThrows(UnsupportedOperationException.class, () ->
                    PredicateEvaluator.evaluateComparison(Operator.SIZE, "test", "test"));
        }

        @Test
        @DisplayName("Unsupported comparison type throws exception")
        void shouldThrowExceptionForUnsupportedComparisonType() {
            Integer actual = 5;
            Integer expected = 10;
            assertThrows(UnsupportedOperationException.class, () ->
                    PredicateEvaluator.evaluateComparison(Operator.GT, actual, expected));
        }

        @Test
        @DisplayName("IN/NIN operators with non-list throws exception")
        void shouldThrowExceptionForInNinOperatorsWithNonList() {
            assertThrows(UnsupportedOperationException.class, () ->
                    PredicateEvaluator.evaluateComparison(Operator.IN, "test", "not-a-list"));

            assertThrows(UnsupportedOperationException.class, () ->
                    PredicateEvaluator.evaluateComparison(Operator.NIN, "test", "not-a-list"));
        }
    }

    @Nested
    @DisplayName("ResidualPredicate Tests")
    class ResidualPredicateTests {

        @Test
        @DisplayName("String field equality match")
        void shouldMatchStringFieldEquality() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, new StringVal("John"));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
            // Note: PredicateEvaluator doesn't guarantee buffer position after test
        }

        @Test
        @DisplayName("String field equality no match")
        void shouldNotMatchStringFieldEquality() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EQ, new StringVal("Jane"));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Int32 field comparison")
        void shouldCompareInt32Field() {
            BsonDocument doc = new BsonDocument("age", new BsonInt32(25));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, new Int32Val(20));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Int64 field comparison")
        void shouldCompareInt64Field() {
            BsonDocument doc = new BsonDocument("timestamp", new BsonInt64(1234567890L));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "timestamp", Operator.LTE, new Int64Val(2000000000L));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Double field comparison")
        void shouldCompareDoubleField() {
            BsonDocument doc = new BsonDocument("price", new BsonDouble(19.99));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "price", Operator.LT, new DoubleVal(20.0));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Decimal128 field comparison")
        void shouldCompareDecimal128Field() {
            BsonDocument doc = new BsonDocument("amount", new BsonDecimal128(new Decimal128(new BigDecimal("100.50"))));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "amount", Operator.GTE, new Decimal128Val(new BigDecimal("100.00")));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Boolean field comparison")
        void shouldCompareBooleanField() {
            BsonDocument doc = new BsonDocument("active", new BsonBoolean(true));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "active", Operator.EQ, new BooleanVal(true));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Null field comparison")
        void shouldCompareNullField() {
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "optional", Operator.EQ, NullVal.INSTANCE);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Binary field comparison")
        void shouldCompareBinaryField() {
            byte[] binaryData = {1, 2, 3, 4, 5};
            BsonDocument doc = new BsonDocument("data", new BsonBinary(BsonBinarySubType.BINARY, binaryData));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "data", Operator.EQ, new BinaryVal(binaryData));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Timestamp field comparison")
        void shouldCompareTimestampField() {
            BsonTimestamp bsonTimestamp = new BsonTimestamp(1000, 1);
            BsonDocument doc = new BsonDocument("created", bsonTimestamp);
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            // BsonTimestamp.getValue() returns the encoded timestamp value
            ResidualPredicate predicate = new ResidualPredicate(1, "created", Operator.EQ, new TimestampVal(bsonTimestamp.getValue()));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("DateTime field comparison")
        void shouldCompareDateTimeField() {
            long dateTime = 1609459200000L; // 2021-01-01 00:00:00 UTC
            BsonDocument doc = new BsonDocument("eventTime", new BsonDateTime(dateTime));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "eventTime", Operator.EQ, new DateTimeVal(dateTime));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Versionstamp field comparison")
        void shouldCompareVersionstampField() {
            byte[] versionstampBytes = new byte[12];
            Arrays.fill(versionstampBytes, (byte) 1);
            Versionstamp versionstamp = Versionstamp.fromBytes(versionstampBytes);

            BsonDocument doc = new BsonDocument("version", new BsonBinary(BsonBinarySubType.BINARY, versionstampBytes));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "version", Operator.EQ, new VersionstampVal(versionstamp));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Array field IN operation - current behavior returns false")
        void shouldReturnFalseForArrayFieldInOperation() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana"),
                    new BsonString("cherry")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            // Testing current behavior: array IN operation with ArrayVal operand
            List<BqlValue> expectedValues = List.of(new StringVal("banana"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.IN, new ArrayVal(expectedValues));
            boolean result = PredicateEvaluator.testResidualPredicate(predicate, buffer);

            // Current implementation returns false for this operation
            assertFalse(result);
        }

        @Test
        @DisplayName("Array field SIZE operation")
        void shouldHandleArrayFieldSizeOperation() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("item1"),
                    new BsonString("item2"),
                    new BsonString("item3")
            ));
            BsonDocument doc = new BsonDocument("items", array);
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            List<BqlValue> expectedSize = List.of(new Int32Val(3));
            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, new ArrayVal(expectedSize));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Array field ALL operation")
        void shouldHandleArrayFieldAllOperation() {
            BsonArray array = new BsonArray(Arrays.asList(
                    new BsonString("apple"),
                    new BsonString("banana"),
                    new BsonString("cherry")
            ));
            BsonDocument doc = new BsonDocument("fruits", array);
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            List<BqlValue> expectedValues = List.of(new StringVal("apple"), new StringVal("banana"));
            ResidualPredicate predicate = new ResidualPredicate(1, "fruits", Operator.ALL, new ArrayVal(expectedValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Field type mismatch returns false")
        void shouldReturnFalseForFieldTypeMismatch() {
            BsonDocument doc = new BsonDocument("age", new BsonString("not-a-number"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, new Int32Val(20));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Non-existent field returns false")
        void shouldReturnFalseForNonExistentField() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "age", Operator.GT, new Int32Val(20));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("IN operation with List operand")
        void shouldHandleInOperationWithListOperand() {
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            List<BqlValue> categories = List.of(new StringVal("electronics"), new StringVal("books"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.IN, categories);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("NIN operation with List operand")
        void shouldHandleNinOperationWithListOperand() {
            BsonDocument doc = new BsonDocument("category", new BsonString("electronics"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            List<BqlValue> categories = List.of(new StringVal("books"), new StringVal("clothing"));
            ResidualPredicate predicate = new ResidualPredicate(1, "category", Operator.NIN, categories);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Non-array field with SIZE operation returns false")
        void shouldReturnFalseForNonArrayFieldWithSizeOperation() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            List<BqlValue> expectedSize = List.of(new Int32Val(1));
            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.SIZE, new ArrayVal(expectedSize));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("DocumentVal operand returns false")
        void shouldReturnFalseForDocumentValOperand() {
            BsonDocument doc = new BsonDocument("nested", new BsonDocument("key", new BsonString("value")));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "nested", Operator.EQ, new DocumentVal(java.util.Map.of()));
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }
    }

    @Nested
    @DisplayName("validateListOperand Tests")
    class ValidateListOperandTests {

        @Test
        @DisplayName("Valid list operand returns list")
        void shouldReturnListForValidListOperand() {
            List<Object> values = List.of("a", "b", "c");
            List<Object> result = PredicateEvaluator.validateListOperand(values, "TEST");
            assertEquals(values, result);
        }

        @Test
        @DisplayName("Non-list operand throws exception")
        void shouldThrowExceptionForNonListOperand() {
            UnsupportedOperationException exception = assertThrows(
                    UnsupportedOperationException.class,
                    () -> PredicateEvaluator.validateListOperand("not-a-list", "TEST")
            );
            assertTrue(exception.getMessage().contains("TEST operator requires a list of expected values"));
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCasesAndErrorHandlingTests {

        @Test
        @DisplayName("Empty array SIZE operation")
        void shouldHandleEmptyArraySizeOperation() {
            BsonArray emptyArray = new BsonArray();
            BsonDocument doc = new BsonDocument("items", emptyArray);
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            List<BqlValue> expectedSize = List.of(new Int32Val(0));
            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.SIZE, new ArrayVal(expectedSize));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Empty array ALL operation")
        void shouldHandleEmptyArrayAllOperation() {
            BsonArray emptyArray = new BsonArray();
            BsonDocument doc = new BsonDocument("items", emptyArray);
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            List<BqlValue> expectedValues = List.of(); // Empty list
            ResidualPredicate predicate = new ResidualPredicate(1, "items", Operator.ALL, new ArrayVal(expectedValues));
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Array with mixed types - current implementation behavior")
        void shouldReturnFalseForArrayWithMixedTypesInOperation() {
            BsonArray mixedArray = new BsonArray(Arrays.asList(
                    new BsonString("text"),
                    new BsonInt32(42),
                    new BsonBoolean(true)
            ));
            BsonDocument doc = new BsonDocument("mixed", mixedArray);
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            // Testing current behavior: array IN operation with mixed types
            List<BqlValue> expectedValues = List.of(new Int32Val(42));
            ResidualPredicate predicate = new ResidualPredicate(1, "mixed", Operator.IN, new ArrayVal(expectedValues));
            boolean result = PredicateEvaluator.testResidualPredicate(predicate, buffer);

            // Current implementation returns false for array IN operations
            assertFalse(result);
        }

        @Test
        @DisplayName("Null comparison with EXISTS operator")
        void shouldReturnFalseForNullComparisonWithExistsOperator() {
            BsonDocument doc = new BsonDocument("optional", new BsonNull());
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "optional", Operator.EXISTS, NullVal.INSTANCE);
            assertFalse(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }

        @Test
        @DisplayName("Non-null field with EXISTS operator")
        void shouldReturnTrueForNonNullFieldWithExistsOperator() {
            BsonDocument doc = new BsonDocument("name", new BsonString("John"));
            ByteBuffer buffer = ByteBuffer.wrap(bsonDocumentToBytes(doc));

            ResidualPredicate predicate = new ResidualPredicate(1, "name", Operator.EXISTS, NullVal.INSTANCE);
            assertTrue(PredicateEvaluator.testResidualPredicate(predicate, buffer));
        }
    }
}