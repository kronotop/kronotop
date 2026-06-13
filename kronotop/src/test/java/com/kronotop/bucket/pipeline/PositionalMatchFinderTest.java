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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.bql.ast.*;
import org.bson.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PositionalMatchFinderTest {

    private ByteBuffer createDocument(BsonDocument doc) {
        return BSONUtil.toByteBuffer(doc);
    }

    // ==================== Basic Matching Tests ====================

    @Nested
    @DisplayName("Basic Equality Matching")
    class BasicEqualityMatching {

        @Test
        void shouldFindFirstMatchingElementByStringEquality() {
            // Behavior: Finds the first array element where a nested field equals the query value.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")).append("price", new BsonInt32(100)),
                            new BsonDocument().append("name", new BsonString("B")).append("price", new BsonInt32(200)),
                            new BsonDocument().append("name", new BsonString("C")).append("price", new BsonInt32(300))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.name", new StringVal("B"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.size());
            assertEquals(1, result.get("items"), "Should match index 1 (element B)");
        }

        @Test
        void shouldFindFirstMatchingElementByIntEquality() {
            // Behavior: Finds the first array element where a nested field equals an integer value.
            BsonDocument doc = new BsonDocument()
                    .append("scores", new BsonArray(List.of(
                            new BsonDocument().append("value", new BsonInt32(85)),
                            new BsonDocument().append("value", new BsonInt32(90)),
                            new BsonDocument().append("value", new BsonInt32(95))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("scores.value", new Int32Val(90));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("scores"));

            assertEquals(1, result.size());
            assertEquals(1, result.get("scores"), "Should match index 1 (value 90)");
        }

        @Test
        void shouldReturnFirstMatchWhenMultipleElementsMatch() {
            // Behavior: When multiple elements match the query, returns only the first matching index.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("pending")),
                            new BsonDocument().append("status", new BsonString("pending")),
                            new BsonDocument().append("status", new BsonString("pending"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.status", new StringVal("pending"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(0, result.get("items"), "Should return index 0 (first match)");
        }
    }

    // ==================== Comparison Operator Tests ====================

    @Nested
    @DisplayName("Comparison Operators")
    class ComparisonOperators {

        @Test
        void shouldFindFirstMatchWithGreaterThan() {
            // Behavior: Finds the first element where the field value is greater than the query value.
            BsonDocument doc = new BsonDocument()
                    .append("scores", new BsonArray(List.of(
                            new BsonDocument().append("value", new BsonInt32(50)),
                            new BsonDocument().append("value", new BsonInt32(75)),
                            new BsonDocument().append("value", new BsonInt32(100))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGt("scores.value", new Int32Val(60));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("scores"));

            assertEquals(1, result.get("scores"), "Should match index 1 (value 75 > 60)");
        }

        @Test
        void shouldFindFirstMatchWithGreaterThanOrEqual() {
            // Behavior: Finds the first element where the field value is >= the query value.
            BsonDocument doc = new BsonDocument()
                    .append("scores", new BsonArray(List.of(
                            new BsonDocument().append("value", new BsonInt32(50)),
                            new BsonDocument().append("value", new BsonInt32(75)),
                            new BsonDocument().append("value", new BsonInt32(100))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGte("scores.value", new Int32Val(75));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("scores"));

            assertEquals(1, result.get("scores"), "Should match index 1 (value 75 >= 75)");
        }

        @Test
        void shouldFindFirstMatchWithLessThan() {
            // Behavior: Finds the first element where the field value is less than the query value.
            BsonDocument doc = new BsonDocument()
                    .append("scores", new BsonArray(List.of(
                            new BsonDocument().append("value", new BsonInt32(100)),
                            new BsonDocument().append("value", new BsonInt32(75)),
                            new BsonDocument().append("value", new BsonInt32(50))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlLt("scores.value", new Int32Val(80));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("scores"));

            assertEquals(1, result.get("scores"), "Should match index 1 (value 75 < 80)");
        }

        @Test
        void shouldFindFirstMatchWithLessThanOrEqual() {
            // Behavior: Finds the first element where the field value is <= the query value.
            BsonDocument doc = new BsonDocument()
                    .append("scores", new BsonArray(List.of(
                            new BsonDocument().append("value", new BsonInt32(100)),
                            new BsonDocument().append("value", new BsonInt32(75)),
                            new BsonDocument().append("value", new BsonInt32(50))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlLte("scores.value", new Int32Val(75));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("scores"));

            assertEquals(1, result.get("scores"), "Should match index 1 (value 75 <= 75)");
        }

        @Test
        void shouldFindFirstMatchWithNotEqual() {
            // Behavior: Finds the first element where the field value is not equal to the query value.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("active")),
                            new BsonDocument().append("status", new BsonString("pending")),
                            new BsonDocument().append("status", new BsonString("active"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlNe("items.status", new StringVal("active"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (status != 'active')");
        }
    }

    // ==================== Scalar Array Tests ====================

    @Nested
    @DisplayName("Scalar Array Matching")
    class ScalarArrayMatching {

        @Test
        void shouldFindFirstMatchInScalarArrayWithGte() {
            // Behavior: $gte on a plain scalar array finds the first element >= the query value.
            BsonDocument doc = new BsonDocument()
                    .append("grades", new BsonArray(List.of(
                            new BsonInt32(70), new BsonInt32(87), new BsonInt32(90)
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGte("grades", new Int32Val(85));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("grades"));

            assertEquals(1, result.size());
            assertEquals(1, result.get("grades"), "Should match index 1 (87 >= 85)");
        }

        @Test
        void shouldFindFirstMatchInScalarArrayViaBsonDocumentOverload() {
            // Behavior: BsonDocument overload produces the same result as ByteBuffer overload.
            BsonDocument doc = new BsonDocument()
                    .append("grades", new BsonArray(List.of(
                            new BsonInt32(70), new BsonInt32(87), new BsonInt32(90)
                    )));

            BqlExpr query = new BqlGte("grades", new Int32Val(85));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    doc, query, Set.of("grades"));

            assertEquals(1, result.size());
            assertEquals(1, result.get("grades"), "Should match index 1 (87 >= 85)");
        }
    }

    // ==================== $in Operator Tests ====================

    @Nested
    @DisplayName("$in Operator")
    class InOperator {

        @Test
        void shouldFindFirstMatchWithInOperator() {
            // Behavior: Finds the first element whose field value is in the specified list.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("shipped")),
                            new BsonDocument().append("status", new BsonString("pending")),
                            new BsonDocument().append("status", new BsonString("delivered"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlIn("items.status", List.of(new StringVal("pending"), new StringVal("processing")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (status in ['pending', 'processing'])");
        }
    }

    // ==================== $and Operator Tests ====================

    @Nested
    @DisplayName("$and Operator")
    class AndOperator {

        @Test
        void shouldFindFirstMatchWithAndConditions() {
            // Behavior: Finds the first element satisfying all conditions in an $and query.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("price", new BsonInt32(50)).append("inStock", new BsonInt32(1)),
                            new BsonDocument().append("price", new BsonInt32(150)).append("inStock", new BsonInt32(0)),
                            new BsonDocument().append("price", new BsonInt32(150)).append("inStock", new BsonInt32(1)),
                            new BsonDocument().append("price", new BsonInt32(200)).append("inStock", new BsonInt32(1))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: items.price >= 100 AND items.inStock == 1
            BqlExpr query = new BqlAnd(List.of(
                    new BqlGte("items.price", new Int32Val(100)),
                    new BqlEq("items.inStock", new Int32Val(1))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(2, result.get("items"), "Should match index 2 (price >= 100 AND inStock == 1)");
        }
    }

    // ==================== $elemMatch Tests ====================

    @Nested
    @DisplayName("$elemMatch Operator")
    class ElemMatchOperator {

        @Test
        void shouldFindFirstMatchWithElemMatch() {
            // Behavior: Finds the first element satisfying all conditions within $elemMatch.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("price", new BsonInt32(50)).append("qty", new BsonInt32(5)),
                            new BsonDocument().append("price", new BsonInt32(150)).append("qty", new BsonInt32(2)),
                            new BsonDocument().append("price", new BsonInt32(100)).append("qty", new BsonInt32(10)),
                            new BsonDocument().append("price", new BsonInt32(200)).append("qty", new BsonInt32(8))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: items: { $elemMatch: { price: { $gte: 100 }, qty: { $gte: 5 } } }
            BqlExpr innerExpr = new BqlAnd(List.of(
                    new BqlGte("price", new Int32Val(100)),
                    new BqlGte("qty", new Int32Val(5))
            ));
            BqlExpr query = new BqlElemMatch("items", innerExpr);

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(2, result.get("items"), "Should match index 2 (price >= 100 AND qty >= 5)");
        }

        @Test
        void shouldFindFirstMatchWithElemMatchSimpleEquality() {
            // Behavior: $elemMatch with simple equality condition.
            BsonDocument doc = new BsonDocument()
                    .append("tags", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("red")).append("priority", new BsonInt32(1)),
                            new BsonDocument().append("name", new BsonString("blue")).append("priority", new BsonInt32(2)),
                            new BsonDocument().append("name", new BsonString("green")).append("priority", new BsonInt32(3))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: tags: { $elemMatch: { name: "blue" } }
            BqlExpr innerExpr = new BqlEq("name", new StringVal("blue"));
            BqlExpr query = new BqlElemMatch("tags", innerExpr);

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("tags"));

            assertEquals(1, result.get("tags"), "Should match index 1 (name == 'blue')");
        }
    }

    // ==================== $regex Operator Tests ====================

    @Nested
    @DisplayName("$regex Operator")
    class RegexOperator {

        @Test
        void shouldFindFirstMatchWithRegexOnNestedField() {
            // Behavior: $regex on a nested field finds the first element whose string field matches.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("Alice")),
                            new BsonDocument().append("name", new BsonString("Bob")),
                            new BsonDocument().append("name", new BsonString("Bert"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlRegex("items.name", new RegexVal("^B", ""));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (first name starting with B)");
        }

        @Test
        void shouldFindFirstMatchWithRegexOnScalarArray() {
            // Behavior: $regex on a plain scalar string array finds the first matching string element.
            BsonDocument doc = new BsonDocument()
                    .append("tags", new BsonArray(List.of(
                            new BsonString("red"),
                            new BsonString("green"),
                            new BsonString("grey")
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlRegex("tags", new RegexVal("^gr", ""));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("tags"));

            assertEquals(1, result.get("tags"), "Should match index 1 (first string starting with 'gr')");
        }

        @Test
        void shouldFindFirstMatchWithRegexInsideElemMatch() {
            // Behavior: $regex nested inside $elemMatch finds the first element whose field matches.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("red")),
                            new BsonDocument().append("name", new BsonString("blue")),
                            new BsonDocument().append("name", new BsonString("black"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlElemMatch("items", new BqlRegex("name", new RegexVal("^bl", "")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (first name starting with 'bl')");
        }

        @Test
        void shouldMatchRegexCaseInsensitivelyWithIOption() {
            // Behavior: the i option makes positional $regex matching case-insensitive.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("ALICE")),
                            new BsonDocument().append("name", new BsonString("BOB"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlRegex("items.name", new RegexVal("^bob$", "i"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 case-insensitively");
        }

        @Test
        void shouldNotMatchRegexAgainstNonStringField() {
            // Behavior: $regex never matches a non-string field, so no position is found.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("code", new BsonInt32(42)),
                            new BsonDocument().append("code", new BsonInt32(99))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlRegex("items.code", new RegexVal("42", ""));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertTrue(result.isEmpty(), "Regex against a non-string field should yield no matched position");
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        void shouldReturnEmptyMapForEmptyPositionalFields() {
            // Behavior: Returns empty map when no positional fields are specified.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.name", new StringVal("A"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of());

            assertTrue(result.isEmpty(), "Should return empty map for empty positional fields");
        }

        @Test
        void shouldThrowWhenArrayFieldNotInQuery() {
            // Behavior: Throws exception when positional field is not referenced in the query.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query references "other" field, not "items"
            BqlExpr query = new BqlEq("other.name", new StringVal("A"));

            assertThrows(IllegalArgumentException.class, () ->
                    PositionalMatchFinder.findMatchedPositions(buffer, query, Set.of("items")));
        }

        @Test
        void shouldDefaultToSpecifiedIndexWhenArrayNotInQuery() {
            // Behavior: When defaultOnMissing is non-negative and the array is not referenced
            // in the query, returns the default index instead of throwing.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")),
                            new BsonDocument().append("name", new BsonString("B"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query references "other" field, not "items"
            BqlExpr query = new BqlEq("other.name", new StringVal("A"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"), 0);

            assertEquals(1, result.size());
            assertEquals(0, result.get("items"), "Should default to index 0");
        }

        @Test
        void shouldThrowWhenArrayIsEmpty() {
            // Behavior: Throws exception when the array is empty.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray());

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.name", new StringVal("A"));

            assertThrows(IllegalStateException.class, () ->
                    PositionalMatchFinder.findMatchedPositions(buffer, query, Set.of("items")));
        }

        @Test
        void shouldOmitFieldFromResultWhenNoElementMatches() {
            // Behavior: When no array element matches the query, the field is omitted from the result map.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")),
                            new BsonDocument().append("name", new BsonString("B")),
                            new BsonDocument().append("name", new BsonString("C"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.name", new StringVal("D"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(buffer, query, Set.of("items"));
            assertFalse(result.containsKey("items"));
        }

        @Test
        void shouldThrowWhenFieldIsNotArray() {
            // Behavior: Throws exception when the field is not an array.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonString("not an array"));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.name", new StringVal("A"));

            assertThrows(IllegalStateException.class, () ->
                    PositionalMatchFinder.findMatchedPositions(buffer, query, Set.of("items")));
        }

        @Test
        void shouldMatchFirstElementAtIndexZero() {
            // Behavior: Correctly returns index 0 when the first element matches.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("pending")),
                            new BsonDocument().append("status", new BsonString("shipped")),
                            new BsonDocument().append("status", new BsonString("delivered"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.status", new StringVal("pending"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(0, result.get("items"), "Should match index 0");
        }

        @Test
        void shouldMatchLastElement() {
            // Behavior: Correctly returns the last index when only the last element matches.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("shipped")),
                            new BsonDocument().append("status", new BsonString("delivered")),
                            new BsonDocument().append("status", new BsonString("pending"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlEq("items.status", new StringVal("pending"));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(2, result.get("items"), "Should match index 2 (last element)");
        }
    }

    // ==================== Multiple Positional Fields Tests ====================

    @Nested
    @DisplayName("Multiple Positional Fields")
    class MultiplePositionalFields {

        @Test
        void shouldHandleMultiplePositionalFields() {
            // Behavior: Correctly finds matches for multiple array fields in a single query.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")),
                            new BsonDocument().append("name", new BsonString("B"))
                    )))
                    .append("tags", new BsonArray(List.of(
                            new BsonDocument().append("label", new BsonString("X")),
                            new BsonDocument().append("label", new BsonString("Y")),
                            new BsonDocument().append("label", new BsonString("Z"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlAnd(List.of(
                    new BqlEq("items.name", new StringVal("B")),
                    new BqlEq("tags.label", new StringVal("Z"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items", "tags"));

            assertEquals(2, result.size());
            assertEquals(1, result.get("items"), "Should match items index 1");
            assertEquals(2, result.get("tags"), "Should match tags index 2");
        }
    }

    // ==================== $nin Operator Tests ====================

    @Nested
    @DisplayName("$nin Operator")
    class NinOperator {

        @Test
        void shouldFindFirstMatchWithNinOperator() {
            // Behavior: Finds the first element whose field value is NOT in the specified list.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("shipped")),
                            new BsonDocument().append("status", new BsonString("pending")),
                            new BsonDocument().append("status", new BsonString("delivered"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlNin("items.status", List.of(new StringVal("shipped"), new StringVal("delivered")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (status 'pending' is not in ['shipped', 'delivered'])");
        }

        @Test
        void shouldFindFirstMatchWithNinWhenFieldMissing() {
            // Behavior: $nin matches elements where the field doesn't exist.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("shipped")),
                            new BsonDocument().append("name", new BsonString("Item")), // No status field
                            new BsonDocument().append("status", new BsonString("delivered"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlNin("items.status", List.of(new StringVal("shipped"), new StringVal("delivered")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (missing field is not in excluded list)");
        }
    }

    // ==================== $exists Operator Tests ====================

    @Nested
    @DisplayName("$exists Operator")
    class ExistsOperator {

        @Test
        void shouldFindFirstMatchWithExistsTrue() {
            // Behavior: Finds the first element where the field exists.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")),
                            new BsonDocument().append("name", new BsonString("B")).append("discount", new BsonInt32(10)),
                            new BsonDocument().append("name", new BsonString("C"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlExists("items.discount", true);

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (has discount field)");
        }

        @Test
        void shouldFindFirstMatchWithExistsFalse() {
            // Behavior: Finds the first element where the field does not exist.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")).append("discount", new BsonInt32(5)),
                            new BsonDocument().append("name", new BsonString("B")),
                            new BsonDocument().append("name", new BsonString("C")).append("discount", new BsonInt32(10))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlExists("items.discount", false);

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (no discount field)");
        }
    }

    // ==================== $not Operator Tests ====================

    @Nested
    @DisplayName("$not Operator")
    class NotOperator {

        @Test
        void shouldFindFirstMatchWithNotInsideElemMatch() {
            // Behavior: $not inside $elemMatch negates the inner condition.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("price", new BsonInt32(100)),
                            new BsonDocument().append("price", new BsonInt32(50)),
                            new BsonDocument().append("price", new BsonInt32(200))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: items: { $elemMatch: { $not: { price: { $gte: 100 } } } }
            // Matches element where price is NOT >= 100 (i.e., price < 100)
            BqlExpr innerExpr = new BqlNot(new BqlGte("price", new Int32Val(100)));
            BqlExpr query = new BqlElemMatch("items", innerExpr);

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (price 50 is not >= 100)");
        }
    }

    // ==================== $exists inside $elemMatch Tests ====================

    @Nested
    @DisplayName("$exists inside $elemMatch")
    class ExistsInsideElemMatch {

        @Test
        void shouldFindFirstMatchWithExistsInsideElemMatch() {
            // Behavior: $exists inside $elemMatch finds the first element where the field exists.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")),
                            new BsonDocument().append("name", new BsonString("B")).append("discount", new BsonInt32(10)),
                            new BsonDocument().append("name", new BsonString("C"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlElemMatch("items", new BqlExists("discount", true));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (has discount field)");
        }
    }

    // ==================== $in inside $elemMatch Tests ====================

    @Nested
    @DisplayName("$in inside $elemMatch")
    class InInsideElemMatch {

        @Test
        void shouldFindFirstMatchWithInInsideElemMatch() {
            // Behavior: $in operator works correctly inside $elemMatch.
            BsonDocument doc = new BsonDocument()
                    .append("orders", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("pending")).append("amount", new BsonInt32(100)),
                            new BsonDocument().append("status", new BsonString("shipped")).append("amount", new BsonInt32(200)),
                            new BsonDocument().append("status", new BsonString("delivered")).append("amount", new BsonInt32(300))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: orders: { $elemMatch: { status: { $in: ["shipped", "delivered"] }, amount: { $gte: 200 } } }
            BqlExpr innerExpr = new BqlAnd(List.of(
                    new BqlIn("status", List.of(new StringVal("shipped"), new StringVal("delivered"))),
                    new BqlGte("amount", new Int32Val(200))
            ));
            BqlExpr query = new BqlElemMatch("orders", innerExpr);

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("orders"));

            assertEquals(1, result.get("orders"), "Should match index 1 (status in ['shipped', 'delivered'] AND amount >= 200)");
        }
    }

    // ==================== Nested $elemMatch Tests ====================

    @Nested
    @DisplayName("Nested $elemMatch")
    class NestedElemMatch {

        @Test
        void shouldFindFirstMatchWithNestedElemMatch() {
            // Behavior: Nested $elemMatch works correctly for documents with nested arrays.
            BsonDocument doc = new BsonDocument()
                    .append("orders", new BsonArray(List.of(
                            new BsonDocument()
                                    .append("id", new BsonString("ORD1"))
                                    .append("items", new BsonArray(List.of(
                                            new BsonDocument().append("name", new BsonString("Widget")).append("qty", new BsonInt32(1)),
                                            new BsonDocument().append("name", new BsonString("Gadget")).append("qty", new BsonInt32(2))
                                    ))),
                            new BsonDocument()
                                    .append("id", new BsonString("ORD2"))
                                    .append("items", new BsonArray(List.of(
                                            new BsonDocument().append("name", new BsonString("Widget")).append("qty", new BsonInt32(5)),
                                            new BsonDocument().append("name", new BsonString("Gizmo")).append("qty", new BsonInt32(3))
                                    )))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: orders: { $elemMatch: { items: { $elemMatch: { name: "Widget", qty: { $gte: 5 } } } } }
            BqlExpr nestedElemMatch = new BqlElemMatch("items", new BqlAnd(List.of(
                    new BqlEq("name", new StringVal("Widget")),
                    new BqlGte("qty", new Int32Val(5))
            )));
            BqlExpr query = new BqlElemMatch("orders", nestedElemMatch);

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("orders"));

            assertEquals(1, result.get("orders"), "Should match index 1 (ORD2 has Widget with qty >= 5)");
        }
    }

    // ==================== $or Operator Tests ====================

    @Nested
    @DisplayName("$or Operator")
    class OrOperator {

        @Test
        void shouldFindFirstMatchInArrayOrderWithOrQuery() {
            // Behavior: With $or, finds the first element in array order that matches ANY branch,
            // regardless of query branch order.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("shipped")),
                            new BsonDocument().append("status", new BsonString("pending"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: {$or: [{items.status: "pending"}, {items.status: "shipped"}]}
            // "pending" is first in query, but "shipped" is first in array
            BqlExpr query = new BqlOr(List.of(
                    new BqlEq("items.status", new StringVal("pending")),
                    new BqlEq("items.status", new StringVal("shipped"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(0, result.get("items"), "Should match index 0 (first in array order, matches second branch)");
        }

        @Test
        void shouldMatchWithPartialOrBranch() {
            // Behavior: Only one $or branch needs to match (true OR semantics).
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("pending"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: {$or: [{items.status: "pending"}, {items.status: "non_existent"}]}
            BqlExpr query = new BqlOr(List.of(
                    new BqlEq("items.status", new StringVal("pending")),
                    new BqlEq("items.status", new StringVal("non_existent"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(0, result.get("items"), "Should match index 0 (only first branch matches)");
        }

        @Test
        void shouldSelectFirstElementWhenMultipleMatchDifferentBranches() {
            // Behavior: When elements match different $or branches, first-element-wins.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")).append("status", new BsonString("active")),
                            new BsonDocument().append("name", new BsonString("B")).append("status", new BsonString("shipped"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: {$or: [{items.status: "shipped"}, {items.name: "A"}]}
            // Index 0 matches "name=A", Index 1 matches "status=shipped"
            BqlExpr query = new BqlOr(List.of(
                    new BqlEq("items.status", new StringVal("shipped")),
                    new BqlEq("items.name", new StringVal("A"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(0, result.get("items"), "Should match index 0 (first element that matches any branch)");
        }

        @Test
        void shouldSkipElementsWithMissingFields() {
            // Behavior: Elements missing the queried field are skipped.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("NoStatus")),
                            new BsonDocument().append("status", new BsonString("pending"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: {$or: [{items.status: "pending"}, {items.status: "shipped"}]}
            BqlExpr query = new BqlOr(List.of(
                    new BqlEq("items.status", new StringVal("pending")),
                    new BqlEq("items.status", new StringVal("shipped"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (first element with matching field)");
        }

        @Test
        void shouldOmitFieldWhenNoElementMatchesAnyOrBranch() {
            // Behavior: When no array element matches any $or branch, the field is omitted from the result map.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("status", new BsonString("active")),
                            new BsonDocument().append("status", new BsonString("inactive"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: {$or: [{items.status: "pending"}, {items.status: "shipped"}]}
            BqlExpr query = new BqlOr(List.of(
                    new BqlEq("items.status", new StringVal("pending")),
                    new BqlEq("items.status", new StringVal("shipped"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(buffer, query, Set.of("items"));
            assertFalse(result.containsKey("items"));
        }

        @Test
        void shouldHandleOrWithComparisonOperators() {
            // Behavior: $or works with comparison operators in branches.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("price", new BsonInt32(50)),
                            new BsonDocument().append("price", new BsonInt32(150)),
                            new BsonDocument().append("price", new BsonInt32(250))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: {$or: [{items.price: {$lt: 30}}, {items.price: {$gt: 200}}]}
            // Index 0: 50 (not < 30, not > 200)
            // Index 1: 150 (not < 30, not > 200)
            // Index 2: 250 (not < 30, but > 200) - matches!
            BqlExpr query = new BqlOr(List.of(
                    new BqlLt("items.price", new Int32Val(30)),
                    new BqlGt("items.price", new Int32Val(200))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(2, result.get("items"), "Should match index 2 (price > 200)");
        }
    }

    // ==================== Cross-Numeric-Type Matching Tests ====================

    @Nested
    @DisplayName("Cross-Numeric-Type Matching")
    class CrossNumericTypeMatching {

        @Test
        void shouldFindMatchWhenDocumentHasInt64AndQueryUsesInt32() {
            // Behavior: Positional match works when document stores Int64 but query uses Int32.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("price", new BsonInt64(50)),
                            new BsonDocument().append("price", new BsonInt64(150)),
                            new BsonDocument().append("price", new BsonInt64(250))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // Query: {items.price: {$gt: 100}} with Int32Val
            BqlExpr query = new BqlGt("items.price", new Int32Val(100));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (Int64 150 > Int32 100)");
        }

        @Test
        void shouldFindMatchWhenDocumentHasInt32AndQueryUsesInt64() {
            // Behavior: Positional match works when document stores Int32 but query uses Int64.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("price", new BsonInt32(50)),
                            new BsonDocument().append("price", new BsonInt32(150)),
                            new BsonDocument().append("price", new BsonInt32(250))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGt("items.price", new Int64Val(100));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (Int32 150 > Int64 100)");
        }

        @Test
        void shouldFindMatchWhenDocumentHasInt32AndQueryUsesDouble() {
            // Behavior: Positional match works when document stores Int32 but query uses Double.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("score", new BsonInt32(85)),
                            new BsonDocument().append("score", new BsonInt32(92)),
                            new BsonDocument().append("score", new BsonInt32(78))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGte("items.score", new DoubleVal(90.0));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (Int32 92 >= Double 90.0)");
        }

        @Test
        void shouldFindMatchWhenDocumentHasDoubleAndQueryUsesInt32() {
            // Behavior: Positional match works when document stores Double but query uses Int32.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("weight", new BsonDouble(10.5)),
                            new BsonDocument().append("weight", new BsonDouble(99.99)),
                            new BsonDocument().append("weight", new BsonDouble(5.0))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGt("items.weight", new Int32Val(50));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (Double 99.99 > Int32 50)");
        }

        @Test
        void shouldFindMatchWhenDocumentHasInt32AndQueryUsesDecimal128() {
            // Behavior: Positional match works when document stores Int32 but query uses Decimal128.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("amount", new BsonInt32(100)),
                            new BsonDocument().append("amount", new BsonInt32(200)),
                            new BsonDocument().append("amount", new BsonInt32(300))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGte("items.amount", new Decimal128Val(new BigDecimal("200")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.get("items"), "Should match index 1 (Int32 200 >= Decimal128 200)");
        }

        @Test
        void shouldFindMatchWithCrossNumericEqualityInElemMatch() {
            // Behavior: Cross-numeric comparison works inside $elemMatch with positional operator.
            BsonDocument doc = new BsonDocument()
                    .append("scores", new BsonArray(List.of(
                            new BsonInt64(85),
                            new BsonInt64(92),
                            new BsonInt64(78)
                    )));

            ByteBuffer buffer = createDocument(doc);
            // $elemMatch with Int32 value against Int64 array elements
            BqlExpr query = new BqlElemMatch("scores", new BqlGte("", new Int32Val(90)));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("scores"));

            assertEquals(1, result.get("scores"), "Should match index 1 (Int64 92 >= Int32 90)");
        }

        @Test
        void shouldFindMatchWithDateTimeValSameType() {
            // Behavior: DateTimeVal same-type comparison works for positional matching.
            BsonDocument doc = new BsonDocument()
                    .append("events", new BsonArray(List.of(
                            new BsonDocument().append("ts", new BsonDateTime(1000)),
                            new BsonDocument().append("ts", new BsonDateTime(2000)),
                            new BsonDocument().append("ts", new BsonDateTime(3000))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlGt("events.ts", new DateTimeVal(1500));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("events"));

            assertEquals(1, result.get("events"), "Should match index 1 (DateTime 2000 > 1500)");
        }

        @Test
        void shouldFindMatchWithTimestampValSameType() {
            // Behavior: TimestampVal same-type comparison works for positional matching.
            BsonDocument doc = new BsonDocument()
                    .append("logs", new BsonArray(List.of(
                            new BsonDocument().append("ts", new BsonTimestamp(100, 1)),
                            new BsonDocument().append("ts", new BsonTimestamp(200, 1)),
                            new BsonDocument().append("ts", new BsonTimestamp(300, 1))
                    )));

            ByteBuffer buffer = createDocument(doc);
            // BsonTimestamp getValue() returns the combined (seconds << 32 | increment) value
            long targetValue = new BsonTimestamp(200, 1).getValue();
            BqlExpr query = new BqlEq("logs.ts", new TimestampVal(targetValue));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("logs"));

            assertEquals(1, result.get("logs"), "Should match index 1 (Timestamp 200,1 == 200,1)");
        }
    }

    // ==================== $or Mixed with Non-Array Predicates ====================

    @Nested
    @DisplayName("$or Mixed with Non-Array Predicates")
    class OrMixedWithNonArrayPredicates {

        @Test
        void shouldMatchOnlyArrayBranchWhenOrBranchReferencesNonArrayField() {
            // Behavior: When one $or branch references a non-array field, that branch evaluates
            // per-element (element lacks the field), so only the array-referencing branch contributes.
            BsonDocument doc = new BsonDocument()
                    .append("status", new BsonString("active"))
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")),
                            new BsonDocument().append("name", new BsonString("B")),
                            new BsonDocument().append("name", new BsonString("C"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlOr(List.of(
                    new BqlEq("status", new StringVal("active")),
                    new BqlEq("items.name", new StringVal("C"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(2, result.get("items"), "Should match index 2 (only the array branch items.name==C contributes)");
        }
    }

    // ==================== $all Operator Tests ====================

    @Nested
    @DisplayName("All Operator")
    class AllOperator {

        @Test
        void shouldFindFirstMatchingElementWithAll() {
            // Behavior: $all on a scalar array finds the first element matching any value in the list.
            BsonDocument doc = new BsonDocument()
                    .append("tags", new BsonArray(List.of(
                            new BsonString("java"),
                            new BsonString("python"),
                            new BsonString("go")
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlAll("tags", List.of(new StringVal("python"), new StringVal("go")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("tags"));

            assertEquals(1, result.size());
            assertEquals(1, result.get("tags"), "Should match index 1 (python is the first match)");
        }

        @Test
        void shouldFindFirstMatchingElementWithAllOnNestedField() {
            // Behavior: $all on a nested document field finds the first element whose field matches any value.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("category", new BsonString("electronics")),
                            new BsonDocument().append("category", new BsonString("books")),
                            new BsonDocument().append("category", new BsonString("clothing"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlAll("items.category", List.of(new StringVal("books"), new StringVal("clothing")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.size());
            assertEquals(1, result.get("items"), "Should match index 1 (books is the first match)");
        }

        @Test
        void shouldReturnEmptyWhenNoElementMatchesAll() {
            // Behavior: When no array element matches any $all value, the result map is empty.
            BsonDocument doc = new BsonDocument()
                    .append("tags", new BsonArray(List.of(
                            new BsonString("java"),
                            new BsonString("python"),
                            new BsonString("go")
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlAll("tags", List.of(new StringVal("rust"), new StringVal("haskell")));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("tags"));

            assertTrue(result.isEmpty(), "No element matches any $all value");
        }

        @Test
        void shouldCombineAllWithOtherPredicates() {
            // Behavior: $all combined with $eq via $and — both predicates must match the same element.
            BsonDocument doc = new BsonDocument()
                    .append("items", new BsonArray(List.of(
                            new BsonDocument().append("name", new BsonString("A")).append("status", new BsonString("active")),
                            new BsonDocument().append("name", new BsonString("B")).append("status", new BsonString("inactive")),
                            new BsonDocument().append("name", new BsonString("C")).append("status", new BsonString("active"))
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlAnd(List.of(
                    new BqlAll("items.name", List.of(new StringVal("B"), new StringVal("C"))),
                    new BqlEq("items.status", new StringVal("active"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("items"));

            assertEquals(1, result.size());
            assertEquals(2, result.get("items"), "Should match index 2 (C is active and in $all list)");
        }
    }

    // ==================== $size Operator Tests ====================

    @Nested
    @DisplayName("Size Operator")
    class SizeOperator {

        @Test
        void shouldThrowWhenSizeIsOnlyPredicateOnPositionalArray() {
            // Behavior: $size alone cannot identify a specific element — throws IllegalArgumentException.
            BsonDocument doc = new BsonDocument()
                    .append("tags", new BsonArray(List.of(
                            new BsonString("java"),
                            new BsonString("python"),
                            new BsonString("go")
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlSize("tags", 3);

            assertThrows(IllegalArgumentException.class, () ->
                    PositionalMatchFinder.findMatchedPositions(buffer, query, Set.of("tags")));
        }

        @Test
        void shouldIgnoreSizeWhenCombinedWithElementPredicate() {
            // Behavior: $size is silently ignored; only the element-level $eq predicate contributes.
            BsonDocument doc = new BsonDocument()
                    .append("tags", new BsonArray(List.of(
                            new BsonString("java"),
                            new BsonString("python"),
                            new BsonString("go")
                    )));

            ByteBuffer buffer = createDocument(doc);
            BqlExpr query = new BqlAnd(List.of(
                    new BqlSize("tags", 3),
                    new BqlEq("tags", new StringVal("python"))
            ));

            Map<String, Integer> result = PositionalMatchFinder.findMatchedPositions(
                    buffer, query, Set.of("tags"));

            assertEquals(1, result.size());
            assertEquals(1, result.get("tags"), "Should match index 1 (python), $size ignored");
        }
    }
}
