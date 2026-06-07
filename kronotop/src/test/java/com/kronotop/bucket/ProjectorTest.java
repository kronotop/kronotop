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

import com.kronotop.bucket.bql.ast.*;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProjectorTest {

    // --- $ Positional Operator ---

    @Test
    void shouldProjectFirstMatchingElementOfScalarArray() {
        // Behavior: $ returns the first array element matching the query condition, not the first element
        // Query: grades >= 85, so first match in [70, 87, 90] is 87 (index 1)
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("semester", new BsonInt32(1))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1, \"semester\": 1}");
        BqlExpr query = new BqlGte("grades", new Int32Val(85));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertEquals(1, result.getInt32("semester").getValue());
        BsonArray grades = result.getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(87, grades.get(0).asInt32().getValue());
    }

    @Test
    void shouldProjectFirstMatchingElementOfDocumentArray() {
        // Behavior: $ on array of documents returns the first document element matching the query
        // Query: grades.mean > 70, so first match is {grade: 80, mean: 75, std: 8}
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray(List.of(
                        new BsonDocument("grade", new BsonInt32(80))
                                .append("mean", new BsonInt32(75))
                                .append("std", new BsonInt32(8)),
                        new BsonDocument("grade", new BsonInt32(85))
                                .append("mean", new BsonInt32(90))
                                .append("std", new BsonInt32(5)))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlGt("grades.mean", new Int32Val(70));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        BsonDocument result = results.get(0);
        BsonArray grades = result.getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(80, grades.get(0).asDocument().getInt32("grade").getValue());
        assertEquals(75, grades.get(0).asDocument().getInt32("mean").getValue());
    }

    @Test
    void shouldProjectWithElemMatchQuery() {
        // Behavior: $ with $elemMatch query returns the first element matching all conditions
        // Query: grades elemMatch {mean > 70, grade > 90}, matches {grade: 92, mean: 88}
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray(List.of(
                        new BsonDocument("grade", new BsonInt32(80))
                                .append("mean", new BsonInt32(75)),
                        new BsonDocument("grade", new BsonInt32(92))
                                .append("mean", new BsonInt32(88)),
                        new BsonDocument("grade", new BsonInt32(78))
                                .append("mean", new BsonInt32(90)))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlElemMatch("grades", new BqlAnd(List.of(
                new BqlGt("mean", new Int32Val(70)),
                new BqlGt("grade", new Int32Val(90)))));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        BsonDocument result = results.get(0);
        BsonArray grades = result.getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(92, grades.get(0).asDocument().getInt32("grade").getValue());
        assertEquals(88, grades.get(0).asDocument().getInt32("mean").getValue());
    }

    @Test
    void shouldProjectFirstElementWhenArrayNotInQuery() {
        // Behavior: When the query does not reference the array used with $,
        // projection defaults to the first element (index 0).
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("semester", new BsonInt32(1))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1, \"semester\": 1}");
        // Query references "status", not "grades"
        BqlExpr query = new BqlEq("status", new StringVal("active"));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertEquals(1, result.getInt32("semester").getValue());
        BsonArray grades = result.getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(70, grades.get(0).asInt32().getValue(), "Should default to first element");
    }

    @Test
    void shouldReturnEmptyArrayWhenNoElementMatchesQuery() {
        // Behavior: $ returns an empty array when no element matches the query condition
        // Query: grades >= 95, but all elements are [70, 87, 90] — none match
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlGte("grades", new Int32Val(95));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        BsonArray grades = result.getArray("grades");
        assertTrue(grades.isEmpty());
    }

    @Test
    void shouldReturnEmptyArrayWhenArrayIsEmpty() {
        // Behavior: $ on an empty array returns an empty array (no match possible)
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray());

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlGte("grades", new Int32Val(85));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        BsonDocument result = results.get(0);
        BsonArray grades = result.getArray("grades");
        assertTrue(grades.isEmpty());
    }

    @Test
    void shouldProjectMultipleDocumentsWithPositional() {
        // Behavior: each document gets its own matched element based on the query condition
        // Query: grades >= 85
        // doc1: [70, 87, 90] -> first match is 87
        // doc2: [90, 88, 92] -> first match is 90
        // doc3: [85, 100, 90] -> first match is 85
        BsonDocument doc1 = new BsonDocument()
                .append("_id", new BsonInt32(1))
                .append("semester", new BsonInt32(1))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));
        BsonDocument doc2 = new BsonDocument()
                .append("_id", new BsonInt32(2))
                .append("semester", new BsonInt32(1))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(90), new BsonInt32(88), new BsonInt32(92))));
        BsonDocument doc3 = new BsonDocument()
                .append("_id", new BsonInt32(3))
                .append("semester", new BsonInt32(1))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(85), new BsonInt32(100), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlGte("grades", new Int32Val(85));

        List<BsonDocument> results = Projector.project(List.of(doc1, doc2, doc3), spec, query);

        assertEquals(3, results.size());
        assertEquals(87, results.get(0).getArray("grades").get(0).asInt32().getValue());
        assertEquals(90, results.get(1).getArray("grades").get(0).asInt32().getValue());
        assertEquals(85, results.get(2).getArray("grades").get(0).asInt32().getValue());
    }

    // --- Validation ---

    @Test
    void shouldProjectNestedPositionalPath() {
        // Behavior: $ works with dot-notation prefix like "user.grades.$"
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("user", new BsonDocument()
                        .append("grades", new BsonArray(List.of(
                                new BsonInt32(70), new BsonInt32(87), new BsonInt32(90)))));

        BsonDocument spec = BsonDocument.parse("{\"user.grades.$\": 1}");
        BqlExpr query = new BqlGte("user.grades", new Int32Val(85));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        BsonArray grades = result.getDocument("user").getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(87, grades.get(0).asInt32().getValue());
    }

    @Test
    void shouldOmitPositionalFieldWhenArrayNotInDocument() {
        // Behavior: When the document lacks the array field used with $, the field is omitted from the result
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlGte("grades", new Int32Val(85));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertFalse(result.containsKey("grades"));
    }

    @Test
    void shouldProjectPositionalWithCompoundAndQuery() {
        // Behavior: $ works with $and query combining non-array and array conditions
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("semester", new BsonInt32(1))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlAnd(List.of(
                new BqlEq("semester", new Int32Val(1)),
                new BqlGte("grades", new Int32Val(85))));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonArray grades = results.get(0).getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(87, grades.get(0).asInt32().getValue());
    }

    @Test
    void shouldProjectPositionalWithOrQuery() {
        // Behavior: $ with $or returns first element matching any branch
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlOr(List.of(
                new BqlEq("grades", new Int32Val(99)),
                new BqlGte("grades", new Int32Val(85))));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonArray grades = results.get(0).getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(87, grades.get(0).asInt32().getValue());
    }

    @Test
    void shouldProjectPositionalWithEqOnScalarArray() {
        // Behavior: $ with $eq on scalar array returns the exact matching element
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlEq("grades", new Int32Val(87));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonArray grades = results.get(0).getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(87, grades.get(0).asInt32().getValue());
    }

    @Test
    void shouldProjectPositionalWithInOnScalarArray() {
        // Behavior: $ with $in on scalar array returns the first element found in the list
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlIn("grades", List.of(new Int32Val(87), new Int32Val(99)));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonArray grades = results.get(0).getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(87, grades.get(0).asInt32().getValue());
    }

    @Test
    void shouldProjectPositionalWithNeOnScalarArray() {
        // Behavior: $ with $ne on scalar array returns the first element not equal to the value
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("grades", new BsonArray(List.of(
                        new BsonInt32(70), new BsonInt32(87), new BsonInt32(90))));

        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");
        BqlExpr query = new BqlNe("grades", new Int32Val(70));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonArray grades = results.get(0).getArray("grades");
        assertEquals(1, grades.size());
        assertEquals(87, grades.get(0).asInt32().getValue());
    }

    @Test
    void shouldOmitPositionalFieldWhenFieldDoesNotExist() {
        // Behavior: $ on a field absent from the document omits the field, does not error
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"));

        BsonDocument spec = BsonDocument.parse("{\"scores.$\": 1, \"name\": 1}");
        BqlExpr query = new BqlGte("scores", new Int32Val(85));

        List<BsonDocument> results = Projector.project(List.of(doc), spec, query);

        assertEquals(1, results.size());
        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertEquals("Alice", result.getString("name").getValue());
        assertFalse(result.containsKey("scores"));
    }

    // --- Validation ---

    @Test
    void shouldRejectPositionalWithoutParsedQuery() {
        // Behavior: $ operator requires a parsed query
        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1}");

        assertThrows(IllegalArgumentException.class, () ->
                Projector.project(List.of(), spec, null));
    }

    @Test
    void shouldRejectPositionalNotAtEnd() {
        // Behavior: $ in the middle of a path is rejected
        BsonDocument spec = BsonDocument.parse("{\"instock.$.qty\": 1}");

        assertThrows(IllegalArgumentException.class, () ->
                Projector.project(List.of(), spec));
    }

    @Test
    void shouldRejectMultiplePositionalOperators() {
        // Behavior: only one $ operator allowed per projection spec
        BsonDocument spec = BsonDocument.parse("{\"grades.$\": 1, \"scores.$\": 1}");

        assertThrows(IllegalArgumentException.class, () ->
                Projector.project(List.of(), spec));
    }

    @Test
    void shouldRejectMixedInclusionExclusion() {
        // Behavior: cannot mix inclusion and exclusion
        BsonDocument spec = BsonDocument.parse("{\"name\": 1, \"age\": 0}");

        assertThrows(IllegalArgumentException.class, () ->
                Projector.project(List.of(), spec));
    }

    // --- _id handling ---

    @Test
    void shouldIncludeIdByDefault() {
        // Behavior: _id is always included unless explicitly excluded
        BsonObjectId id = new BsonObjectId(new ObjectId());
        BsonDocument doc = new BsonDocument()
                .append("_id", id)
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30));

        BsonDocument spec = BsonDocument.parse("{\"name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertEquals(id, result.getObjectId("_id"));
        assertEquals("Alice", result.getString("name").getValue());
        assertFalse(result.containsKey("age"));
    }

    @Test
    void shouldExcludeIdCombinedWithFieldExclusions() {
        // Behavior: _id: 0 with field exclusions removes both _id and the specified fields
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30))
                .append("email", new BsonString("alice@example.com"));

        BsonDocument spec = BsonDocument.parse("{\"_id\": 0, \"age\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertFalse(result.containsKey("_id"));
        assertEquals("Alice", result.getString("name").getValue());
        assertFalse(result.containsKey("age"));
        assertEquals("alice@example.com", result.getString("email").getValue());
    }

    @Test
    void shouldExcludeOnlyIdWhenSpecIsIdZero() {
        // Behavior: {"_id": 0} alone means exclusion mode — return all fields except _id
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30));

        BsonDocument spec = BsonDocument.parse("{\"_id\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertFalse(result.containsKey("_id"));
        assertEquals("Alice", result.getString("name").getValue());
        assertEquals(30, result.getInt32("age").getValue());
    }

    @Test
    void shouldExcludeIdWhenExplicit() {
        // Behavior: _id: 0 excludes _id from the result
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"));

        BsonDocument spec = BsonDocument.parse("{\"_id\": 0, \"name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertFalse(result.containsKey("_id"));
        assertEquals("Alice", result.getString("name").getValue());
    }

    // --- Empty spec ---

    @Test
    void shouldReturnAllFieldsWhenSpecIsEmpty() {
        // Behavior: empty projection spec {} returns all fields unchanged
        BsonObjectId id = new BsonObjectId(new ObjectId());
        BsonDocument doc = new BsonDocument()
                .append("_id", id)
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30));

        BsonDocument spec = new BsonDocument();

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertEquals(id, result.getObjectId("_id"));
        assertEquals("Alice", result.getString("name").getValue());
        assertEquals(30, result.getInt32("age").getValue());
    }

    // --- Basic inclusion/exclusion ---

    @Test
    void shouldIncludeMultipleSpecifiedFields() {
        // Behavior: multiple field inclusion returns all listed fields when present
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30))
                .append("email", new BsonString("alice@example.com"));

        BsonDocument spec = BsonDocument.parse("{\"name\": 1, \"age\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertEquals("Alice", result.getString("name").getValue());
        assertEquals(30, result.getInt32("age").getValue());
        assertFalse(result.containsKey("email"));
    }

    @Test
    void shouldIncludeOnlySpecifiedFields() {
        // Behavior: inclusion mode returns only the listed fields
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30))
                .append("email", new BsonString("alice@example.com"));

        BsonDocument spec = BsonDocument.parse("{\"name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertTrue(result.containsKey("name"));
        assertFalse(result.containsKey("age"));
        assertFalse(result.containsKey("email"));
    }

    @Test
    void shouldExcludeSpecifiedFields() {
        // Behavior: exclusion mode returns everything except the listed fields
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30))
                .append("email", new BsonString("alice@example.com"));

        BsonDocument spec = BsonDocument.parse("{\"age\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertTrue(result.containsKey("name"));
        assertFalse(result.containsKey("age"));
        assertTrue(result.containsKey("email"));
    }

    @Test
    void shouldIncludeDeepNestedField() {
        // Behavior: multi-level dot-notation paths work in inclusion mode
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("a", new BsonDocument("b", new BsonDocument()
                        .append("c", new BsonInt32(1))
                        .append("d", new BsonInt32(2))));

        BsonDocument spec = BsonDocument.parse("{\"a.b.c\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertEquals(1, result.getDocument("a").getDocument("b").getInt32("c").getValue());
        assertFalse(result.getDocument("a").getDocument("b").containsKey("d"));
    }

    @Test
    void shouldIncludeNestedField() {
        // Behavior: dot-notation paths work in inclusion mode
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("user", new BsonDocument()
                        .append("name", new BsonString("Alice"))
                        .append("age", new BsonInt32(30)));

        BsonDocument spec = BsonDocument.parse("{\"user.name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertEquals("Alice", result.getDocument("user").getString("name").getValue());
        assertFalse(result.getDocument("user").containsKey("age"));
    }

    // --- Dot-notation exclusion ---

    @Test
    void shouldExcludeNestedField() {
        // Behavior: dot-notation paths work in exclusion mode
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("user", new BsonDocument()
                        .append("name", new BsonString("Alice"))
                        .append("age", new BsonInt32(30)));

        BsonDocument spec = BsonDocument.parse("{\"user.name\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertTrue(result.containsKey("user"));
        assertFalse(result.getDocument("user").containsKey("name"));
        assertEquals(30, result.getDocument("user").getInt32("age").getValue());
    }

    @Test
    void shouldExcludeDeepNestedField() {
        // Behavior: multi-level dot-notation paths work in exclusion mode
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("a", new BsonDocument("b", new BsonDocument()
                        .append("c", new BsonInt32(1))
                        .append("d", new BsonInt32(2))));

        BsonDocument spec = BsonDocument.parse("{\"a.b.c\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertFalse(result.getDocument("a").getDocument("b").containsKey("c"));
        assertEquals(2, result.getDocument("a").getDocument("b").getInt32("d").getValue());
    }

    @Test
    void shouldExcludeMultipleNestedFieldsInSameParent() {
        // Behavior: multiple dot-notation exclusions within the same parent document
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("user", new BsonDocument()
                        .append("name", new BsonString("Alice"))
                        .append("email", new BsonString("alice@example.com"))
                        .append("age", new BsonInt32(30)));

        BsonDocument spec = BsonDocument.parse("{\"user.name\": 0, \"user.email\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("user"));
        assertFalse(result.getDocument("user").containsKey("name"));
        assertFalse(result.getDocument("user").containsKey("email"));
        assertEquals(30, result.getDocument("user").getInt32("age").getValue());
    }

    @Test
    void shouldExcludeNestedFieldGracefullyWhenMissing() {
        // Behavior: excluding a dot-notation path that doesn't exist is a no-op
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"));

        BsonDocument spec = BsonDocument.parse("{\"user.name\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertEquals("Alice", result.getString("name").getValue());
    }

    @Test
    void shouldNotMutateOriginalDocumentOnNestedExclusion() {
        // Behavior: nested exclusion clones intermediate documents, leaving the original untouched
        BsonDocument original = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("user", new BsonDocument()
                        .append("name", new BsonString("Alice"))
                        .append("age", new BsonInt32(30)));

        BsonDocument spec = BsonDocument.parse("{\"user.name\": 0}");

        Projector.project(List.of(original), spec);

        assertTrue(original.getDocument("user").containsKey("name"));
    }

    // --- Multiple documents ---

    @Test
    void shouldProjectMultipleDocuments() {
        // Behavior: spec is parsed once and applied to all documents
        BsonDocument doc1 = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"))
                .append("age", new BsonInt32(30));

        BsonDocument doc2 = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Bob"))
                .append("age", new BsonInt32(25));

        BsonDocument spec = BsonDocument.parse("{\"name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc1, doc2), spec);

        assertEquals(2, results.size());
        assertEquals("Alice", results.get(0).getString("name").getValue());
        assertEquals("Bob", results.get(1).getString("name").getValue());
        assertFalse(results.get(0).containsKey("age"));
        assertFalse(results.get(1).containsKey("age"));
    }

    @Test
    void shouldSkipMissingFieldGracefully() {
        // Behavior: if a document doesn't have the projected field, it's simply absent in the result
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"));

        BsonDocument spec = BsonDocument.parse("{\"name\": 1, \"age\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertEquals("Alice", result.getString("name").getValue());
        assertFalse(result.containsKey("age"));
    }

    // --- Exclusion through array intermediates ---

    @Test
    void shouldExcludeNestedFieldFromDocumentsInArray() {
        // Behavior: dot-notation exclusion removes the specified field from every document in an array
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("items", new BsonArray(List.of(
                        new BsonDocument("name", new BsonString("a")).append("qty", new BsonInt32(1)),
                        new BsonDocument("name", new BsonString("b")).append("qty", new BsonInt32(2)))));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        BsonArray items = result.getArray("items");
        assertEquals(2, items.size());
        assertFalse(items.get(0).asDocument().containsKey("name"));
        assertEquals(1, items.get(0).asDocument().getInt32("qty").getValue());
        assertFalse(items.get(1).asDocument().containsKey("name"));
        assertEquals(2, items.get(1).asDocument().getInt32("qty").getValue());
    }

    @Test
    void shouldExcludeNestedFieldFromDocumentsInArrayPreservingNonDocElements() {
        // Behavior: non-document elements in a mixed array are preserved during exclusion
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("items", new BsonArray(List.of(
                        new BsonDocument("name", new BsonString("a")).append("qty", new BsonInt32(1)),
                        new BsonString("scalar"),
                        new BsonDocument("name", new BsonString("b")).append("qty", new BsonInt32(2)))));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        BsonArray items = result.getArray("items");
        assertEquals(3, items.size());
        assertFalse(items.get(0).asDocument().containsKey("name"));
        assertEquals("scalar", items.get(1).asString().getValue());
        assertFalse(items.get(2).asDocument().containsKey("name"));
    }

    @Test
    void shouldExcludeDeepNestedFieldThroughMultipleArrays() {
        // Behavior: exclusion works through multiple levels of array intermediates
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("orders", new BsonArray(List.of(
                        new BsonDocument("items", new BsonArray(List.of(
                                new BsonDocument("name", new BsonString("x")).append("qty", new BsonInt32(1)),
                                new BsonDocument("name", new BsonString("y")).append("qty", new BsonInt32(2))))),
                        new BsonDocument("items", new BsonArray(List.of(
                                new BsonDocument("name", new BsonString("z")).append("qty", new BsonInt32(3))))))));

        BsonDocument spec = BsonDocument.parse("{\"orders.items.name\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        BsonArray orders = result.getArray("orders");
        BsonArray items0 = orders.get(0).asDocument().getArray("items");
        assertEquals(2, items0.size());
        assertFalse(items0.get(0).asDocument().containsKey("name"));
        assertEquals(1, items0.get(0).asDocument().getInt32("qty").getValue());
        assertFalse(items0.get(1).asDocument().containsKey("name"));
        assertEquals(2, items0.get(1).asDocument().getInt32("qty").getValue());

        BsonArray items1 = orders.get(1).asDocument().getArray("items");
        assertEquals(1, items1.size());
        assertFalse(items1.get(0).asDocument().containsKey("name"));
        assertEquals(3, items1.get(0).asDocument().getInt32("qty").getValue());
    }

    @Test
    void shouldNotMutateOriginalDocumentOnArrayExclusion() {
        // Behavior: array exclusion clones intermediate arrays and documents, leaving the original untouched
        BsonDocument original = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("items", new BsonArray(List.of(
                        new BsonDocument("name", new BsonString("a")).append("qty", new BsonInt32(1)))));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 0}");

        Projector.project(List.of(original), spec);

        assertTrue(original.getArray("items").get(0).asDocument().containsKey("name"));
    }

    @Test
    void shouldExcludeNestedFieldGracefullyWhenArrayElementLacksField() {
        // Behavior: if some array elements don't have the excluded field, they are passed through unchanged
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("items", new BsonArray(List.of(
                        new BsonDocument("name", new BsonString("a")).append("qty", new BsonInt32(1)),
                        new BsonDocument("qty", new BsonInt32(2)))));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 0}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        BsonArray items = result.getArray("items");
        assertEquals(2, items.size());
        assertFalse(items.get(0).asDocument().containsKey("name"));
        assertEquals(1, items.get(0).asDocument().getInt32("qty").getValue());
        assertEquals(2, items.get(1).asDocument().getInt32("qty").getValue());
    }

    // --- Inclusion through array intermediates ---

    @Test
    void shouldIncludeNestedFieldFromDocumentsInArray() {
        // Behavior: dot-notation inclusion extracts the specified field from every document in an array, preserving array-of-documents shape
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("items", new BsonArray(List.of(
                        new BsonDocument("name", new BsonString("a")).append("qty", new BsonInt32(1)),
                        new BsonDocument("name", new BsonString("b")).append("qty", new BsonInt32(2)))));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        BsonArray items = result.getArray("items");
        assertEquals(2, items.size());
        assertEquals("a", items.get(0).asDocument().getString("name").getValue());
        assertFalse(items.get(0).asDocument().containsKey("qty"));
        assertEquals("b", items.get(1).asDocument().getString("name").getValue());
        assertFalse(items.get(1).asDocument().containsKey("qty"));
    }

    @Test
    void shouldIncludeMultipleNestedFieldsFromDocumentsInArray() {
        // Behavior: multiple dot-notation inclusions from the same array merge correctly into each element
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("items", new BsonArray(List.of(
                        new BsonDocument("name", new BsonString("a")).append("qty", new BsonInt32(1)).append("price", new BsonInt32(10)),
                        new BsonDocument("name", new BsonString("b")).append("qty", new BsonInt32(2)).append("price", new BsonInt32(20)))));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 1, \"items.qty\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        BsonArray items = result.getArray("items");
        assertEquals(2, items.size());
        assertEquals("a", items.get(0).asDocument().getString("name").getValue());
        assertEquals(1, items.get(0).asDocument().getInt32("qty").getValue());
        assertFalse(items.get(0).asDocument().containsKey("price"));
        assertEquals("b", items.get(1).asDocument().getString("name").getValue());
        assertEquals(2, items.get(1).asDocument().getInt32("qty").getValue());
        assertFalse(items.get(1).asDocument().containsKey("price"));
    }

    @Test
    void shouldIncludeDeepNestedFieldThroughMultipleArrays() {
        // Behavior: inclusion works through multiple levels of array intermediates
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("orders", new BsonArray(List.of(
                        new BsonDocument("items", new BsonArray(List.of(
                                new BsonDocument("name", new BsonString("x")).append("qty", new BsonInt32(1)),
                                new BsonDocument("name", new BsonString("y")).append("qty", new BsonInt32(2))))),
                        new BsonDocument("items", new BsonArray(List.of(
                                new BsonDocument("name", new BsonString("z")).append("qty", new BsonInt32(3))))))));

        BsonDocument spec = BsonDocument.parse("{\"orders.items.name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        BsonArray orders = result.getArray("orders");
        assertEquals(2, orders.size());

        BsonArray items0 = orders.get(0).asDocument().getArray("items");
        assertEquals(2, items0.size());
        assertEquals("x", items0.get(0).asDocument().getString("name").getValue());
        assertFalse(items0.get(0).asDocument().containsKey("qty"));
        assertEquals("y", items0.get(1).asDocument().getString("name").getValue());

        BsonArray items1 = orders.get(1).asDocument().getArray("items");
        assertEquals(1, items1.size());
        assertEquals("z", items1.get(0).asDocument().getString("name").getValue());
        assertFalse(items1.get(0).asDocument().containsKey("qty"));
    }

    @Test
    void shouldIncludeNestedFieldSkippingNonDocArrayElements() {
        // Behavior: non-document elements in a mixed array are skipped during inclusion
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("items", new BsonArray(List.of(
                        new BsonDocument("name", new BsonString("a")),
                        new BsonString("scalar"),
                        new BsonDocument("name", new BsonString("b")))));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        BsonArray items = result.getArray("items");
        assertEquals(2, items.size());
        assertEquals("a", items.get(0).asDocument().getString("name").getValue());
        assertEquals("b", items.get(1).asDocument().getString("name").getValue());
    }

    @Test
    void shouldIncludeNestedFieldGracefullyWhenArrayFieldIsMissing() {
        // Behavior: if the array field doesn't exist, result omits it
        BsonDocument doc = new BsonDocument()
                .append("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Alice"));

        BsonDocument spec = BsonDocument.parse("{\"items.name\": 1}");

        List<BsonDocument> results = Projector.project(List.of(doc), spec);

        BsonDocument result = results.get(0);
        assertTrue(result.containsKey("_id"));
        assertFalse(result.containsKey("items"));
    }
}
