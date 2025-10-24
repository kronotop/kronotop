/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.index;

import com.kronotop.bucket.BSONUtil;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class SelectorMatcherTest {
    @Test
    void testRootLevel() {
        Document document = new Document();
        document.append("key", new BsonString("value"));
        BsonValue matchedValue = SelectorMatcher.match("key", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("value", matchedValue.asString().getValue());
    }

    @Test
    void testMatchWithByteBuffer() {
        Document document = new Document();
        document.append("key", new BsonString("value"));
        ByteBuffer buffer = ByteBuffer.wrap(BSONUtil.toBytes(document));
        BsonValue matchedValue = SelectorMatcher.match("key", buffer);

        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("value", matchedValue.asString().getValue());
    }

    @Test
    void testNestedDocument() {
        Document nested = new Document();
        nested.append("innerKey", new BsonString("innerValue"));

        Document document = new Document();
        document.append("outer", nested);

        BsonValue matchedValue = SelectorMatcher.match("outer.innerKey", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("innerValue", matchedValue.asString().getValue());
    }

    @Test
    void testArrayAccess() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("first"));
        array.add(new BsonString("second"));
        array.add(new BsonString("third"));

        Document document = new Document();
        document.append("items", array);

        BsonValue matchedValue = SelectorMatcher.match("items.1", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("second", matchedValue.asString().getValue());
    }

    @Test
    void testNestedArrayInDocument() {
        BsonDocument innerDoc = new BsonDocument();
        innerDoc.append("name", new BsonString("test"));

        BsonArray array = new BsonArray();
        array.add(innerDoc);

        Document document = new Document();
        document.append("data", array);

        BsonValue matchedValue = SelectorMatcher.match("data.0.name", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("test", matchedValue.asString().getValue());
    }

    @Test
    void testDeeplyNestedPath() {
        Document level3 = new Document();
        level3.append("finalKey", new BsonInt32(42));

        Document level2 = new Document();
        level2.append("level3", level3);

        Document level1 = new Document();
        level1.append("level2", level2);

        Document document = new Document();
        document.append("level1", level1);

        BsonValue matchedValue = SelectorMatcher.match("level1.level2.level3.finalKey", document);
        assertInstanceOf(BsonInt32.class, matchedValue);
        assertEquals(42, matchedValue.asInt32().getValue());
    }

    @Test
    void testNonExistentPath() {
        Document document = new Document();
        document.append("key", new BsonString("value"));

        BsonValue matchedValue = SelectorMatcher.match("nonexistent", document);
        assertNull(matchedValue);
    }

    @Test
    void testInvalidArrayIndex() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("first"));

        Document document = new Document();
        document.append("items", array);

        BsonValue matchedValue = SelectorMatcher.match("items.5", document);
        assertNull(matchedValue);
    }

    // Test all BSON types mentioned in JavaDoc
    @Test
    void testBsonInt64() {
        Document document = new Document();
        document.append("longValue", new BsonInt64(9223372036854775807L));

        BsonValue matchedValue = SelectorMatcher.match("longValue", document);
        assertInstanceOf(BsonInt64.class, matchedValue);
        assertEquals(9223372036854775807L, matchedValue.asInt64().getValue());
    }

    @Test
    void testBsonDouble() {
        Document document = new Document();
        document.append("doubleValue", new BsonDouble(3.14159));

        BsonValue matchedValue = SelectorMatcher.match("doubleValue", document);
        assertInstanceOf(BsonDouble.class, matchedValue);
        assertEquals(3.14159, matchedValue.asDouble().getValue(), 0.00001);
    }

    @Test
    void testBsonBoolean() {
        Document document = new Document();
        document.append("boolValue", new BsonBoolean(true));

        BsonValue matchedValue = SelectorMatcher.match("boolValue", document);
        assertInstanceOf(BsonBoolean.class, matchedValue);
        assertTrue(matchedValue.asBoolean().getValue());
    }

    @Test
    void testBsonNull() {
        Document document = new Document();
        document.append("nullValue", new BsonNull());

        BsonValue matchedValue = SelectorMatcher.match("nullValue", document);
        assertInstanceOf(BsonNull.class, matchedValue);
    }

    @Test
    void testBsonDateTime() {
        Date now = new Date();
        Document document = new Document();
        document.append("dateValue", new BsonDateTime(now.getTime()));

        BsonValue matchedValue = SelectorMatcher.match("dateValue", document);
        assertInstanceOf(BsonDateTime.class, matchedValue);
        assertEquals(now.getTime(), matchedValue.asDateTime().getValue());
    }

    @Test
    void testBsonObjectId() {
        ObjectId objectId = new ObjectId();
        Document document = new Document();
        document.append("idValue", new BsonObjectId(objectId));

        BsonValue matchedValue = SelectorMatcher.match("idValue", document);
        assertInstanceOf(BsonObjectId.class, matchedValue);
        assertEquals(objectId, matchedValue.asObjectId().getValue());
    }

    @Test
    void testNestedBsonDocument() {
        BsonDocument nestedDoc = new BsonDocument();
        nestedDoc.append("innerField", new BsonString("innerValue"));

        Document document = new Document();
        document.append("nested", nestedDoc);

        BsonValue matchedValue = SelectorMatcher.match("nested", document);
        assertInstanceOf(BsonDocument.class, matchedValue);
        assertEquals("innerValue", matchedValue.asDocument().getString("innerField").getValue());
    }

    @Test
    void testNestedBsonArray() {
        BsonArray nestedArray = new BsonArray();
        nestedArray.add(new BsonString("item1"));
        nestedArray.add(new BsonString("item2"));

        Document document = new Document();
        document.append("arrayField", nestedArray);

        BsonValue matchedValue = SelectorMatcher.match("arrayField", document);
        assertInstanceOf(BsonArray.class, matchedValue);
        assertEquals(2, matchedValue.asArray().size());
        assertEquals("item1", matchedValue.asArray().get(0).asString().getValue());
    }

    // Test invalid array index with non-numeric string
    @Test
    void testNonNumericArrayIndex() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("first"));

        Document document = new Document();
        document.append("items", array);

        BsonValue matchedValue = SelectorMatcher.match("items.abc", document);
        assertNull(matchedValue);
    }

    // Test complex nested structures from JavaDoc examples
    @Test
    void testComplexNestedStructure() {
        // Build: company.departments.0.employees.5.address.coordinates.latitude
        BsonDocument coordinates = new BsonDocument();
        coordinates.append("latitude", new BsonDouble(40.7128));
        coordinates.append("longitude", new BsonDouble(-74.0060));

        BsonDocument address = new BsonDocument();
        address.append("coordinates", coordinates);

        BsonDocument employee = new BsonDocument();
        employee.append("address", address);

        BsonArray employees = new BsonArray();
        // Add 6 employees so index 5 exists
        for (int i = 0; i < 6; i++) {
            if (i == 5) {
                employees.add(employee);
            } else {
                employees.add(new BsonDocument().append("name", new BsonString("emp" + i)));
            }
        }

        BsonDocument department = new BsonDocument();
        department.append("employees", employees);

        BsonArray departments = new BsonArray();
        departments.add(department);

        Document company = new Document();
        company.append("departments", departments);

        Document document = new Document();
        document.append("company", company);

        BsonValue matchedValue = SelectorMatcher.match("company.departments.0.employees.5.address.coordinates.latitude", document);
        assertInstanceOf(BsonDouble.class, matchedValue);
        assertEquals(40.7128, matchedValue.asDouble().getValue(), 0.0001);
    }

    // Test the exact example from method JavaDoc
    @Test
    void testMethodJavaDocExample() {
        Document user = new Document()
                .append("name", "Alice")
                .append("contact", new Document()
                        .append("email", "alice@example.com")
                        .append("phones", Arrays.asList("123-456-7890", "987-654-3210")));

        BsonValue name = SelectorMatcher.match("name", user);
        assertEquals("Alice", name.asString().getValue());

        BsonValue email = SelectorMatcher.match("contact.email", user);
        assertEquals("alice@example.com", email.asString().getValue());

        BsonValue firstPhone = SelectorMatcher.match("contact.phones.0", user);
        assertEquals("123-456-7890", firstPhone.asString().getValue());

        BsonValue missing = SelectorMatcher.match("contact.address", user);
        assertNull(missing);
    }

    // Test class JavaDoc usage examples
    @Test
    void testClassJavaDocExample() {
        Document doc = new Document()
                .append("user", new Document()
                        .append("name", "John")
                        .append("age", 30))
                .append("scores", Arrays.asList(95, 87, 92));

        BsonValue userName = SelectorMatcher.match("user.name", doc);
        assertEquals("John", userName.asString().getValue());

        BsonValue firstScore = SelectorMatcher.match("scores.0", doc);
        assertEquals(95, firstScore.asInt32().getValue());

        BsonValue missing = SelectorMatcher.match("user.email", doc);
        assertNull(missing);
    }

    // Test type mismatch scenarios
    @Test
    void testTypeMismatchArrayIndexOnPrimitive() {
        Document document = new Document();
        document.append("stringField", new BsonString("text"));

        // Try to access array index on a string
        BsonValue matchedValue = SelectorMatcher.match("stringField.0", document);
        assertNull(matchedValue);
    }

    @Test
    void testTypeMismatchFieldOnArray() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("item"));

        Document document = new Document();
        document.append("arrayField", array);

        // Try to access field name on array (should try to parse as index and fail)
        BsonValue matchedValue = SelectorMatcher.match("arrayField.nonNumeric", document);
        assertNull(matchedValue);
    }

    // Test input validation as mentioned in JavaDoc throws clauses
    @Test
    void testNullSelector() {
        Document document = new Document();
        document.append("key", new BsonString("value"));

        assertThrows(Exception.class, () -> SelectorMatcher.match(null, document));
    }

    @Test
    void testEmptySelector() {
        Document document = new Document();
        document.append("key", new BsonString("value"));

        BsonValue result = SelectorMatcher.match("", document);
        assertNull(result);
    }

    // Test arrays within documents within arrays (from JavaDoc example)
    @Test
    void testArraysWithinDocumentsWithinArrays() {
        BsonArray tags = new BsonArray();
        tags.add(new BsonString("tag1"));
        tags.add(new BsonString("tag2"));

        BsonDocument metadata = new BsonDocument();
        metadata.append("tags", tags);

        BsonDocument dataItem = new BsonDocument();
        dataItem.append("metadata", metadata);

        BsonArray dataArray = new BsonArray();
        dataArray.add(dataItem);

        Document document = new Document();
        document.append("data", dataArray);

        BsonValue matchedValue = SelectorMatcher.match("data.0.metadata.tags.0", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("tag1", matchedValue.asString().getValue());
    }
}