/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.internal.StringUtil;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class SelectorMatcherTest {
    @Test
    void shouldMatchRootLevelField() {
        BsonDocument document = new BsonDocument();
        document.append("key", new BsonString("value"));
        BsonValue matchedValue = SelectorMatcher.match("key", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("value", matchedValue.asString().getValue());
    }

    @Test
    void shouldMatchWithByteBuffer() {
        BsonDocument document = new BsonDocument();
        document.append("key", new BsonString("value"));
        ByteBuffer buffer = ByteBuffer.wrap(BSONUtil.toBytes(document));
        BsonValue matchedValue = SelectorMatcher.match("key", buffer);

        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("value", matchedValue.asString().getValue());
    }

    @Test
    void shouldMatchNestedDocument() {
        BsonDocument nested = new BsonDocument();
        nested.append("innerKey", new BsonString("innerValue"));

        BsonDocument document = new BsonDocument();
        document.append("outer", nested);

        BsonValue matchedValue = SelectorMatcher.match("outer.innerKey", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("innerValue", matchedValue.asString().getValue());
    }

    @Test
    void shouldMatchArrayAccess() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("first"));
        array.add(new BsonString("second"));
        array.add(new BsonString("third"));

        BsonDocument document = new BsonDocument();
        document.append("items", array);

        BsonValue matchedValue = SelectorMatcher.match("items.1", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("second", matchedValue.asString().getValue());
    }

    @Test
    void shouldMatchNestedArrayInDocument() {
        BsonDocument innerDoc = new BsonDocument();
        innerDoc.append("name", new BsonString("test"));

        BsonArray array = new BsonArray();
        array.add(innerDoc);

        BsonDocument document = new BsonDocument();
        document.append("data", array);

        BsonValue matchedValue = SelectorMatcher.match("data.0.name", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("test", matchedValue.asString().getValue());
    }

    @Test
    void shouldMatchDeeplyNestedPath() {
        BsonDocument level3 = new BsonDocument();
        level3.append("finalKey", new BsonInt32(42));

        BsonDocument level2 = new BsonDocument();
        level2.append("level3", level3);

        BsonDocument level1 = new BsonDocument();
        level1.append("level2", level2);

        BsonDocument document = new BsonDocument();
        document.append("level1", level1);

        BsonValue matchedValue = SelectorMatcher.match("level1.level2.level3.finalKey", document);
        assertInstanceOf(BsonInt32.class, matchedValue);
        assertEquals(42, matchedValue.asInt32().getValue());
    }

    @Test
    void shouldReturnNullForNonExistentPath() {
        BsonDocument document = new BsonDocument();
        document.append("key", new BsonString("value"));

        BsonValue matchedValue = SelectorMatcher.match("nonexistent", document);
        assertNull(matchedValue);
    }

    @Test
    void shouldReturnNullForInvalidArrayIndex() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("first"));

        BsonDocument document = new BsonDocument();
        document.append("items", array);

        BsonValue matchedValue = SelectorMatcher.match("items.5", document);
        assertNull(matchedValue);
    }

    // Test all BSON types mentioned in JavaDoc
    @Test
    void shouldMatchBsonInt64() {
        BsonDocument document = new BsonDocument();
        document.append("longValue", new BsonInt64(9223372036854775807L));

        BsonValue matchedValue = SelectorMatcher.match("longValue", document);
        assertInstanceOf(BsonInt64.class, matchedValue);
        assertEquals(9223372036854775807L, matchedValue.asInt64().getValue());
    }

    @Test
    void shouldMatchBsonDouble() {
        BsonDocument document = new BsonDocument();
        document.append("doubleValue", new BsonDouble(3.14159));

        BsonValue matchedValue = SelectorMatcher.match("doubleValue", document);
        assertInstanceOf(BsonDouble.class, matchedValue);
        assertEquals(3.14159, matchedValue.asDouble().getValue(), 0.00001);
    }

    @Test
    void shouldMatchBsonBoolean() {
        BsonDocument document = new BsonDocument();
        document.append("boolValue", new BsonBoolean(true));

        BsonValue matchedValue = SelectorMatcher.match("boolValue", document);
        assertInstanceOf(BsonBoolean.class, matchedValue);
        assertTrue(matchedValue.asBoolean().getValue());
    }

    @Test
    void shouldMatchBsonNull() {
        BsonDocument document = new BsonDocument();
        document.append("nullValue", new BsonNull());

        BsonValue matchedValue = SelectorMatcher.match("nullValue", document);
        assertInstanceOf(BsonNull.class, matchedValue);
    }

    @Test
    void shouldMatchBsonDateTime() {
        Date now = new Date();
        BsonDocument document = new BsonDocument();
        document.append("dateValue", new BsonDateTime(now.getTime()));

        BsonValue matchedValue = SelectorMatcher.match("dateValue", document);
        assertInstanceOf(BsonDateTime.class, matchedValue);
        assertEquals(now.getTime(), matchedValue.asDateTime().getValue());
    }

    @Test
    void shouldMatchNestedBsonDocument() {
        BsonDocument nestedDoc = new BsonDocument();
        nestedDoc.append("innerField", new BsonString("innerValue"));

        BsonDocument document = new BsonDocument();
        document.append("nested", nestedDoc);

        BsonValue matchedValue = SelectorMatcher.match("nested", document);
        assertInstanceOf(BsonDocument.class, matchedValue);
        assertEquals("innerValue", matchedValue.asDocument().getString("innerField").getValue());
    }

    @Test
    void shouldMatchNestedBsonArray() {
        BsonArray nestedArray = new BsonArray();
        nestedArray.add(new BsonString("item1"));
        nestedArray.add(new BsonString("item2"));

        BsonDocument document = new BsonDocument();
        document.append("arrayField", nestedArray);

        BsonValue matchedValue = SelectorMatcher.match("arrayField", document);
        assertInstanceOf(BsonArray.class, matchedValue);
        assertEquals(2, matchedValue.asArray().size());
        assertEquals("item1", matchedValue.asArray().get(0).asString().getValue());
    }

    // Test invalid array index with non-numeric string
    @Test
    void shouldReturnNullForNonNumericArrayIndex() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("first"));

        BsonDocument document = new BsonDocument();
        document.append("items", array);

        BsonValue matchedValue = SelectorMatcher.match("items.abc", document);
        assertNull(matchedValue);
    }

    // Test complex nested structures from JavaDoc examples
    @Test
    void shouldMatchComplexNestedStructure() {
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

        BsonDocument company = new BsonDocument();
        company.append("departments", departments);

        BsonDocument document = new BsonDocument();
        document.append("company", company);

        BsonValue matchedValue = SelectorMatcher.match("company.departments.0.employees.5.address.coordinates.latitude", document);
        assertInstanceOf(BsonDouble.class, matchedValue);
        assertEquals(40.7128, matchedValue.asDouble().getValue(), 0.0001);
    }

    // Test the exact example from method JavaDoc
    @Test
    void shouldMatchMethodJavaDocExample() {
        BsonArray phones = new BsonArray();
        phones.add(new BsonString("123-456-7890"));
        phones.add(new BsonString("987-654-3210"));

        BsonDocument user = new BsonDocument()
                .append("name", new BsonString("Alice"))
                .append("contact", new BsonDocument()
                        .append("email", new BsonString("alice@example.com"))
                        .append("phones", phones));

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
    void shouldMatchClassJavaDocExample() {
        BsonArray scores = new BsonArray();
        scores.add(new BsonInt32(95));
        scores.add(new BsonInt32(87));
        scores.add(new BsonInt32(92));

        BsonDocument doc = new BsonDocument()
                .append("user", new BsonDocument()
                        .append("name", new BsonString("John"))
                        .append("age", new BsonInt32(30)))
                .append("scores", scores);

        BsonValue userName = SelectorMatcher.match("user.name", doc);
        assertEquals("John", userName.asString().getValue());

        BsonValue firstScore = SelectorMatcher.match("scores.0", doc);
        assertEquals(95, firstScore.asInt32().getValue());

        BsonValue missing = SelectorMatcher.match("user.email", doc);
        assertNull(missing);
    }

    // Test type mismatch scenarios
    @Test
    void shouldReturnNullForTypeMismatchArrayIndexOnPrimitive() {
        BsonDocument document = new BsonDocument();
        document.append("stringField", new BsonString("text"));

        // Try to access array index on a string
        BsonValue matchedValue = SelectorMatcher.match("stringField.0", document);
        assertNull(matchedValue);
    }

    @Test
    void shouldReturnNullForTypeMismatchFieldOnArray() {
        BsonArray array = new BsonArray();
        array.add(new BsonString("item"));

        BsonDocument document = new BsonDocument();
        document.append("arrayField", array);

        // Try to access field name on array (should try to parse as index and fail)
        BsonValue matchedValue = SelectorMatcher.match("arrayField.nonNumeric", document);
        assertNull(matchedValue);
    }

    // Test input validation as mentioned in JavaDoc throws clauses
    @Test
    void shouldThrowForNullSelector() {
        BsonDocument document = new BsonDocument();
        document.append("key", new BsonString("value"));

        assertThrows(Exception.class, () -> SelectorMatcher.match((String) null, document));
    }

    @Test
    void shouldReturnNullForEmptySelector() {
        BsonDocument document = new BsonDocument();
        document.append("key", new BsonString("value"));

        BsonValue result = SelectorMatcher.match("", document);
        assertNull(result);
    }

    // Test arrays within documents within arrays (from JavaDoc example)
    @Test
    void shouldMatchArraysWithinDocumentsWithinArrays() {
        BsonArray tags = new BsonArray();
        tags.add(new BsonString("tag1"));
        tags.add(new BsonString("tag2"));

        BsonDocument metadata = new BsonDocument();
        metadata.append("tags", tags);

        BsonDocument dataItem = new BsonDocument();
        dataItem.append("metadata", metadata);

        BsonArray dataArray = new BsonArray();
        dataArray.add(dataItem);

        BsonDocument document = new BsonDocument();
        document.append("data", dataArray);

        BsonValue matchedValue = SelectorMatcher.match("data.0.metadata.tags.0", document);
        assertInstanceOf(BsonString.class, matchedValue);
        assertEquals("tag1", matchedValue.asString().getValue());
    }

    @Test
    void shouldMatchBsonBinary() {
        byte[] binaryData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        BsonDocument document = new BsonDocument();
        document.append("binaryField", new BsonBinary(binaryData));

        BsonValue matchedValue = SelectorMatcher.match("binaryField", document);
        assertInstanceOf(BsonBinary.class, matchedValue);
        assertArrayEquals(binaryData, matchedValue.asBinary().getData());
    }

    @Test
    void shouldMatchBsonBinaryWithByteBuffer() {
        byte[] binaryData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        BsonDocument document = new BsonDocument();
        document.append("binaryField", new BsonBinary(binaryData));
        ByteBuffer buffer = ByteBuffer.wrap(BSONUtil.toBytes(document));

        BsonValue matchedValue = SelectorMatcher.match("binaryField", buffer);
        assertInstanceOf(BsonBinary.class, matchedValue);
        assertArrayEquals(binaryData, matchedValue.asBinary().getData());
    }

    @Test
    void shouldMatchBsonTimestamp() {
        long timestampValue = 1234567890L;
        BsonDocument document = new BsonDocument();
        document.append("timestampField", new BsonTimestamp(timestampValue));

        BsonValue matchedValue = SelectorMatcher.match("timestampField", document);
        assertInstanceOf(BsonTimestamp.class, matchedValue);
        assertEquals(timestampValue, matchedValue.asTimestamp().getValue());
    }

    @Test
    void shouldMatchBsonTimestampWithByteBuffer() {
        long timestampValue = 1234567890L;
        BsonDocument document = new BsonDocument();
        document.append("timestampField", new BsonTimestamp(timestampValue));
        ByteBuffer buffer = ByteBuffer.wrap(BSONUtil.toBytes(document));

        BsonValue matchedValue = SelectorMatcher.match("timestampField", buffer);
        assertInstanceOf(BsonTimestamp.class, matchedValue);
        assertEquals(timestampValue, matchedValue.asTimestamp().getValue());
    }

    @Test
    void shouldMatchBsonDecimal128() {
        BigDecimal decimalValue = new BigDecimal("12345.6789");
        BsonDocument document = new BsonDocument();
        document.append("decimalField", new BsonDecimal128(new Decimal128(decimalValue)));

        BsonValue matchedValue = SelectorMatcher.match("decimalField", document);
        assertInstanceOf(BsonDecimal128.class, matchedValue);
        assertEquals(decimalValue, matchedValue.asDecimal128().decimal128Value().bigDecimalValue());
    }

    @Test
    void shouldMatchBsonDecimal128WithByteBuffer() {
        BigDecimal decimalValue = new BigDecimal("12345.6789");
        BsonDocument document = new BsonDocument();
        document.append("decimalField", new BsonDecimal128(new Decimal128(decimalValue)));
        ByteBuffer buffer = ByteBuffer.wrap(BSONUtil.toBytes(document));

        BsonValue matchedValue = SelectorMatcher.match("decimalField", buffer);
        assertInstanceOf(BsonDecimal128.class, matchedValue);
        assertEquals(decimalValue, matchedValue.asDecimal128().decimal128Value().bigDecimalValue());
    }

    @Test
    void shouldMatchNestedBsonBinary() {
        byte[] binaryData = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        BsonDocument nested = new BsonDocument();
        nested.append("data", new BsonBinary(binaryData));

        BsonDocument document = new BsonDocument();
        document.append("payload", nested);

        BsonValue matchedValue = SelectorMatcher.match("payload.data", document);
        assertInstanceOf(BsonBinary.class, matchedValue);
        assertArrayEquals(binaryData, matchedValue.asBinary().getData());
    }

    @Test
    void shouldMatchNestedBsonTimestamp() {
        long timestampValue = 9876543210L;
        BsonDocument nested = new BsonDocument();
        nested.append("createdAt", new BsonTimestamp(timestampValue));

        BsonDocument document = new BsonDocument();
        document.append("metadata", nested);

        BsonValue matchedValue = SelectorMatcher.match("metadata.createdAt", document);
        assertInstanceOf(BsonTimestamp.class, matchedValue);
        assertEquals(timestampValue, matchedValue.asTimestamp().getValue());
    }

    @Test
    void shouldMatchNestedBsonDecimal128() {
        BigDecimal decimalValue = new BigDecimal("99999.99");
        BsonDocument nested = new BsonDocument();
        nested.append("price", new BsonDecimal128(new Decimal128(decimalValue)));

        BsonDocument document = new BsonDocument();
        document.append("product", nested);

        BsonValue matchedValue = SelectorMatcher.match("product.price", document);
        assertInstanceOf(BsonDecimal128.class, matchedValue);
        assertEquals(decimalValue, matchedValue.asDecimal128().decimal128Value().bigDecimalValue());
    }

    @Test
    void shouldMatchFieldInArrayOfDocuments() {
        // BsonDocument: { scores: [ { type: "math", score: 90 }, { type: "english", score: 70 } ] }
        BsonDocument score1 = new BsonDocument();
        score1.append("type", new BsonString("math"));
        score1.append("score", new BsonInt32(90));

        BsonDocument score2 = new BsonDocument();
        score2.append("type", new BsonString("english"));
        score2.append("score", new BsonInt32(70));

        BsonArray scoresArray = new BsonArray();
        scoresArray.add(score1);
        scoresArray.add(score2);

        BsonDocument document = new BsonDocument();
        document.append("scores", scoresArray);

        // "scores.type" should return an array of all "type" values from the array elements
        BsonValue typeValues = SelectorMatcher.match("scores.type", document);
        assertNotNull(typeValues);
        assertInstanceOf(BsonArray.class, typeValues);
        BsonArray typeArray = typeValues.asArray();
        assertEquals(2, typeArray.size());
        assertEquals("math", typeArray.get(0).asString().getValue());
        assertEquals("english", typeArray.get(1).asString().getValue());

        // "scores.score" should return an array of all "score" values from the array elements
        BsonValue scoreValues = SelectorMatcher.match("scores.score", document);
        assertNotNull(scoreValues);
        assertInstanceOf(BsonArray.class, scoreValues);
        BsonArray scoreArray = scoreValues.asArray();
        assertEquals(2, scoreArray.size());
        assertEquals(90, scoreArray.get(0).asInt32().getValue());
        assertEquals(70, scoreArray.get(1).asInt32().getValue());
    }

    @Test
    void shouldMatchFieldInNestedArraysOfDocuments() {
        // Document structure:
        // {
        //   "departments": [
        //     {
        //       "name": "Electronics",
        //       "products": [
        //         {"sku": "TV1", "price": 500},
        //         {"sku": "TV2", "price": 800}
        //       ]
        //     },
        //     {
        //       "name": "Books",
        //       "products": [
        //         {"sku": "B1", "price": 20}
        //       ]
        //     }
        //   ]
        // }

        BsonDocument product1 = new BsonDocument()
                .append("sku", new BsonString("TV1"))
                .append("price", new BsonInt32(500));
        BsonDocument product2 = new BsonDocument()
                .append("sku", new BsonString("TV2"))
                .append("price", new BsonInt32(800));
        BsonDocument product3 = new BsonDocument()
                .append("sku", new BsonString("B1"))
                .append("price", new BsonInt32(20));

        BsonArray electronicsProducts = new BsonArray();
        electronicsProducts.add(product1);
        electronicsProducts.add(product2);

        BsonArray booksProducts = new BsonArray();
        booksProducts.add(product3);

        BsonDocument electronics = new BsonDocument()
                .append("name", new BsonString("Electronics"))
                .append("products", electronicsProducts);

        BsonDocument books = new BsonDocument()
                .append("name", new BsonString("Books"))
                .append("products", booksProducts);

        BsonArray departments = new BsonArray();
        departments.add(electronics);
        departments.add(books);

        BsonDocument document = new BsonDocument();
        document.append("departments", departments);

        // "departments.products.price" should return a flattened array of all prices
        // from all products across all departments: [500, 800, 20]
        BsonValue priceValues = SelectorMatcher.match("departments.products.price", document);
        assertNotNull(priceValues);
        assertInstanceOf(BsonArray.class, priceValues);
        BsonArray priceArray = priceValues.asArray();
        assertEquals(3, priceArray.size());
        assertEquals(500, priceArray.get(0).asInt32().getValue());
        assertEquals(800, priceArray.get(1).asInt32().getValue());
        assertEquals(20, priceArray.get(2).asInt32().getValue());
    }

    @Test
    void shouldMatchFieldInDeeplyNestedArraysOfDocuments() {
        // Document structure with 5 levels of nested arrays:
        // {
        //   "org": [
        //     {
        //       "divisions": [
        //         {
        //           "teams": [
        //             {
        //               "members": [
        //                 {
        //                   "skills": [
        //                     {"name": "Java", "level": 5},
        //                     {"name": "Python", "level": 3}
        //                   ]
        //                 },
        //                 {
        //                   "skills": [
        //                     {"name": "Go", "level": 4}
        //                   ]
        //                 }
        //               ]
        //             }
        //           ]
        //         },
        //         {
        //           "teams": [
        //             {
        //               "members": [
        //                 {
        //                   "skills": [
        //                     {"name": "Rust", "level": 2}
        //                   ]
        //                 }
        //               ]
        //             }
        //           ]
        //         }
        //       ]
        //     },
        //     {
        //       "divisions": [
        //         {
        //           "teams": [
        //             {
        //               "members": [
        //                 {
        //                   "skills": [
        //                     {"name": "C++", "level": 6}
        //                   ]
        //                 }
        //               ]
        //             }
        //           ]
        //         }
        //       ]
        //     }
        //   ]
        // }

        // Build from innermost to outermost

        // Skills arrays
        BsonArray skills1 = new BsonArray();
        skills1.add(new BsonDocument().append("name", new BsonString("Java")).append("level", new BsonInt32(5)));
        skills1.add(new BsonDocument().append("name", new BsonString("Python")).append("level", new BsonInt32(3)));

        BsonArray skills2 = new BsonArray();
        skills2.add(new BsonDocument().append("name", new BsonString("Go")).append("level", new BsonInt32(4)));

        BsonArray skills3 = new BsonArray();
        skills3.add(new BsonDocument().append("name", new BsonString("Rust")).append("level", new BsonInt32(2)));

        BsonArray skills4 = new BsonArray();
        skills4.add(new BsonDocument().append("name", new BsonString("C++")).append("level", new BsonInt32(6)));

        // Members arrays
        BsonArray members1 = new BsonArray();
        members1.add(new BsonDocument().append("skills", skills1));
        members1.add(new BsonDocument().append("skills", skills2));

        BsonArray members2 = new BsonArray();
        members2.add(new BsonDocument().append("skills", skills3));

        BsonArray members3 = new BsonArray();
        members3.add(new BsonDocument().append("skills", skills4));

        // Teams arrays
        BsonArray teams1 = new BsonArray();
        teams1.add(new BsonDocument().append("members", members1));

        BsonArray teams2 = new BsonArray();
        teams2.add(new BsonDocument().append("members", members2));

        BsonArray teams3 = new BsonArray();
        teams3.add(new BsonDocument().append("members", members3));

        // Divisions arrays
        BsonArray divisions1 = new BsonArray();
        divisions1.add(new BsonDocument().append("teams", teams1));
        divisions1.add(new BsonDocument().append("teams", teams2));

        BsonArray divisions2 = new BsonArray();
        divisions2.add(new BsonDocument().append("teams", teams3));

        // Org array
        BsonArray org = new BsonArray();
        org.add(new BsonDocument().append("divisions", divisions1));
        org.add(new BsonDocument().append("divisions", divisions2));

        BsonDocument document = new BsonDocument();
        document.append("org", org);

        // "org.divisions.teams.members.skills.level" should return a flattened array
        // of all skill levels: [5, 3, 4, 2, 6]
        BsonValue levelValues = SelectorMatcher.match("org.divisions.teams.members.skills.level", document);
        assertNotNull(levelValues);
        assertInstanceOf(BsonArray.class, levelValues);
        BsonArray levelArray = levelValues.asArray();
        assertEquals(5, levelArray.size());
        assertEquals(5, levelArray.get(0).asInt32().getValue());
        assertEquals(3, levelArray.get(1).asInt32().getValue());
        assertEquals(4, levelArray.get(2).asInt32().getValue());
        assertEquals(2, levelArray.get(3).asInt32().getValue());
        assertEquals(6, levelArray.get(4).asInt32().getValue());

        // Also test intermediate paths to verify partial traversal works
        // "org.divisions.teams.members.skills.name" should return all skill names
        BsonValue nameValues = SelectorMatcher.match("org.divisions.teams.members.skills.name", document);
        assertNotNull(nameValues);
        assertInstanceOf(BsonArray.class, nameValues);
        BsonArray nameArray = nameValues.asArray();
        assertEquals(5, nameArray.size());
        assertEquals("Java", nameArray.get(0).asString().getValue());
        assertEquals("Python", nameArray.get(1).asString().getValue());
        assertEquals("Go", nameArray.get(2).asString().getValue());
        assertEquals("Rust", nameArray.get(3).asString().getValue());
        assertEquals("C++", nameArray.get(4).asString().getValue());

        // "org.divisions.teams.members.skills" should return a flattened array of all skill documents
        BsonValue skillsValues = SelectorMatcher.match("org.divisions.teams.members.skills", document);
        assertNotNull(skillsValues);
        assertInstanceOf(BsonArray.class, skillsValues);
        BsonArray skillsArray = skillsValues.asArray();
        // Should contain all 5 skill documents flattened from all members
        assertEquals(5, skillsArray.size());
        // Verify each is a document with name and level
        for (BsonValue skill : skillsArray) {
            assertInstanceOf(BsonDocument.class, skill);
            BsonDocument skillDoc = skill.asDocument();
            assertTrue(skillDoc.containsKey("name"));
            assertTrue(skillDoc.containsKey("level"));
        }
        // Verify order and content
        assertEquals("Java", skillsArray.get(0).asDocument().getString("name").getValue());
        assertEquals("Python", skillsArray.get(1).asDocument().getString("name").getValue());
        assertEquals("Go", skillsArray.get(2).asDocument().getString("name").getValue());
        assertEquals("Rust", skillsArray.get(3).asDocument().getString("name").getValue());
        assertEquals("C++", skillsArray.get(4).asDocument().getString("name").getValue());
    }

    @Test
    void shouldMatchBsonObjectId() {
        // Behavior: SelectorMatcher should correctly extract BsonObjectId values from documents.
        ObjectId objectId = new ObjectId();
        BsonDocument document = new BsonDocument();
        document.append("_id", new BsonObjectId(objectId));

        BsonValue matchedValue = SelectorMatcher.match("_id", document);
        assertInstanceOf(BsonObjectId.class, matchedValue);
        assertEquals(objectId, matchedValue.asObjectId().getValue());
    }

    @Test
    void shouldMatchBsonObjectIdWithByteBuffer() {
        // Behavior: SelectorMatcher should correctly extract BsonObjectId values from ByteBuffer input.
        ObjectId objectId = new ObjectId();
        BsonDocument document = new BsonDocument();
        document.append("_id", new BsonObjectId(objectId));
        ByteBuffer buffer = ByteBuffer.wrap(BSONUtil.toBytes(document));

        BsonValue matchedValue = SelectorMatcher.match("_id", buffer);
        assertInstanceOf(BsonObjectId.class, matchedValue);
        assertEquals(objectId, matchedValue.asObjectId().getValue());
    }

    @Test
    void shouldMatchNestedBsonObjectId() {
        // Behavior: SelectorMatcher should correctly extract BsonObjectId values from nested documents.
        ObjectId objectId = new ObjectId();
        BsonDocument nested = new BsonDocument();
        nested.append("ref", new BsonObjectId(objectId));

        BsonDocument document = new BsonDocument();
        document.append("reference", nested);

        BsonValue matchedValue = SelectorMatcher.match("reference.ref", document);
        assertInstanceOf(BsonObjectId.class, matchedValue);
        assertEquals(objectId, matchedValue.asObjectId().getValue());
    }

    @Test
    void shouldMatchWithPreSplitPathSegmentsOnBsonDocument() {
        // Behavior: match(String[], BsonDocument) produces identical results to match(String, BsonDocument).
        BsonDocument nested = new BsonDocument();
        nested.append("innerKey", new BsonString("innerValue"));

        BsonDocument document = new BsonDocument();
        document.append("outer", nested);

        String selector = "outer.innerKey";
        String[] segments = StringUtil.split(selector);

        BsonValue fromString = SelectorMatcher.match(selector, document);
        BsonValue fromArray = SelectorMatcher.match(segments, document);

        assertEquals(fromString, fromArray);
    }

    @Test
    void shouldMatchWithPreSplitPathSegmentsOnByteBuffer() {
        // Behavior: match(String[], ByteBuffer) produces identical results to match(String, ByteBuffer).
        BsonDocument nested = new BsonDocument();
        nested.append("innerKey", new BsonString("innerValue"));

        BsonDocument document = new BsonDocument();
        document.append("outer", nested);

        String selector = "outer.innerKey";
        String[] segments = StringUtil.split(selector);

        ByteBuffer buffer1 = ByteBuffer.wrap(BSONUtil.toBytes(document));
        BsonValue fromString = SelectorMatcher.match(selector, buffer1);

        ByteBuffer buffer2 = ByteBuffer.wrap(BSONUtil.toBytes(document));
        BsonValue fromArray = SelectorMatcher.match(segments, buffer2);

        assertEquals(fromString, fromArray);
    }
}