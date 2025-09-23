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

package com.kronotop.bucket;

import org.bson.*;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BSONUtilTest {

    @Test
    void test_toBsonValue_primitiveTypes() {
        // String
        assertEquals(new BsonString("hello"), BSONUtil.toBsonValue("hello"));

        // Integer
        assertEquals(new BsonInt32(42), BSONUtil.toBsonValue(42));

        // Long
        assertEquals(new BsonInt64(123L), BSONUtil.toBsonValue(123L));

        // Double
        assertEquals(new BsonDouble(3.14), BSONUtil.toBsonValue(3.14));

        // Boolean
        assertEquals(new BsonBoolean(true), BSONUtil.toBsonValue(true));
        assertEquals(new BsonBoolean(false), BSONUtil.toBsonValue(false));

        // Null
        assertEquals(BsonNull.VALUE, BSONUtil.toBsonValue(null));
    }

    @Test
    void test_toBsonValue_dateAndDecimal() {
        // Date
        Date date = new Date(1640995200000L); // 2022-01-01
        assertEquals(new BsonDateTime(1640995200000L), BSONUtil.toBsonValue(date));

        // BigDecimal
        BigDecimal decimal = new BigDecimal("123.456");
        assertEquals(new BsonDecimal128(new Decimal128(decimal)), BSONUtil.toBsonValue(decimal));

        // Decimal128
        Decimal128 decimal128 = new Decimal128(789L);
        assertEquals(new BsonDecimal128(decimal128), BSONUtil.toBsonValue(decimal128));
    }

    @Test
    void test_toBsonValue_binaryAndExistingBsonValue() {
        // byte array
        byte[] bytes = {1, 2, 3, 4};
        assertEquals(new BsonBinary(bytes), BSONUtil.toBsonValue(bytes));

        // Already a BsonValue
        BsonString existing = new BsonString("existing");
        assertSame(existing, BSONUtil.toBsonValue(existing));
    }

    @Test
    void test_toBsonValue_document() {
        Document doc = new Document("name", "John").append("age", 25);
        BsonValue result = BSONUtil.toBsonValue(doc);

        assertInstanceOf(BsonDocument.class, result);
        BsonDocument bsonDoc = (BsonDocument) result;
        assertEquals(new BsonString("John"), bsonDoc.get("name"));
        assertEquals(new BsonInt32(25), bsonDoc.get("age"));
    }

    @Test
    void test_toBsonValue_simpleArray() {
        List<Integer> numbers = Arrays.asList(2, 3, 4);
        BsonValue result = BSONUtil.toBsonValue(numbers);

        assertInstanceOf(BsonArray.class, result);
        BsonArray bsonArray = (BsonArray) result;
        assertEquals(3, bsonArray.size());
        assertEquals(new BsonInt32(2), bsonArray.get(0));
        assertEquals(new BsonInt32(3), bsonArray.get(1));
        assertEquals(new BsonInt32(4), bsonArray.get(2));
    }

    @Test
    void test_toBsonValue_mixedTypeArray() {
        List<Object> mixed = Arrays.asList("hello", 42, true, null);
        BsonValue result = BSONUtil.toBsonValue(mixed);

        assertInstanceOf(BsonArray.class, result);
        BsonArray bsonArray = (BsonArray) result;
        assertEquals(4, bsonArray.size());
        assertEquals(new BsonString("hello"), bsonArray.get(0));
        assertEquals(new BsonInt32(42), bsonArray.get(1));
        assertEquals(new BsonBoolean(true), bsonArray.get(2));
        assertEquals(BsonNull.VALUE, bsonArray.get(3));
    }

    @Test
    void test_toBsonValue_nestedArray() {
        List<Object> nested = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList("a", "b"),
                3
        );
        BsonValue result = BSONUtil.toBsonValue(nested);

        assertInstanceOf(BsonArray.class, result);
        BsonArray bsonArray = (BsonArray) result;
        assertEquals(3, bsonArray.size());

        // First nested array [1, 2]
        assertInstanceOf(BsonArray.class, bsonArray.get(0));
        BsonArray firstNested = bsonArray.get(0).asArray();
        assertEquals(new BsonInt32(1), firstNested.get(0));
        assertEquals(new BsonInt32(2), firstNested.get(1));

        // Second nested array ["a", "b"]
        assertInstanceOf(BsonArray.class, bsonArray.get(1));
        BsonArray secondNested = bsonArray.get(1).asArray();
        assertEquals(new BsonString("a"), secondNested.get(0));
        assertEquals(new BsonString("b"), secondNested.get(1));

        // Simple element
        assertEquals(new BsonInt32(3), bsonArray.get(2));
    }

    @Test
    void test_toBsonValue_objectArray() {
        Object[] array = {"test", 123, false};
        BsonValue result = BSONUtil.toBsonValue(array);

        assertInstanceOf(BsonArray.class, result);
        BsonArray bsonArray = (BsonArray) result;
        assertEquals(3, bsonArray.size());
        assertEquals(new BsonString("test"), bsonArray.get(0));
        assertEquals(new BsonInt32(123), bsonArray.get(1));
        assertEquals(new BsonBoolean(false), bsonArray.get(2));
    }

    @Test
    void test_toBsonValue_bsonTypes() {
        // Test that existing BsonValue types are returned as-is
        BsonString bsonString = new BsonString("test");
        BsonInt32 bsonInt32 = new BsonInt32(123);
        BsonBinary bsonBinary = new BsonBinary("binary data".getBytes());
        BsonDecimal128 bsonDecimal = new BsonDecimal128(new Decimal128(456L));

        // Should return the same instances
        assertSame(bsonString, BSONUtil.toBsonValue(bsonString));
        assertSame(bsonInt32, BSONUtil.toBsonValue(bsonInt32));
        assertSame(bsonBinary, BSONUtil.toBsonValue(bsonBinary));
        assertSame(bsonDecimal, BSONUtil.toBsonValue(bsonDecimal));
    }

    @Test
    void test_toBsonValue_binaryTypes() {
        // Test org.bson.types.Binary conversion
        Binary binary = new Binary("Hello World".getBytes());
        BsonValue result = BSONUtil.toBsonValue(binary);

        assertInstanceOf(BsonBinary.class, result);
        BsonBinary bsonBinary = (BsonBinary) result;
        assertArrayEquals("Hello World".getBytes(), bsonBinary.getData());
    }

    @Test
    void test_toBsonValue_unsupportedType() {
        Object unsupported = new Object();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> BSONUtil.toBsonValue(unsupported)
        );

        assertTrue(exception.getMessage().contains("Unsupported value type for BSON conversion"));
        assertTrue(exception.getMessage().contains("Object"));
    }
}