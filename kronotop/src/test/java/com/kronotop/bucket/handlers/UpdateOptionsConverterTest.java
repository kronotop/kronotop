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

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.UpdateOptionsConverter;
import com.kronotop.bucket.pipeline.ArrayFilter;
import com.kronotop.bucket.pipeline.UpdateOptions;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UpdateOptionsConverterTest {

    @Test
    void shouldConvertDocumentToUpdateOptionsWithSetAndUnsetArrayList() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("likes", new BsonInt32(2));
        setDoc.append("name", new BsonString("John"));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonString("oldField"));
        unsetArray.add(new BsonString("deprecatedField"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(2, result.setOps().size());
        assertTrue(result.setOps().containsKey("likes"));
        assertTrue(result.setOps().containsKey("name"));
        assertEquals(new BsonInt32(2), result.setOps().get("likes"));
        assertEquals(new BsonString("John"), result.setOps().get("name"));

        assertEquals(2, result.unsetOps().size());
        assertTrue(result.unsetOps().contains("oldField"));
        assertTrue(result.unsetOps().contains("deprecatedField"));
    }

    @Test
    void shouldConvertDocumentToUpdateOptionsWithBsonArray() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("status", new BsonString("active"));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonString("tempField"));
        unsetArray.add(new BsonString("cacheField"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(1, result.setOps().size());
        assertEquals(new BsonString("active"), result.setOps().get("status"));

        assertEquals(2, result.unsetOps().size());
        assertTrue(result.unsetOps().contains("tempField"));
        assertTrue(result.unsetOps().contains("cacheField"));
    }

    @Test
    void shouldConvertDocumentToUpdateOptionsWithOnlySet() {
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("count", new BsonInt32(42));
        setDoc.append("enabled", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(2, result.setOps().size());
        assertEquals(new BsonInt32(42), result.setOps().get("count"));
        assertEquals(new BsonBoolean(true), result.setOps().get("enabled"));
        assertTrue(result.unsetOps().isEmpty());
    }

    @Test
    void shouldConvertDocumentToUpdateOptionsWithOnlyUnset() {
        BsonDocument updateDoc = new BsonDocument();
        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonString("field1"));
        unsetArray.add(new BsonString("field2"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertTrue(result.setOps().isEmpty());
        assertEquals(2, result.unsetOps().size());
        assertTrue(result.unsetOps().contains("field1"));
        assertTrue(result.unsetOps().contains("field2"));
    }

    @Test
    void shouldThrowExceptionForInvalidUnsetKey() {
        BsonDocument updateDoc = new BsonDocument();
        BsonArray unsetArray = new BsonArray();
        unsetArray.add(new BsonInt32(123));
        unsetArray.add(new BsonString("validField"));
        updateDoc.append(UpdateOptions.UNSET, unsetArray);

        assertThrows(IllegalArgumentException.class, () -> {
            UpdateOptionsConverter.fromDocument(updateDoc);
        });
    }

    // ==================== array_filters Tests ====================

    @Test
    void shouldConvertArrayFiltersWithOperator() {
        // Behavior: array_filters like [{ "score": { "$gte": 8 } }] should be parsed correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("items.$[elem].value", new BsonInt32(100));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$gte", new BsonInt32(8)));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(1, result.arrayFilters().size());
        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals("elem", filter.identifier());
        assertEquals(Operator.GTE, filter.op());
        assertEquals(8, filter.operand());
    }

    @Test
    void shouldConvertArrayFiltersWithImplicitEq() {
        // Behavior: array_filters like [{ "score": 5 }] should use implicit $eq.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("items.$[elem]", new BsonString("updated"));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonInt32(5));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(1, result.arrayFilters().size());
        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals("elem", filter.identifier());
        assertEquals(Operator.EQ, filter.op());
        assertEquals(5, filter.operand());
    }

    @Test
    void shouldConvertMultipleArrayFilters() {
        // Behavior: Multiple array filters should all be parsed and stored.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("items.$[big].$[small]", new BsonInt32(0));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray arrayFilters = new BsonArray();
        // First filter: { "big": { "$gt": 100 } }
        BsonDocument filter1 = new BsonDocument();
        filter1.append("big", new BsonDocument("$gt", new BsonInt32(100)));
        arrayFilters.add(filter1);
        // Second filter: { "small": { "$lt": 10 } }
        BsonDocument filter2 = new BsonDocument();
        filter2.append("small", new BsonDocument("$lt", new BsonInt32(10)));
        arrayFilters.add(filter2);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(2, result.arrayFilters().size());

        ArrayFilter bigFilter = result.arrayFilters().get("big");
        assertNotNull(bigFilter);
        assertEquals(Operator.GT, bigFilter.op());
        assertEquals(100, bigFilter.operand());

        ArrayFilter smallFilter = result.arrayFilters().get("small");
        assertNotNull(smallFilter);
        assertEquals(Operator.LT, smallFilter.op());
        assertEquals(10, smallFilter.operand());
    }

    @Test
    void shouldConvertArrayFilterWithStringOperand() {
        // Behavior: array_filters with string operands should work correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("tags.$[tag]", new BsonString("UPDATED"));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("tag", new BsonDocument("$eq", new BsonString("java")));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertEquals(1, result.arrayFilters().size());
        ArrayFilter filter = result.arrayFilters().get("tag");
        assertNotNull(filter);
        assertEquals(Operator.EQ, filter.op());
        assertEquals("java", filter.operand());
    }

    @Test
    void shouldConvertArrayFilterWithAllComparisonOperators() {
        // Behavior: All comparison operators ($eq, $ne, $gt, $gte, $lt, $lte) should be supported.
        String[] operators = {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"};
        Operator[] expectedOps = {Operator.EQ, Operator.NE, Operator.GT, Operator.GTE, Operator.LT, Operator.LTE};

        for (int i = 0; i < operators.length; i++) {
            BsonDocument updateDoc = new BsonDocument();
            BsonDocument setDoc = new BsonDocument();
            setDoc.append("x.$[elem]", new BsonInt32(1));
            updateDoc.append(UpdateOptions.SET, setDoc);

            BsonArray arrayFilters = new BsonArray();
            BsonDocument filterDoc = new BsonDocument();
            filterDoc.append("elem", new BsonDocument(operators[i], new BsonInt32(10)));
            arrayFilters.add(filterDoc);
            updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

            UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

            ArrayFilter filter = result.arrayFilters().get("elem");
            assertNotNull(filter);
            assertEquals(expectedOps[i], filter.op(), "Failed for operator: " + operators[i]);
        }
    }

    // ==================== Extended Type Support Tests ====================

    @Test
    void shouldConvertArrayFilterWithDateTimeOperand() {
        // Behavior: DateTime operands should be preserved as BsonDateTime for type disambiguation.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("events.$[elem].processed", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        long timestamp = 1609459200000L; // 2021-01-01T00:00:00Z
        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$gte", new BsonDateTime(timestamp)));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.GTE, filter.op());
        assertInstanceOf(BsonDateTime.class, filter.operand());
        assertEquals(timestamp, ((BsonDateTime) filter.operand()).getValue());
    }

    @Test
    void shouldConvertArrayFilterWithTimestampOperand() {
        // Behavior: Timestamp operands should be preserved as BsonTimestamp for type disambiguation.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("logs.$[elem].archived", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$lt", ts));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.LT, filter.op());
        assertInstanceOf(BsonTimestamp.class, filter.operand());
        assertEquals(ts.getValue(), ((BsonTimestamp) filter.operand()).getValue());
    }

    @Test
    void shouldConvertArrayFilterWithBinaryOperand() {
        // Behavior: Binary operands should be preserved as BsonBinary.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("data.$[elem].valid", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        byte[] binaryData = new byte[]{1, 2, 3, 4, 5};
        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$eq", new BsonBinary(binaryData)));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.EQ, filter.op());
        assertInstanceOf(BsonBinary.class, filter.operand());
        assertArrayEquals(binaryData, ((BsonBinary) filter.operand()).getData());
    }

    @Test
    void shouldConvertArrayFilterWithArrayOperand() {
        // Behavior: Array operands should be preserved as BsonArray.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("matrix.$[elem]", new BsonInt32(0));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray expectedArray = new BsonArray();
        expectedArray.add(new BsonInt32(1));
        expectedArray.add(new BsonInt32(2));
        expectedArray.add(new BsonInt32(3));

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$eq", expectedArray));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.EQ, filter.op());
        assertInstanceOf(BsonArray.class, filter.operand());
        BsonArray actualArray = (BsonArray) filter.operand();
        assertEquals(3, actualArray.size());
        assertEquals(1, actualArray.get(0).asInt32().getValue());
        assertEquals(2, actualArray.get(1).asInt32().getValue());
        assertEquals(3, actualArray.get(2).asInt32().getValue());
    }

    @Test
    void shouldConvertArrayFilterWithDocumentOperand() {
        // Behavior: Document operands (for nested document matching) should be preserved as BsonDocument.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("records.$[elem].verified", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonDocument expectedDoc = new BsonDocument();
        expectedDoc.append("type", new BsonString("premium"));
        expectedDoc.append("level", new BsonInt32(5));

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$eq", expectedDoc));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.EQ, filter.op());
        assertInstanceOf(BsonDocument.class, filter.operand());
        BsonDocument actualDoc = (BsonDocument) filter.operand();
        assertEquals("premium", actualDoc.getString("type").getValue());
        assertEquals(5, actualDoc.getInt32("level").getValue());
    }

    // ==================== $in, $nin Operator Tests ====================

    @Test
    void shouldConvertArrayFilterWithInOperator() {
        // Behavior: array_filters with $in should be parsed correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("scores.$[elem]", new BsonInt32(100));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray inValues = new BsonArray();
        inValues.add(new BsonInt32(85));
        inValues.add(new BsonInt32(90));

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$in", inValues));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.IN, filter.op());
        assertInstanceOf(BsonArray.class, filter.operand());
        BsonArray actualArray = (BsonArray) filter.operand();
        assertEquals(2, actualArray.size());
        assertEquals(85, actualArray.get(0).asInt32().getValue());
        assertEquals(90, actualArray.get(1).asInt32().getValue());
    }

    @Test
    void shouldConvertArrayFilterWithNinOperator() {
        // Behavior: array_filters with $nin should be parsed correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("scores.$[elem]", new BsonInt32(0));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray ninValues = new BsonArray();
        ninValues.add(new BsonInt32(85));
        ninValues.add(new BsonInt32(90));

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$nin", ninValues));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.NIN, filter.op());
        assertInstanceOf(BsonArray.class, filter.operand());
        BsonArray actualArray = (BsonArray) filter.operand();
        assertEquals(2, actualArray.size());
        assertEquals(85, actualArray.get(0).asInt32().getValue());
        assertEquals(90, actualArray.get(1).asInt32().getValue());
    }

    // ==================== $all, $size, $exists Operator Tests ====================

    @Test
    void shouldConvertArrayFilterWithAllOperator() {
        // Behavior: array_filters with $all should be parsed correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("data.$[elem].matched", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray allValues = new BsonArray();
        allValues.add(new BsonInt32(1));
        allValues.add(new BsonInt32(2));

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$all", allValues));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.ALL, filter.op());
        assertInstanceOf(BsonArray.class, filter.operand());
        BsonArray actualArray = (BsonArray) filter.operand();
        assertEquals(2, actualArray.size());
        assertEquals(1, actualArray.get(0).asInt32().getValue());
        assertEquals(2, actualArray.get(1).asInt32().getValue());
    }

    @Test
    void shouldConvertArrayFilterWithSizeOperator() {
        // Behavior: array_filters with $size should be parsed correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("data.$[elem].matched", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem", new BsonDocument("$size", new BsonInt32(3)));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals(Operator.SIZE, filter.op());
        assertEquals(3, filter.operand());
    }

    @Test
    void shouldConvertArrayFilterWithExistsTrueOperator() {
        // Behavior: array_filters with $exists: true should be parsed correctly.
        // Note: The identifier "elem.field" is stored with map key "elem" (base identifier before dot).
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("items.$[elem].checked", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem.field", new BsonDocument("$exists", new BsonBoolean(true)));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        // Look up by base identifier "elem"
        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals("elem.field", filter.identifier());
        assertEquals(Operator.EXISTS, filter.op());
        assertEquals(true, filter.operand());
    }

    @Test
    void shouldConvertArrayFilterWithExistsFalseOperator() {
        // Behavior: array_filters with $exists: false should be parsed correctly.
        // Note: The identifier "elem.optionalField" is stored with map key "elem" (base identifier before dot).
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("items.$[elem].missing", new BsonBoolean(true));
        updateDoc.append(UpdateOptions.SET, setDoc);

        BsonArray arrayFilters = new BsonArray();
        BsonDocument filterDoc = new BsonDocument();
        filterDoc.append("elem.optionalField", new BsonDocument("$exists", new BsonBoolean(false)));
        arrayFilters.add(filterDoc);
        updateDoc.append(UpdateOptions.ARRAY_FILTERS, arrayFilters);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        // Look up by base identifier "elem"
        ArrayFilter filter = result.arrayFilters().get("elem");
        assertNotNull(filter);
        assertEquals("elem.optionalField", filter.identifier());
        assertEquals(Operator.EXISTS, filter.op());
        assertEquals(false, filter.operand());
    }

    // ==================== Upsert Tests ====================

    @Test
    void shouldParseUpsertTrue() {
        // Behavior: upsert: true should be parsed correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("field", new BsonString("value"));
        updateDoc.append(UpdateOptions.SET, setDoc);
        updateDoc.append(UpdateOptions.UPSERT, new BsonBoolean(true));

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertTrue(result.upsert(), "upsert should be true");
    }

    @Test
    void shouldParseUpsertFalse() {
        // Behavior: upsert: false should be parsed correctly.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("field", new BsonString("value"));
        updateDoc.append(UpdateOptions.SET, setDoc);
        updateDoc.append(UpdateOptions.UPSERT, new BsonBoolean(false));

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertFalse(result.upsert(), "upsert should be false");
    }

    @Test
    void shouldDefaultUpsertToFalse() {
        // Behavior: When upsert is not specified, it should default to false.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("field", new BsonString("value"));
        updateDoc.append(UpdateOptions.SET, setDoc);

        UpdateOptions result = UpdateOptionsConverter.fromDocument(updateDoc);

        assertFalse(result.upsert(), "upsert should default to false");
    }

    @Test
    void shouldThrowOnInvalidUpsertType() {
        // Behavior: Non-boolean upsert should throw an exception.
        BsonDocument updateDoc = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        setDoc.append("field", new BsonString("value"));
        updateDoc.append(UpdateOptions.SET, setDoc);
        updateDoc.append(UpdateOptions.UPSERT, new BsonString("true")); // String instead of boolean

        assertThrows(IllegalArgumentException.class, () -> {
            UpdateOptionsConverter.fromDocument(updateDoc);
        });
    }
}
