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
import com.kronotop.bucket.bson.BasicOutputBuffer;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class BSONUpdateUtilTest {

    private ByteBuffer createBsonDocument(BsonDocument doc) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer)) {
            writer.writeStartDocument();
            for (Map.Entry<String, BsonValue> entry : doc.entrySet()) {
                writer.writeName(entry.getKey());
                writeBsonValue(writer, entry.getValue());
            }
            writer.writeEndDocument();
        }
        return ByteBuffer.wrap(outputBuffer.toByteArray());
    }

    private void writeBsonValue(BsonWriter writer, BsonValue value) {
        switch (value.getBsonType()) {
            case STRING -> writer.writeString(value.asString().getValue());
            case INT32 -> writer.writeInt32(value.asInt32().getValue());
            case INT64 -> writer.writeInt64(value.asInt64().getValue());
            case DOUBLE -> writer.writeDouble(value.asDouble().getValue());
            case BOOLEAN -> writer.writeBoolean(value.asBoolean().getValue());
            case TIMESTAMP -> writer.writeTimestamp(value.asTimestamp());
            case DATE_TIME -> writer.writeDateTime(value.asDateTime().getValue());
            case BINARY -> writer.writeBinaryData(value.asBinary());
            case NULL -> writer.writeNull();
        }
    }

    private BsonDocument readBsonDocument(ByteBuffer buffer) {
        buffer.rewind();
        BsonDocument doc = new BsonDocument();
        try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();
                BsonValue value = readBsonValue(reader);
                doc.put(fieldName, value);
            }
            reader.readEndDocument();
        }
        return doc;
    }

    private BsonValue readBsonValue(BsonReader reader) {
        BsonType type = reader.getCurrentBsonType();
        return switch (type) {
            case STRING -> new BsonString(reader.readString());
            case INT32 -> new BsonInt32(reader.readInt32());
            case INT64 -> new BsonInt64(reader.readInt64());
            case DOUBLE -> new BsonDouble(reader.readDouble());
            case BOOLEAN -> new BsonBoolean(reader.readBoolean());
            case TIMESTAMP -> reader.readTimestamp();
            case DATE_TIME -> new BsonDateTime(reader.readDateTime());
            case BINARY -> reader.readBinaryData();
            case NULL -> {
                reader.readNull();
                yield new BsonNull();
            }
            default -> {
                reader.skipValue();
                yield new BsonNull();
            }
        };
    }

    @Test
    void shouldApplySetOperationsWithEmptyUpdateOps() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Apply empty setOps
        Map<String, BsonValue> emptySetOps = Map.of();
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, emptySetOps, Set.of());

        // Should return the same document with empty new values
        assertSame(originalBuffer, result.document());
        assertTrue(result.newValues().isEmpty());
    }

    @Test
    void shouldUpdateExistingStringField() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Update name field
        Map<String, BsonValue> setOps = Map.of("name", new BsonString("Donald"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify update
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("Donald", updatedDoc.getString("name").getValue());
        assertEquals(25, updatedDoc.getInt32("age").getValue());

        // Verify new value
        assertEquals(1, result.newValues().size());
        assertEquals("Donald", result.newValues().get("name").asString().getValue());
    }

    @Test
    void shouldAddNewField() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Add a new field
        Map<String, BsonValue> setOps = Map.of("age", new BsonInt32(30));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify new field added
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("John", updatedDoc.getString("name").getValue());
        assertEquals(30, updatedDoc.getInt32("age").getValue());
    }

    @Test
    void shouldApplyMixedOperationsUpdateAndAdd() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Update the existing and add a new one
        Map<String, BsonValue> setOps = Map.of(
                "name", new BsonString("Alice"),
                "city", new BsonString("New York")
        );
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify changes
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("Alice", updatedDoc.getString("name").getValue());
        assertEquals(25, updatedDoc.getInt32("age").getValue());
        assertEquals("New York", updatedDoc.getString("city").getValue());
    }

    @Test
    void shouldHandleAllBsonDataTypes() {
        // Create a document with all supported BSON types
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("stringField", new BsonString("original"));
        originalDoc.put("int32Field", new BsonInt32(100));
        originalDoc.put("int64Field", new BsonInt64(200L));
        originalDoc.put("doubleField", new BsonDouble(3.14));
        originalDoc.put("booleanField", new BsonBoolean(true));
        originalDoc.put("timestampField", new BsonTimestamp(1000, 1));
        originalDoc.put("dateTimeField", new BsonDateTime(System.currentTimeMillis()));
        originalDoc.put("binaryField", new BsonBinary(new byte[]{1, 2, 3, 4}));
        originalDoc.put("nullField", new BsonNull());
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Update each field
        Map<String, BsonValue> setOps = Map.of(
                "stringField", new BsonString("updated"),
                "int32Field", new BsonInt32(999),
                "int64Field", new BsonInt64(888L),
                "doubleField", new BsonDouble(2.71),
                "booleanField", new BsonBoolean(false)
        );
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify all types are preserved and updated correctly
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("updated", updatedDoc.getString("stringField").getValue());
        assertEquals(999, updatedDoc.getInt32("int32Field").getValue());
        assertEquals(888L, updatedDoc.getInt64("int64Field").getValue());
        assertEquals(2.71, updatedDoc.getDouble("doubleField").getValue(), 0.001);
        assertFalse(updatedDoc.getBoolean("booleanField").getValue());

        // Check that non-updated fields are preserved
        assertTrue(updatedDoc.containsKey("timestampField"));
        assertTrue(updatedDoc.containsKey("dateTimeField"));
        assertTrue(updatedDoc.containsKey("binaryField"));
        assertTrue(updatedDoc.containsKey("nullField"));
        assertEquals(BsonType.TIMESTAMP, updatedDoc.get("timestampField").getBsonType());
        assertEquals(BsonType.DATE_TIME, updatedDoc.get("dateTimeField").getBsonType());
        assertEquals(BsonType.BINARY, updatedDoc.get("binaryField").getBsonType());
        assertEquals(BsonType.NULL, updatedDoc.get("nullField").getBsonType());
    }

    @Test
    void shouldUpdateWithNullValue() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Set field to null
        Map<String, BsonValue> setOps = new HashMap<>();
        setOps.put("name", BsonNull.VALUE);
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify null value
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals(BsonType.NULL, updatedDoc.get("name").getBsonType());
        assertEquals(25, updatedDoc.getInt32("age").getValue());
    }

    @Test
    void shouldConvertToBsonValueWithAllJavaTypes() {
        // Create the document to test conversion
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("existing", new BsonString("test"));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Test all Java types supported by convertToBsonValue
        Map<String, BsonValue> setOps = new HashMap<>();
        setOps.put("nullValue", new BsonNull());
        setOps.put("stringValue", new BsonString("hello"));
        setOps.put("integerValue", new BsonInt32(42));
        setOps.put("longValue", new BsonInt64(123L));
        setOps.put("doubleValue", new BsonDouble(9.99));
        setOps.put("booleanValue", new BsonBoolean(true));
        setOps.put("unknownType", new BsonUndefined()); // Should be converted to string

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify conversions
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals(BsonType.NULL, updatedDoc.get("nullValue").getBsonType());
        assertEquals(BsonType.STRING, updatedDoc.get("stringValue").getBsonType());
        assertEquals("hello", updatedDoc.getString("stringValue").getValue());
        assertEquals(BsonType.INT32, updatedDoc.get("integerValue").getBsonType());
        assertEquals(42, updatedDoc.getInt32("integerValue").getValue());
        assertEquals(BsonType.INT64, updatedDoc.get("longValue").getBsonType());
        assertEquals(123L, updatedDoc.getInt64("longValue").getValue());
        assertEquals(BsonType.DOUBLE, updatedDoc.get("doubleValue").getBsonType());
        assertEquals(9.99, updatedDoc.getDouble("doubleValue").getValue(), 0.001);
        assertEquals(BsonType.BOOLEAN, updatedDoc.get("booleanValue").getBsonType());
        assertTrue(updatedDoc.getBoolean("booleanValue").getValue());
        assertEquals(BsonType.NULL, updatedDoc.get("unknownType").getBsonType());
        // Date should be converted to string representation
        assertEquals(BsonNull.VALUE, updatedDoc.get("unknownType"));
    }

    @Test
    void shouldHandleEmptyDocument() {
        // Create an empty document
        BsonDocument originalDoc = new BsonDocument();
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Add fields to the empty document
        Map<String, BsonValue> setOps = Map.of(
                "name", new BsonString("John"),
                "age", new BsonInt32(30)
        );
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify fields added
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("John", updatedDoc.getString("name").getValue());
        assertEquals(30, updatedDoc.getInt32("age").getValue());
    }

    @Test
    void shouldPreserveFieldOrder() {
        // Create document with multiple fields
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("first", new BsonString("1"));
        originalDoc.put("second", new BsonString("2"));
        originalDoc.put("third", new BsonString("3"));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Update the middle field
        Map<String, BsonValue> setOps = Map.of("second", new BsonString("updated"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify field order and values
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals(3, updatedDoc.size());
        assertTrue(updatedDoc.containsKey("first"));
        assertTrue(updatedDoc.containsKey("second"));
        assertTrue(updatedDoc.containsKey("third"));
        assertEquals("1", updatedDoc.getString("first").getValue());
        assertEquals("updated", updatedDoc.getString("second").getValue());
        assertEquals("3", updatedDoc.getString("third").getValue());
    }

    @Test
    void shouldTrackNewValues() {
        // Create a document with multiple field types
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("stringField", new BsonString("original"));
        originalDoc.put("intField", new BsonInt32(100));
        originalDoc.put("boolField", new BsonBoolean(true));
        originalDoc.put("doubleField", new BsonDouble(3.14));
        originalDoc.put("nullField", new BsonNull());
        originalDoc.put("unchangedField", new BsonString("unchanged"));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Update some fields and add a new one
        Map<String, BsonValue> setOps = Map.of(
                "stringField", new BsonString("updated"),     // Update existing
                "intField", new BsonInt32(999),              // Update existing
                "boolField", new BsonBoolean(false),           // Update existing
                "doubleField", new BsonDouble(2.71),          // Update existing
                "nullField", new BsonString("now a string"),  // Update existing
                "newField", new BsonString("brand new")       // Add new field
        );
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify updated document
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("updated", updatedDoc.getString("stringField").getValue());
        assertEquals(999, updatedDoc.getInt32("intField").getValue());
        assertFalse(updatedDoc.getBoolean("boolField").getValue());
        assertEquals(2.71, updatedDoc.getDouble("doubleField").getValue(), 0.001);
        assertEquals("now a string", updatedDoc.getString("nullField").getValue());
        assertEquals("brand new", updatedDoc.getString("newField").getValue());
        assertEquals("unchanged", updatedDoc.getString("unchangedField").getValue());

        // Verify new values - should contain all fields that were set
        Map<String, BsonValue> newValues = result.newValues();
        assertEquals(6, newValues.size(), "Should contain exactly 6 new values for all fields in setOps");

        // Check each new value
        assertEquals("updated", newValues.get("stringField").asString().getValue());
        assertEquals(999, newValues.get("intField").asInt32().getValue());
        assertEquals(2.71, newValues.get("doubleField").asDouble().getValue(), 0.001);
        assertFalse(newValues.get("boolField").asBoolean().getValue());
        assertEquals("now a string", newValues.get("nullField").asString().getValue());
        assertEquals("brand new", newValues.get("newField").asString().getValue());

        // Unchanged field should not be in new values
        assertFalse(newValues.containsKey("unchangedField"),
                "unchangedField should not be in newValues - it was not included in setOps");

        // Verify all keys in newValues correspond to fields that were actually set
        for (String fieldName : newValues.keySet()) {
            assertTrue(setOps.containsKey(fieldName),
                    "Field " + fieldName + " in newValues must correspond to a field in setOps");
        }
    }

    @Test
    void shouldTrackNewValuesWithAllBsonTypes() {
        // Create a document with all supported BSON types using fixed values for deterministic tests
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("timestamp", new BsonTimestamp(1000, 1));
        originalDoc.put("dateTime", new BsonDateTime(1609459200000L)); // Fixed timestamp: 2021-01-01T00:00:00.000Z
        originalDoc.put("binary", new BsonBinary(new byte[]{1, 2, 3}));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Update all fields
        Map<String, BsonValue> setOps = Map.of(
                "timestamp", new BsonString("converted to string"),
                "dateTime", new BsonDateTime(12345L),
                "binary", new BsonBoolean(false)
        );
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify new values contain the updated values
        Map<String, BsonValue> newValues = result.newValues();
        assertEquals(3, newValues.size());

        assertEquals(BsonType.STRING, newValues.get("timestamp").getBsonType());
        assertEquals(BsonType.DATE_TIME, newValues.get("dateTime").getBsonType());
        assertEquals(BsonType.BOOLEAN, newValues.get("binary").getBsonType());

        // Verify the actual new values
        assertEquals("converted to string", newValues.get("timestamp").asString().getValue());
        assertEquals(12345L, newValues.get("dateTime").asDateTime().getValue());
        assertFalse(newValues.get("binary").asBoolean().getValue());
    }

    @Test
    void shouldUpdateAndAddFieldsInSameOperation() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("existingField", new BsonString("original"));
        originalDoc.put("unchangedField", new BsonInt32(100));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Update existing field and add new field
        Map<String, BsonValue> setOps = Map.of(
                "existingField", new BsonString("updated"),  // Update existing
                "newField", new BsonString("brand new")     // Add new field
        );
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        // Verify newValues contains all fields that were set (both new and updated)
        Map<String, BsonValue> newValues = result.newValues();
        assertEquals(2, newValues.size());

        // Check new values match what was set
        assertEquals("updated", newValues.get("existingField").asString().getValue());
        assertEquals("brand new", newValues.get("newField").asString().getValue());

        // Verify unchanged field is NOT in newValues
        assertFalse(newValues.containsKey("unchangedField"));

        // Verify all keys in newValues correspond to fields in setOps
        for (String fieldName : newValues.keySet()) {
            assertTrue(setOps.containsKey(fieldName),
                    "Field " + fieldName + " in newValues must correspond to a field in setOps");
        }
    }

    @Test
    void shouldUnsetSingleField() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        originalDoc.put("city", new BsonString("New York"));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Unset the name field
        Set<String> unsetOps = Set.of("name");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify field removed
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertFalse(updatedDoc.containsKey("name"));
        assertEquals(25, updatedDoc.getInt32("age").getValue());
        assertEquals("New York", updatedDoc.getString("city").getValue());

        // Verify droppedSelectors contains removed field
        assertEquals(Set.of("name"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetMultipleFields() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        originalDoc.put("city", new BsonString("New York"));
        originalDoc.put("email", new BsonString("john@example.com"));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Unset multiple fields
        Set<String> unsetOps = Set.of("name", "email");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify fields removed
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertFalse(updatedDoc.containsKey("name"));
        assertFalse(updatedDoc.containsKey("email"));
        assertEquals(25, updatedDoc.getInt32("age").getValue());
        assertEquals("New York", updatedDoc.getString("city").getValue());

        // Verify droppedSelectors contains all removed fields
        assertEquals(Set.of("name", "email"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetNonExistentField() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Try to unset a non-existent field
        Set<String> unsetOps = Set.of("nonExistent");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify document unchanged
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("John", updatedDoc.getString("name").getValue());
        assertEquals(25, updatedDoc.getInt32("age").getValue());

        // Verify droppedSelectors is empty
        assertTrue(result.droppedSelectors().isEmpty());
    }

    @Test
    void shouldSetAndUnsetInSameOperation() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        originalDoc.put("city", new BsonString("New York"));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Set new field and unset existing field
        Map<String, BsonValue> setOps = Map.of("email", new BsonString("john@example.com"));
        Set<String> unsetOps = Set.of("city");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, unsetOps);

        // Verify operations applied
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertEquals("John", updatedDoc.getString("name").getValue());
        assertEquals(25, updatedDoc.getInt32("age").getValue());
        assertEquals("john@example.com", updatedDoc.getString("email").getValue());
        assertFalse(updatedDoc.containsKey("city"));

        // Verify newValues and droppedSelectors
        assertEquals(Map.of("email", new BsonString("john@example.com")), result.newValues());
        assertEquals(Set.of("city"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetAllFields() {
        // Create the original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Unset all fields
        Set<String> unsetOps = Set.of("name", "age");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify the document is empty
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertTrue(updatedDoc.isEmpty());

        // Verify all fields in droppedSelectors
        assertEquals(Set.of("name", "age"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetFieldWithinArrayOfDocuments() {
        // Behavior: Unset using the $[] positional operator (e.g., "tags.$[].name") should remove
        // the "name" field from each object in the tags array.
        String json = "{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"scala\"}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Set<String> unsetOps = Set.of("tags.$[].name");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify the "name" field is removed from each element in the tags array
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Alice", updatedDoc.getString("name").getValue());

        @SuppressWarnings("unchecked")
        List<BsonDocument> tags = (List<BsonDocument>) updatedDoc.get("tags");
        assertNotNull(tags);
        assertEquals(3, tags.size());

        // Each tag object should no longer have the "name" field
        for (BsonDocument tag : tags) {
            assertFalse(tag.containsKey("name"), "Each tag should not have 'name' field after unset");
        }

        // Verify droppedSelectors contains the normalized path (without $[])
        assertEquals(Set.of("tags.name"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetNestedFieldInDocument() {
        // Create the document with the nested object: { user: { profile: { email: "test@example.com", phone: "123" } } }
        String json = "{\"user\": {\"profile\": {\"email\": \"test@example.com\", \"phone\": \"123\"}}}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Unset "user.profile.email" - should remove the email field from the nested profile
        Set<String> unsetOps = Set.of("user.profile.email");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify the email field is removed
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonDocument user = (BsonDocument) updatedDoc.get("user");
        assertNotNull(user);
        BsonDocument profile = (BsonDocument) user.get("profile");
        assertNotNull(profile);
        assertFalse(profile.containsKey("email"), "email should be removed");
        assertEquals("123", profile.getString("phone").getValue(), "phone should remain");

        // Verify droppedSelectors
        assertEquals(Set.of("user.profile.email"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetFieldWithinArrayPreservingOtherFields() {
        // Behavior: Unset using the $[] positional operator (e.g., "items.$[].price") should remove
        // price but keep the name in each array element.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 10}, {\"name\": \"item2\", \"price\": 20}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Set<String> unsetOps = Set.of("items.$[].price");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify price is removed but the name remains
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(2, items.size());

        assertEquals("item1", items.get(0).getString("name").getValue());
        assertFalse(items.get(0).containsKey("price"), "price should be removed from first item");

        assertEquals("item2", items.get(1).getString("name").getValue());
        assertFalse(items.get(1).containsKey("price"), "price should be removed from second item");

        assertEquals(Set.of("items.price"), result.droppedSelectors());
    }

    @Test
    void shouldNotUnsetWhenNonNumericKeyOnArray() {
        // Behavior: Unset with a non-numeric, non-$[] key on an array (e.g., "tags.name") should be
        // a no-op since the path doesn't exist as a literal field. Use "tags.$[].name" to target
        // all array elements.
        String json = "{\"name\": \"Alice\", \"tags\": [{\"type\": \"java\"}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Set<String> unsetOps = Set.of("tags.name");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // BsonDocument should be unchanged
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Alice", updatedDoc.getString("name").getValue());

        @SuppressWarnings("unchecked")
        List<BsonDocument> tags = (List<BsonDocument>) updatedDoc.get("tags");
        assertEquals("java", tags.getFirst().getString("type").getValue());

        // droppedSelectors should be empty since nothing was actually removed
        assertTrue(result.droppedSelectors().isEmpty());
    }

    @Test
    void shouldUnsetFieldFromSomeArrayElementsOnly() {
        // Behavior: Unset using the $[] positional operator should remove the field from elements
        // that have it, while gracefully handling elements that don't have the field.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 10}, {\"name\": \"item2\"}, {\"name\": \"item3\", \"price\": 30}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Set<String> unsetOps = Set.of("items.$[].price");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify price is removed where it existed
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");

        assertEquals("item1", items.get(0).getString("name").getValue());
        assertFalse(items.get(0).containsKey("price"));

        assertEquals("item2", items.get(1).getString("name").getValue());
        assertFalse(items.get(1).containsKey("price"));

        assertEquals("item3", items.get(2).getString("name").getValue());
        assertFalse(items.get(2).containsKey("price"));

        // droppedSelectors should contain the path since some fields were removed
        assertEquals(Set.of("items.price"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetFieldWithinNestedBsonDocument() {
        // Create a BsonDocument containing a nested BsonDocument
        BsonDocument doc = new BsonDocument();
        doc.put("name", new BsonString("Alice"));
        BsonDocument nestedBsonDoc = new BsonDocument();
        nestedBsonDoc.put("email", new BsonString("alice@example.com"));
        nestedBsonDoc.put("phone", new BsonString("123-456"));
        doc.put("contact", nestedBsonDoc);

        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Unset "contact.email" - should remove email from the nested BsonDocument
        Set<String> unsetOps = Set.of("contact.email");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify email is removed but phone remains
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Alice", updatedDoc.getString("name").getValue());

        Object contact = updatedDoc.get("contact");
        assertNotNull(contact);

        if (contact instanceof BsonDocument bsonContact) {
            assertFalse(bsonContact.containsKey("email"), "email should be removed from BsonDocument");
            assertEquals("123-456", bsonContact.getString("phone").getValue(), "phone should remain");
        } else {
            fail("contact must be a BsonDocument");
        }

        assertEquals(Set.of("contact.email"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetFieldWithinBsonArray() {
        // Behavior: Unset using the $[] positional operator (e.g., "skills.$[].level") should remove
        // level from each BsonDocument in the BsonArray.
        BsonDocument doc = new BsonDocument();
        doc.put("name", new BsonString("Bob"));

        BsonArray bsonArray = new BsonArray();
        BsonDocument item1 = new BsonDocument();
        item1.put("tag", new BsonString("java"));
        item1.put("level", new BsonInt32(5));
        bsonArray.add(item1);

        BsonDocument item2 = new BsonDocument();
        item2.put("tag", new BsonString("kotlin"));
        item2.put("level", new BsonInt32(3));
        bsonArray.add(item2);

        doc.put("skills", bsonArray);

        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Set<String> unsetOps = Set.of("skills.$[].level");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify level is removed but tag remains
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Bob", updatedDoc.getString("name").getValue());

        Object skills = updatedDoc.get("skills");
        assertNotNull(skills);

        if (skills instanceof BsonArray bsonSkills) {
            for (BsonValue skill : bsonSkills) {
                BsonDocument skillDoc = skill.asDocument();
                assertTrue(skillDoc.containsKey("tag"), "tag should remain");
                assertFalse(skillDoc.containsKey("level"), "level should be removed");
            }
        } else if (skills instanceof List<?> listSkills) {
            for (Object skill : listSkills) {
                if (skill instanceof BsonDocument skillDoc) {
                    assertTrue(skillDoc.containsKey("tag"), "tag should remain");
                    assertFalse(skillDoc.containsKey("level"), "level should be removed");
                }
            }
        }

        assertEquals(Set.of("skills.level"), result.droppedSelectors());
    }

    @Test
    void shouldUnsetAllArrayElementsUsingPositionalAll() {
        // Behavior: Unset using "grades.$[]" should remove all elements from the grades array,
        // resulting in an empty array. The field still exists, so it goes to newValues (not droppedSelectors).
        String json = "{\"name\": \"Alice\", \"grades\": [85, 90, 78, 92]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Set<String> unsetOps = Set.of("grades.$[]");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Alice", updatedDoc.getString("name").getValue());

        BsonArray grades = updatedDoc.getArray("grades");
        assertNotNull(grades);
        assertTrue(grades.isEmpty(), "All elements should be removed from grades array");

        // The field still exists as an empty array, so it's in newValues, not droppedSelectors
        assertTrue(result.droppedSelectors().isEmpty(), "droppedSelectors should be empty");
        assertTrue(result.newValues().containsKey("grades"), "newValues should contain grades");
        assertTrue(result.newValues().get("grades").isArray(), "grades should be an array");
        assertTrue(result.newValues().get("grades").asArray().isEmpty(), "grades array should be empty");
    }

    @Test
    void shouldNotUnsetWhenBsonDocumentDoesNotContainKey() {
        // Create a BsonDocument containing a nested BsonDocument without the target field
        BsonDocument doc = new BsonDocument();
        doc.put("name", new BsonString("Charlie"));
        BsonDocument nestedBsonDoc = new BsonDocument();
        nestedBsonDoc.put("phone", new BsonString("999-999"));
        doc.put("contact", nestedBsonDoc);

        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Try to unset "contact.email" which doesn't exist
        Set<String> unsetOps = Set.of("contact.email");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify document unchanged
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Charlie", updatedDoc.getString("name").getValue());

        // droppedSelectors should be empty since nothing was removed
        assertTrue(result.droppedSelectors().isEmpty());
    }

    @Test
    void shouldUnsetDeeplyNestedFieldThroughBsonDocuments() {
        // Create a deeply nested structure with BsonDocuments
        BsonDocument doc = new BsonDocument();
        doc.put("id", new BsonString("doc1"));

        BsonDocument level1 = new BsonDocument();
        BsonDocument level2 = new BsonDocument();
        level2.put("target", new BsonString("value_to_remove"));
        level2.put("keep", new BsonString("should_stay"));
        level1.put("nested", level2);
        doc.put("outer", level1);

        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Unset "outer.nested.target"
        Set<String> unsetOps = Set.of("outer.nested.target");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify target is removed but keep remains
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("doc1", updatedDoc.getString("id").getValue());

        Object outer = updatedDoc.get("outer");
        assertNotNull(outer);

        Object nested = null;
        if (outer instanceof BsonDocument bsonOuter) {
            nested = bsonOuter.get("nested");
        } else {
            fail("nested must be BsonDocument");
        }
        assertNotNull(nested);

        if (nested instanceof BsonDocument bsonNested) {
            assertFalse(bsonNested.containsKey("target"), "target should be removed");
            assertEquals("should_stay", bsonNested.getString("keep").getValue(), "keep should remain");
        } else {
            fail("should_stay must be BsonDocument");
        }

        assertEquals(Set.of("outer.nested.target"), result.droppedSelectors());
    }

    @Test
    void shouldSetNestedFieldWithDotNotation() {
        // Behavior: Setting a nested field using dot notation (e.g., "address.city") should update
        // the nested field rather than creating a literal field named "address.city".
        String json = "{\"name\": \"Alice\", \"address\": {\"city\": \"Boston\", \"zip\": \"02101\"}}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("address.city", new BsonString("NYC"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Alice", updatedDoc.getString("name").getValue());

        BsonDocument address = (BsonDocument) updatedDoc.get("address");
        assertNotNull(address);
        assertEquals("NYC", address.getString("city").getValue());
        assertEquals("02101", address.getString("zip").getValue());

        // Verify the literal field "address.city" was NOT created at root level
        assertFalse(updatedDoc.containsKey("address.city"));
    }

    @Test
    void shouldSetArrayElementByIndex() {
        // Behavior: Setting an array element by index (e.g., "items.0.price") should update
        // the specific array element rather than creating a literal field.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.0.price", new BsonInt32(150));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(2, items.size());

        // First item should have updated price
        assertEquals("item1", items.get(0).getString("name").getValue());
        assertEquals(150, items.get(0).getInt32("price").getValue());

        // The second item should be unchanged
        assertEquals("item2", items.get(1).getString("name").getValue());
        assertEquals(200, items.get(1).getInt32("price").getValue());

        // Verify the literal field was NOT created at the root level
        assertFalse(updatedDoc.containsKey("items.0.price"));
    }

    @Test
    void shouldSetNestedFieldInArrayElement() {
        // Behavior: Setting a deeply nested field in an array element (e.g., "items.0.details.color")
        // should navigate through the array and nested documents correctly.
        String json = "{\"items\": [{\"name\": \"item1\", \"details\": {\"color\": \"blue\", \"size\": \"M\"}}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.0.details.color", new BsonString("red"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(1, items.size());

        BsonDocument item = items.getFirst();
        assertEquals("item1", item.getString("name").getValue());

        BsonDocument details = (BsonDocument) item.get("details");
        assertNotNull(details);
        assertEquals("red", details.getString("color").getValue());
        assertEquals("M", details.getString("size").getValue());
    }

    @Test
    void shouldCreateIntermediateDocumentsForNewPath() {
        // Behavior: Setting a nested path where the parent doesn't exist should create
        // intermediate documents as needed.
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("Alice"));
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(originalDoc));

        Map<String, BsonValue> setOps = Map.of("address.city", new BsonString("NYC"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertEquals("Alice", updatedDoc.getString("name").getValue());

        BsonDocument address = (BsonDocument) updatedDoc.get("address");
        assertNotNull(address);
        assertEquals("NYC", address.getString("city").getValue());
    }

    @Test
    void shouldSetSecondArrayElement() {
        // Behavior: Setting a field in an array element other than the first one (e.g., "items.1.price")
        // should correctly target that specific element.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}, {\"name\": \"item3\", \"price\": 300}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.1.price", new BsonInt32(250));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(3, items.size());

        // First and third items unchanged
        assertEquals(100, items.get(0).getInt32("price").getValue());
        assertEquals(300, items.get(2).getInt32("price").getValue());

        // Second item updated
        assertEquals(250, items.get(1).getInt32("price").getValue());
    }

    @Test
    void shouldReplaceEntireArrayElement() {
        // Behavior: Setting an array element directly (e.g., "items.0") should replace the entire element.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        BsonDocument newItem = new BsonDocument();
        newItem.put("name", new BsonString("replaced"));
        newItem.put("price", new BsonInt32(999));

        Map<String, BsonValue> setOps = Map.of("items.0", newItem);
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(2, items.size());

        // First item replaced
        assertEquals("replaced", items.get(0).getString("name").getValue());
        assertEquals(999, items.get(0).getInt32("price").getValue());

        // Second item unchanged
        assertEquals("item2", items.get(1).getString("name").getValue());
        assertEquals(200, items.get(1).getInt32("price").getValue());
    }

    @Test
    void shouldNotModifyWhenArrayIndexOutOfBounds() {
        // Behavior: Setting a field at an array index that doesn't exist should not modify the document.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.5.price", new BsonInt32(500));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(1, items.size());

        // Original item unchanged
        assertEquals("item1", items.getFirst().getString("name").getValue());
        assertEquals(100, items.getFirst().getInt32("price").getValue());
    }

    @Test
    void shouldSetFieldInAllArrayElements() {
        // Behavior: Setting a field using the $[] positional operator (e.g., "items.$[].price")
        // should update that field in all document elements of the array.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}, {\"name\": \"item3\", \"price\": 300}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.$[].price", new BsonInt32(999));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(3, items.size());

        // All items should have price updated to 999
        for (int i = 0; i < items.size(); i++) {
            assertEquals("item" + (i + 1), items.get(i).getString("name").getValue());
            assertEquals(999, items.get(i).getInt32("price").getValue());
        }
    }

    @Test
    void shouldReplaceAllArrayElementsWithValue() {
        // Behavior: Using $[] as the last segment (e.g., "myArray.$[]") should replace all array
        // elements with the specified value.
        String json = "{\"scores\": [10, 20, 30, 40]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("scores.$[]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        BsonArray scores = updatedDoc.getArray("scores");
        assertNotNull(scores);
        assertEquals(4, scores.size());

        // All elements should be replaced with 100
        for (BsonValue score : scores) {
            assertEquals(100, score.asInt32().getValue());
        }
    }

    @Test
    void shouldSetNestedFieldInAllArrayElements() {
        // Behavior: Setting a nested field using the $[] positional operator (e.g., "items.$[].details.color")
        // should update that field in all document elements of the array.
        String json = "{\"items\": [{\"name\": \"a\", \"details\": {\"color\": \"red\"}}, {\"name\": \"b\", \"details\": {\"color\": \"blue\"}}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.$[].details.color", new BsonString("green"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(2, items.size());

        // All items should have details.color updated to "green"
        for (BsonDocument item : items) {
            BsonDocument details = (BsonDocument) item.get("details");
            assertEquals("green", details.getString("color").getValue());
        }
    }

    @Test
    void shouldAddNewFieldToAllArrayElements() {
        // Behavior: Setting a new field using the $[] positional operator should add that field
        // to all document elements in the array.
        String json = "{\"items\": [{\"name\": \"item1\"}, {\"name\": \"item2\"}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.$[].status", new BsonString("active"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(2, items.size());

        // All items should have the new "status" field
        for (BsonDocument item : items) {
            assertEquals("active", item.getString("status").getValue());
        }
    }

    @Test
    void shouldNotModifyWhenNonNumericKeyOnArray() {
        // Behavior: Setting a field with a non-numeric, non-$[] key on an array (e.g., "items.price")
        // should be a no-op since the path doesn't exist as a literal field.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.price", new BsonInt32(999));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());

        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertNotNull(items);
        assertEquals(2, items.size());

        // Prices should remain unchanged since "items.price" is a no-op
        assertEquals(100, items.get(0).getInt32("price").getValue());
        assertEquals(200, items.get(1).getInt32("price").getValue());
    }

    @Test
    void shouldThrowErrorForMalformedPositionalOperator() {
        // Behavior: Malformed positional operators like "$[ ]" (with spaces) or "$[" (incomplete)
        // should throw an IllegalArgumentException.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Test "$[ ]" with space
        Map<String, BsonValue> setOps1 = Map.of("items.$[ ].price", new BsonInt32(999));
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, () ->
                BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps1, Set.of()));
        assertTrue(ex1.getMessage().contains("Invalid positional operator"));

        // Test "$[" incomplete
        originalBuffer.rewind();
        Map<String, BsonValue> setOps2 = Map.of("items.$[.price", new BsonInt32(999));
        IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class, () ->
                BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps2, Set.of()));
        assertTrue(ex2.getMessage().contains("Invalid positional operator"));
    }

    @Test
    void shouldThrowErrorForArrayFilterIdentifier() {
        // Behavior: Array filter identifiers like "$[foo]" should throw an error indicating
        // that no array filter was found.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.$[foo].price", new BsonInt32(999));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of()));
        assertTrue(ex.getMessage().contains("No array filter found for identifier 'foo'"));
    }

    @Test
    void shouldThrowErrorForUnsetWithMalformedPositionalOperator() {
        // Behavior: Unset operations should also throw errors for malformed positional operators.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}, {\"name\": \"item2\", \"price\": 200}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Set<String> unsetOps = Set.of("items.$[foo].price");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps));
        assertTrue(ex.getMessage().contains("No array filter found for identifier 'foo'"));
    }

    // ==================== Filtered Positional Operator Tests ====================

    @Test
    void shouldSetFieldWithFilteredPositionalOperator() {
        // Behavior: $[identifier] with a filter should only update array elements that match the filter.
        String json = "{\"scores\": [5, 10, 15, 20, 25]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Filter: elements >= 15
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.GTE, 15)
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[elem]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray scores = updatedDoc.getArray("scores");
        assertNotNull(scores);
        assertEquals(5, scores.size());

        // Elements < 15 should remain unchanged
        assertEquals(5, scores.get(0).asInt32().getValue());
        assertEquals(10, scores.get(1).asInt32().getValue());
        // Elements >= 15 should be updated to 100
        assertEquals(100, scores.get(2).asInt32().getValue());
        assertEquals(100, scores.get(3).asInt32().getValue());
        assertEquals(100, scores.get(4).asInt32().getValue());
    }

    @Test
    void shouldSetNestedFieldWithFilteredPositionalOperatorOnScalarArray() {
        // Behavior: $[identifier] with nested path updates fields within documents in an array of documents,
        // but the filter matches the element itself. For document elements, use $[] instead.
        // This test verifies that we can combine $[] with $[identifier] for nested arrays.
        String json = "{\"data\": [{\"values\": [5, 10, 15]}, {\"values\": [20, 25, 30]}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Filter: values >= 15
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "big", new ArrayFilter("big", Operator.GTE, 15)
        );

        // Update all data elements, but only values >= 15 in each
        Map<String, BsonValue> setOps = Map.of("data.$[].values.$[big]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> data = (List<BsonDocument>) updatedDoc.get("data");
        assertNotNull(data);
        assertEquals(2, data.size());

        // First data element: values [5, 10, 15] -> [5, 10, 100]
        BsonArray values1 = data.get(0).getArray("values");
        assertEquals(5, values1.get(0).asInt32().getValue());
        assertEquals(10, values1.get(1).asInt32().getValue());
        assertEquals(100, values1.get(2).asInt32().getValue());

        // Second data element: values [20, 25, 30] -> [100, 100, 100]
        BsonArray values2 = data.get(1).getArray("values");
        assertEquals(100, values2.get(0).asInt32().getValue());
        assertEquals(100, values2.get(1).asInt32().getValue());
        assertEquals(100, values2.get(2).asInt32().getValue());
    }

    @Test
    void shouldCombinePositionalAllWithFilteredPositional() {
        // Behavior: grades.$[].questions.$[score] should apply $[] to all grades, then $[score] to matching questions.
        String json = "{\"grades\": [{\"questions\": [5, 8, 10]}, {\"questions\": [3, 9, 12]}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Filter: questions >= 8
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "score", new ArrayFilter("score", Operator.GTE, 8)
        );

        Map<String, BsonValue> setOps = Map.of("grades.$[].questions.$[score]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> grades = (List<BsonDocument>) updatedDoc.get("grades");
        assertNotNull(grades);
        assertEquals(2, grades.size());

        // First grade: questions [5, 8, 10] -> [5, 100, 100]
        BsonArray questions1 = grades.get(0).getArray("questions");
        assertEquals(5, questions1.get(0).asInt32().getValue());
        assertEquals(100, questions1.get(1).asInt32().getValue());
        assertEquals(100, questions1.get(2).asInt32().getValue());

        // Second grade: questions [3, 9, 12] -> [3, 100, 100]
        BsonArray questions2 = grades.get(1).getArray("questions");
        assertEquals(3, questions2.get(0).asInt32().getValue());
        assertEquals(100, questions2.get(1).asInt32().getValue());
        assertEquals(100, questions2.get(2).asInt32().getValue());
    }

    @Test
    void shouldUnsetWithFilteredPositionalOperator() {
        // Behavior: Unset using $[identifier] should remove elements only from matching positions.
        // For scalar arrays, the filter matches element values directly.
        String json = "{\"scores\": [5, 10, 15, 20, 25]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Filter: elements >= 15
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "high", new ArrayFilter("high", Operator.GTE, 15)
        );

        // Unset (remove) matching elements
        Set<String> unsetOps = Set.of("scores.$[high]");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, Map.of(), unsetOps, arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray scores = updatedDoc.getArray("scores");
        assertNotNull(scores);
        // Elements >= 15 (15, 20, 25) should be removed
        assertEquals(2, scores.size());
        assertEquals(5, scores.get(0).asInt32().getValue());
        assertEquals(10, scores.get(1).asInt32().getValue());
    }

    @Test
    void shouldHandleNoMatchingElements() {
        // Behavior: If no elements match the filter, the document should remain unchanged.
        String json = "{\"scores\": [1, 2, 3, 4, 5]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Filter: elements > 100 (none match)
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "big", new ArrayFilter("big", Operator.GT, 100)
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[big]", new BsonInt32(999));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray scores = updatedDoc.getArray("scores");
        assertNotNull(scores);
        assertEquals(5, scores.size());

        // All elements should remain unchanged
        assertEquals(1, scores.get(0).asInt32().getValue());
        assertEquals(2, scores.get(1).asInt32().getValue());
        assertEquals(3, scores.get(2).asInt32().getValue());
        assertEquals(4, scores.get(3).asInt32().getValue());
        assertEquals(5, scores.get(4).asInt32().getValue());
    }

    @Test
    void shouldNotCreateLiteralStructureWhenFilteredPositionalTargetsMissingArray() {
        // Behavior: $set with $[identifier] on a document that lacks the target array field
        // must be a no-op. No literal "$[identifier]" key or intermediate structure is created.
        String json = "{\"name\": \"Dave\", \"age\": 28}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.GTE, 60)
        );

        Map<String, BsonValue> setOps = Map.of("grades.$[elem].passed", BsonBoolean.TRUE);
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertFalse(updatedDoc.containsKey("grades"), "No 'grades' field should be created");
        BsonDocument expectedDoc = BSONUtil.fromBson(ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json)));
        assertEquals(expectedDoc, updatedDoc, "Document should remain unchanged");
    }

    @Test
    void shouldNotCreateLiteralStructureWhenPositionalAllTargetsMissingArray() {
        // Behavior: $set with $[] on a document that lacks the target array field must be a no-op.
        String json = "{\"name\": \"Eve\"}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("grades.$[]", new BsonInt32(10));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), Map.of()
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertFalse(updatedDoc.containsKey("grades"), "No 'grades' field should be created");
        BsonDocument expectedDoc = BSONUtil.fromBson(ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json)));
        assertEquals(expectedDoc, updatedDoc, "Document should remain unchanged");
    }

    @Test
    void shouldNoOpWhenPositionalTargetsMissingArray() {
        // Behavior: $set with the $ positional operator on a document that lacks the target
        // array field must be a no-op.
        String json = "{\"name\": \"Frank\"}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("scores.$.value", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), Map.of()
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        assertFalse(updatedDoc.containsKey("scores"), "No 'scores' field should be created");
        BsonDocument expectedDoc = BSONUtil.fromBson(ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json)));
        assertEquals(expectedDoc, updatedDoc, "Document should remain unchanged");
    }

    @Test
    void shouldNoOpWhenFilteredPositionalTargetsNonArrayField() {
        // Behavior: $set with $[identifier] where the target field exists but is not an array
        // must be a no-op. The existing field is left untouched and no literal key is created.
        String json = "{\"grades\": {\"math\": 90}}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.GTE, 60)
        );

        Map<String, BsonValue> setOps = Map.of("grades.$[elem].passed", BsonBoolean.TRUE);
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonDocument grades = updatedDoc.getDocument("grades");
        assertFalse(grades.containsKey("$[elem]"), "No literal '$[elem]' key should be created");
        BsonDocument expectedDoc = BSONUtil.fromBson(ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json)));
        assertEquals(expectedDoc, updatedDoc, "Document should remain unchanged");
    }

    @Test
    void shouldThrowWhenArrayFilterMissingForFilteredPositionalOnMissingArray() {
        // Behavior: $[identifier] with no matching array filter must throw the same error
        // whether the target array field exists or not.
        String json = "{\"name\": \"Grace\"}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.$[foo].price", new BsonInt32(999));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of()));
        assertTrue(ex.getMessage().contains("No array filter found for identifier 'foo'"));
    }

    @Test
    void shouldHandleEmptyArrayWithFilteredPositional() {
        // Behavior: Applying $[identifier] to an empty array should be a no-op.
        String json = "{\"scores\": []}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.GTE, 0)
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[elem]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray scores = updatedDoc.getArray("scores");
        assertNotNull(scores);
        assertTrue(scores.isEmpty());
    }

    @Test
    void shouldSetWithEqualityFilter() {
        // Behavior: $[identifier] with $eq filter should update elements equal to the value.
        String json = "{\"tags\": [\"java\", \"kotlin\", \"scala\", \"java\"]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Filter: elements == "java"
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "lang", new ArrayFilter("lang", Operator.EQ, "java")
        );

        Map<String, BsonValue> setOps = Map.of("tags.$[lang]", new BsonString("JAVA"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray tags = updatedDoc.getArray("tags");
        assertNotNull(tags);
        assertEquals(4, tags.size());

        assertEquals("JAVA", tags.get(0).asString().getValue());
        assertEquals("kotlin", tags.get(1).asString().getValue());
        assertEquals("scala", tags.get(2).asString().getValue());
        assertEquals("JAVA", tags.get(3).asString().getValue());
    }

    @Test
    void shouldSetWithNotEqualFilter() {
        // Behavior: $[identifier] with $ne filter should update elements not equal to the value.
        String json = "{\"statuses\": [\"active\", \"inactive\", \"active\", \"pending\"]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        // Filter: elements != "active"
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "status", new ArrayFilter("status", Operator.NE, "active")
        );

        Map<String, BsonValue> setOps = Map.of("statuses.$[status]", new BsonString("archived"));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray statuses = updatedDoc.getArray("statuses");
        assertNotNull(statuses);
        assertEquals(4, statuses.size());

        assertEquals("active", statuses.get(0).asString().getValue());
        assertEquals("archived", statuses.get(1).asString().getValue());
        assertEquals("active", statuses.get(2).asString().getValue());
        assertEquals("archived", statuses.get(3).asString().getValue());
    }

    @Test
    void shouldWorkWithFilteredPositionalAndWithoutArrayFiltersForBackwardCompatibility() {
        // Behavior: The old 3-argument version should still work without arrayFilters.
        String json = "{\"items\": [{\"name\": \"item1\", \"price\": 100}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("items.$[].price", new BsonInt32(200));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of()
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertEquals(200, items.get(0).getInt32("price").getValue());
    }

    @Test
    void shouldNormalizeSelectorWithFilteredPositionalOperator() {
        // Behavior: Selectors with $[identifier] should be normalized to remove the positional operator.
        String json = "{\"items\": [{\"price\": 100}, {\"price\": 200}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.GTE, 150)
        );

        Map<String, BsonValue> setOps = Map.of("items.$[elem].price", new BsonInt32(500));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        // newValues should contain the normalized path without $[elem]
        assertTrue(result.newValues().containsKey("items.price"));
    }

    // ==================== Extended Type Support Tests ====================

    @Test
    void shouldFilterWithDateTimeOperand() {
        // Behavior: Filtering with BsonDateTime operand should match datetime values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray timestamps = new BsonArray();
        timestamps.add(new BsonDateTime(1609459200000L)); // 2021-01-01
        timestamps.add(new BsonDateTime(1612137600000L)); // 2021-02-01
        timestamps.add(new BsonDateTime(1614556800000L)); // 2021-03-01
        doc.put("timestamps", timestamps);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter: timestamps >= 2021-02-01
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "ts", new ArrayFilter("ts", Operator.GTE, new BsonDateTime(1612137600000L))
        );

        Map<String, BsonValue> setOps = Map.of("timestamps.$[ts]", new BsonDateTime(0L));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedTimestamps = updatedDoc.getArray("timestamps");
        assertEquals(3, updatedTimestamps.size());
        // First timestamp unchanged (before filter)
        assertEquals(1609459200000L, updatedTimestamps.get(0).asDateTime().getValue());
        // Second and third timestamps updated (>= filter)
        assertEquals(0L, updatedTimestamps.get(1).asDateTime().getValue());
        assertEquals(0L, updatedTimestamps.get(2).asDateTime().getValue());
    }

    @Test
    void shouldFilterWithTimestampOperand() {
        // Behavior: Filtering with BsonTimestamp operand should match timestamp values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray timestamps = new BsonArray();
        timestamps.add(new BsonTimestamp(100, 1));
        timestamps.add(new BsonTimestamp(200, 1));
        timestamps.add(new BsonTimestamp(300, 1));
        doc.put("versions", timestamps);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter: timestamps < 250
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "ver", new ArrayFilter("ver", Operator.LT, new BsonTimestamp(250, 0))
        );

        Map<String, BsonValue> setOps = Map.of("versions.$[ver]", new BsonTimestamp(0, 0));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedVersions = updatedDoc.getArray("versions");
        assertEquals(3, updatedVersions.size());
        // First two timestamps updated (< filter)
        assertEquals(0L, updatedVersions.get(0).asTimestamp().getValue());
        assertEquals(0L, updatedVersions.get(1).asTimestamp().getValue());
        // Third timestamp unchanged (>= filter)
        assertEquals(new BsonTimestamp(300, 1).getValue(), updatedVersions.get(2).asTimestamp().getValue());
    }

    @Test
    void shouldFilterWithBinaryOperand() {
        // Behavior: Filtering with BsonBinary operand should match binary values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray binaries = new BsonArray();
        binaries.add(new BsonBinary(new byte[]{1, 2, 3}));
        binaries.add(new BsonBinary(new byte[]{4, 5, 6}));
        binaries.add(new BsonBinary(new byte[]{1, 2, 3})); // duplicate
        doc.put("data", binaries);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter: binary == [1, 2, 3]
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "bin", new ArrayFilter("bin", Operator.EQ, new BsonBinary(new byte[]{1, 2, 3}))
        );

        Map<String, BsonValue> setOps = Map.of("data.$[bin]", new BsonBinary(new byte[]{0, 0, 0}));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedData = updatedDoc.getArray("data");
        assertEquals(3, updatedData.size());
        // First and third binaries updated (match filter)
        assertArrayEquals(new byte[]{0, 0, 0}, updatedData.get(0).asBinary().getData());
        // Second binary unchanged (doesn't match)
        assertArrayEquals(new byte[]{4, 5, 6}, updatedData.get(1).asBinary().getData());
        // Third binary updated (match filter)
        assertArrayEquals(new byte[]{0, 0, 0}, updatedData.get(2).asBinary().getData());
    }

    @Test
    void shouldFilterWithByteArrayOperand() {
        // Behavior: Filtering with byte[] operand should match binary values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray binaries = new BsonArray();
        binaries.add(new BsonBinary(new byte[]{10, 20}));
        binaries.add(new BsonBinary(new byte[]{30, 40}));
        doc.put("blobs", binaries);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter using raw byte[] (not BsonBinary)
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "blob", new ArrayFilter("blob", Operator.EQ, new byte[]{10, 20})
        );

        Map<String, BsonValue> setOps = Map.of("blobs.$[blob]", new BsonBinary(new byte[]{0}));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedBlobs = updatedDoc.getArray("blobs");
        assertEquals(2, updatedBlobs.size());
        assertArrayEquals(new byte[]{0}, updatedBlobs.get(0).asBinary().getData());
        assertArrayEquals(new byte[]{30, 40}, updatedBlobs.get(1).asBinary().getData());
    }

    @Test
    void shouldFilterWithDecimal128Operand() {
        // Behavior: Filtering with BigDecimal operand should match Decimal128 values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray decimals = new BsonArray();
        decimals.add(new BsonDecimal128(org.bson.types.Decimal128.parse("10.5")));
        decimals.add(new BsonDecimal128(org.bson.types.Decimal128.parse("20.5")));
        decimals.add(new BsonDecimal128(org.bson.types.Decimal128.parse("30.5")));
        doc.put("prices", decimals);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter: prices > 15.0
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "price", new ArrayFilter("price", Operator.GT, new java.math.BigDecimal("15.0"))
        );

        Map<String, BsonValue> setOps = Map.of("prices.$[price]", new BsonDecimal128(org.bson.types.Decimal128.parse("0.0")));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedPrices = updatedDoc.getArray("prices");
        assertEquals(3, updatedPrices.size());
        // First price unchanged (not > 15)
        assertEquals("10.5", updatedPrices.get(0).asDecimal128().getValue().toString());
        // Second and third prices updated (> 15)
        assertEquals("0.0", updatedPrices.get(1).asDecimal128().getValue().toString());
        assertEquals("0.0", updatedPrices.get(2).asDecimal128().getValue().toString());
    }

    @Test
    void shouldFilterWithLongOperand() {
        // Behavior: Filtering with Long operand should match Int64 values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray longs = new BsonArray();
        longs.add(new BsonInt64(100L));
        longs.add(new BsonInt64(200L));
        longs.add(new BsonInt64(300L));
        doc.put("ids", longs);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter: ids <= 200
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "id", new ArrayFilter("id", Operator.LTE, 200L)
        );

        Map<String, BsonValue> setOps = Map.of("ids.$[id]", new BsonInt64(0L));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedIds = updatedDoc.getArray("ids");
        assertEquals(3, updatedIds.size());
        assertEquals(0L, updatedIds.get(0).asInt64().getValue());
        assertEquals(0L, updatedIds.get(1).asInt64().getValue());
        assertEquals(300L, updatedIds.get(2).asInt64().getValue());
    }

    @Test
    void shouldFilterWithDoubleOperand() {
        // Behavior: Filtering with Double operand should match double values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray doubles = new BsonArray();
        doubles.add(new BsonDouble(1.5));
        doubles.add(new BsonDouble(2.5));
        doubles.add(new BsonDouble(3.5));
        doc.put("values", doubles);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter: values != 2.5
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "val", new ArrayFilter("val", Operator.NE, 2.5)
        );

        Map<String, BsonValue> setOps = Map.of("values.$[val]", new BsonDouble(0.0));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedValues = updatedDoc.getArray("values");
        assertEquals(3, updatedValues.size());
        assertEquals(0.0, updatedValues.get(0).asDouble().getValue(), 0.001);
        assertEquals(2.5, updatedValues.get(1).asDouble().getValue(), 0.001); // unchanged
        assertEquals(0.0, updatedValues.get(2).asDouble().getValue(), 0.001);
    }

    @Test
    void shouldFilterWithBooleanOperand() {
        // Behavior: Filtering with Boolean operand should match boolean values in the array.
        BsonDocument doc = new BsonDocument();
        BsonArray booleans = new BsonArray();
        booleans.add(new BsonBoolean(true));
        booleans.add(new BsonBoolean(false));
        booleans.add(new BsonBoolean(true));
        doc.put("flags", booleans);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Filter: flags == true
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "flag", new ArrayFilter("flag", Operator.EQ, true)
        );

        Map<String, BsonValue> setOps = Map.of("flags.$[flag]", new BsonBoolean(false));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedFlags = updatedDoc.getArray("flags");
        assertEquals(3, updatedFlags.size());
        assertFalse(updatedFlags.get(0).asBoolean().getValue()); // was true, now false
        assertFalse(updatedFlags.get(1).asBoolean().getValue()); // unchanged
        assertFalse(updatedFlags.get(2).asBoolean().getValue()); // was true, now false
    }

    // ==================== $in and $nin Operator Tests ====================

    @Test
    void shouldUpdateArrayElementsMatchingInOperator() {
        // Behavior: $in operator should match array elements that are in the specified list.
        // Array: [85, 90, 75, 95, 60]
        // Filter: {"score": {"$in": [90, 95]}}
        // Expected: Elements 90 and 95 are updated to 100
        String json = "{\"scores\": [85, 90, 75, 95, 60]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "match", new ArrayFilter("match", Operator.IN, List.of(90, 95))
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[match]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray scores = updatedDoc.getArray("scores");
        assertEquals(5, scores.size());
        assertEquals(85, scores.get(0).asInt32().getValue());
        assertEquals(100, scores.get(1).asInt32().getValue()); // was 90, now 100
        assertEquals(75, scores.get(2).asInt32().getValue());
        assertEquals(100, scores.get(3).asInt32().getValue()); // was 95, now 100
        assertEquals(60, scores.get(4).asInt32().getValue());
    }

    @Test
    void shouldUpdateArrayElementsNotMatchingNinOperator() {
        // Behavior: $nin operator should match array elements NOT in the specified list.
        // Array: [85, 90, 75, 95, 60]
        // Filter: {"score": {"$nin": [90, 95]}}
        // Expected: Elements 85, 75, 60 are updated to 0
        String json = "{\"scores\": [85, 90, 75, 95, 60]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "nomatch", new ArrayFilter("nomatch", Operator.NIN, List.of(90, 95))
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[nomatch]", new BsonInt32(0));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray scores = updatedDoc.getArray("scores");
        assertEquals(5, scores.size());
        assertEquals(0, scores.get(0).asInt32().getValue()); // was 85, now 0
        assertEquals(90, scores.get(1).asInt32().getValue()); // unchanged
        assertEquals(0, scores.get(2).asInt32().getValue()); // was 75, now 0
        assertEquals(95, scores.get(3).asInt32().getValue()); // unchanged
        assertEquals(0, scores.get(4).asInt32().getValue()); // was 60, now 0
    }

    @Test
    void shouldHandleNullValuesInArrayWithFilter() {
        // Behavior: Array filter should handle null values in arrays correctly.
        // Array: [1, null, 3, null, 5]
        // Filter: {"elem": {"$eq": null}}
        // Expected: Only null elements are matched and updated
        BsonDocument doc = new BsonDocument();
        BsonArray arr = new BsonArray();
        arr.add(new BsonInt32(1));
        arr.add(BsonNull.VALUE);
        arr.add(new BsonInt32(3));
        arr.add(BsonNull.VALUE);
        arr.add(new BsonInt32(5));
        doc.put("values", arr);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.EQ, null)
        );

        Map<String, BsonValue> setOps = Map.of("values.$[elem]", new BsonInt32(0));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray values = updatedDoc.getArray("values");
        assertEquals(5, values.size());
        assertEquals(1, values.get(0).asInt32().getValue());
        assertEquals(0, values.get(1).asInt32().getValue()); // was null, now 0
        assertEquals(3, values.get(2).asInt32().getValue());
        assertEquals(0, values.get(3).asInt32().getValue()); // was null, now 0
        assertEquals(5, values.get(4).asInt32().getValue());
    }

    @Test
    void shouldHandleTypeMismatchInArrayFilter() {
        // Behavior: INT32 filter operand is widened to DOUBLE for comparison against DOUBLE array elements.
        // Array: [10.5, 11.5, 12.5] (DOUBLE)
        // Filter: {"score": {"$gt": 10}} (INT32)
        // All elements match because INT32 10 is widened to DOUBLE 10.0 for comparison.
        BsonDocument doc = new BsonDocument();
        BsonArray arr = new BsonArray();
        arr.add(new BsonDouble(10.5));
        arr.add(new BsonDouble(11.5));
        arr.add(new BsonDouble(12.5));
        doc.put("scores", arr);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "score", new ArrayFilter("score", Operator.GT, 10) // int operand
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[score]", new BsonDouble(999.0));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray scores = updatedDoc.getArray("scores");
        assertEquals(3, scores.size());
        // All elements match: 10.5 > 10.0, 11.5 > 10.0, 12.5 > 10.0
        assertEquals(999.0, scores.get(0).asDouble().getValue(), 0.001);
        assertEquals(999.0, scores.get(1).asDouble().getValue(), 0.001);
        assertEquals(999.0, scores.get(2).asDouble().getValue(), 0.001);
    }

    @Test
    void shouldApplyMultipleDifferentFiltersInSingleUpdate() {
        // Behavior: Multiple $[identifier] with different filters should work independently.
        // Array: items = [{type: "A", value: 10}, {type: "B", value: 20}, {type: "A", value: 30}]
        // Update: items.$[typeA].processed = true, items.$[highValue].priority = "high"
        // Filters: [{"typeA.type": "A"}, {"highValue.value": {"$gte": 20}}]
        String json = "{\"items\": [{\"type\": \"A\", \"value\": 10}, {\"type\": \"B\", \"value\": 20}, {\"type\": \"A\", \"value\": 30}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "typeA", new ArrayFilter("typeA.type", Operator.EQ, "A"),
                "highValue", new ArrayFilter("highValue.value", Operator.GTE, 20)
        );

        // Apply first update for typeA filter
        Map<String, BsonValue> setOps1 = Map.of("items.$[typeA].processed", new BsonBoolean(true));
        BSONUpdateUtil.DocumentUpdateResult result1 = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps1, Set.of(), arrayFilters
        );

        // Apply second update for highValue filter
        Map<String, BsonValue> setOps2 = Map.of("items.$[highValue].priority", new BsonString("high"));
        BSONUpdateUtil.DocumentUpdateResult result2 = BSONUpdateUtil.applyUpdateOperations(
                result1.document(), setOps2, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result2.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertEquals(3, items.size());

        // Item 0: type=A, value=10 -> processed=true, no priority
        assertTrue(items.get(0).containsKey("processed"));
        assertTrue(items.get(0).getBoolean("processed").getValue());
        assertFalse(items.get(0).containsKey("priority"));

        // Item 1: type=B, value=20 -> no processed, priority=high
        assertFalse(items.get(1).containsKey("processed"));
        assertTrue(items.get(1).containsKey("priority"));
        assertEquals("high", items.get(1).getString("priority").getValue());

        // Item 2: type=A, value=30 -> processed=true, priority=high
        assertTrue(items.get(2).containsKey("processed"));
        assertTrue(items.get(2).getBoolean("processed").getValue());
        assertTrue(items.get(2).containsKey("priority"));
        assertEquals("high", items.get(2).getString("priority").getValue());
    }

    @Test
    void shouldUpdateDocumentArrayElementsWithInOperator() {
        // Behavior: $in operator should work with nested paths in document arrays.
        // Array: items = [{category: "A"}, {category: "B"}, {category: "C"}]
        // Filter: {"item.category": {"$in": ["A", "C"]}}
        // Expected: Elements with category A and C are matched
        String json = "{\"items\": [{\"category\": \"A\", \"price\": 10}, {\"category\": \"B\", \"price\": 20}, {\"category\": \"C\", \"price\": 30}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "item", new ArrayFilter("item.category", Operator.IN, List.of("A", "C"))
        );

        Map<String, BsonValue> setOps = Map.of("items.$[item].discounted", new BsonBoolean(true));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertEquals(3, items.size());

        // Category A: discounted=true
        assertTrue(items.get(0).containsKey("discounted"));
        assertTrue(items.get(0).getBoolean("discounted").getValue());

        // Category B: no discounted field
        assertFalse(items.get(1).containsKey("discounted"));

        // Category C: discounted=true
        assertTrue(items.get(2).containsKey("discounted"));
        assertTrue(items.get(2).getBoolean("discounted").getValue());
    }

    @Test
    void shouldNotMatchAnyElementWithEmptyInList() {
        // Behavior: Empty $in a list should match no elements.
        // Array: [1, 2, 3]
        // Filter: {"elem": {"$in": []}}
        // Expected: No elements matched
        String json = "{\"values\": [1, 2, 3]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.IN, List.of())
        );

        Map<String, BsonValue> setOps = Map.of("values.$[elem]", new BsonInt32(999));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray values = updatedDoc.getArray("values");
        assertEquals(3, values.size());
        // No elements should be changed
        assertEquals(1, values.get(0).asInt32().getValue());
        assertEquals(2, values.get(1).asInt32().getValue());
        assertEquals(3, values.get(2).asInt32().getValue());
    }

    @Test
    void shouldMatchAllElementsWithEmptyNinList() {
        // Behavior: Empty $nin list should match all elements (nothing to exclude).
        // Array: [1, 2, 3]
        // Filter: {"elem": {"$nin": []}}
        // Expected: All elements matched
        String json = "{\"values\": [1, 2, 3]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.NIN, List.of())
        );

        Map<String, BsonValue> setOps = Map.of("values.$[elem]", new BsonInt32(999));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray values = updatedDoc.getArray("values");
        assertEquals(3, values.size());
        // All elements should be changed
        assertEquals(999, values.get(0).asInt32().getValue());
        assertEquals(999, values.get(1).asInt32().getValue());
        assertEquals(999, values.get(2).asInt32().getValue());
    }

    // ==================== $all Operator Tests ====================

    @Test
    void shouldUpdateArrayElementsMatchingAllOperator() {
        // Behavior: $all operator matches array elements containing all specified values.
        // Array of arrays: [[1,2,3], [1,2], [2,3,4], [1,2,3,4]]
        // Filter: {"elem": {"$all": [1, 2]}}
        // Expected: Elements [1,2,3], [1,2], [1,2,3,4] are matched
        BsonDocument doc = new BsonDocument();
        BsonArray arr = new BsonArray();
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))));
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2))));
        arr.add(new BsonArray(List.of(new BsonInt32(2), new BsonInt32(3), new BsonInt32(4))));
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3), new BsonInt32(4))));
        doc.put("data", arr);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.ALL, List.of(1, 2))
        );

        Map<String, BsonValue> setOps = Map.of("data.$[elem]", new BsonArray(List.of(new BsonInt32(0))));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray data = updatedDoc.getArray("data");
        assertEquals(4, data.size());

        // First: [1,2,3] contains 1 and 2 -> matched, becomes [0]
        assertEquals(1, data.get(0).asArray().size());
        assertEquals(0, data.get(0).asArray().get(0).asInt32().getValue());

        // Second: [1,2] contains 1 and 2 -> matched, becomes [0]
        assertEquals(1, data.get(1).asArray().size());
        assertEquals(0, data.get(1).asArray().get(0).asInt32().getValue());

        // Third: [2,3,4] does NOT contain 1 -> not matched
        assertEquals(3, data.get(2).asArray().size());
        assertEquals(2, data.get(2).asArray().get(0).asInt32().getValue());

        // Fourth: [1,2,3,4] contains 1 and 2 -> matched, becomes [0]
        assertEquals(1, data.get(3).asArray().size());
        assertEquals(0, data.get(3).asArray().get(0).asInt32().getValue());
    }

    @Test
    void shouldMatchAllElementsWithEmptyAllList() {
        // Behavior: Empty $all list should match all array elements (vacuous truth).
        // Array of arrays: [[1,2], [3,4]]
        // Filter: {"elem": {"$all": []}}
        // Expected: All elements matched
        BsonDocument doc = new BsonDocument();
        BsonArray arr = new BsonArray();
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2))));
        arr.add(new BsonArray(List.of(new BsonInt32(3), new BsonInt32(4))));
        doc.put("data", arr);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.ALL, List.of())
        );

        Map<String, BsonValue> setOps = Map.of("data.$[elem]", new BsonArray(List.of(new BsonInt32(0))));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray data = updatedDoc.getArray("data");
        assertEquals(2, data.size());

        // All elements should be replaced with [0]
        assertEquals(1, data.get(0).asArray().size());
        assertEquals(0, data.get(0).asArray().get(0).asInt32().getValue());
        assertEquals(1, data.get(1).asArray().size());
        assertEquals(0, data.get(1).asArray().get(0).asInt32().getValue());
    }

    // ==================== $size Operator Tests ====================

    @Test
    void shouldUpdateArrayElementsMatchingSizeOperator() {
        // Behavior: $size operator matches array elements with specific size.
        // Array of arrays: [[1,2], [1,2,3], [1], [1,2,3,4]]
        // Filter: {"elem": {"$size": 3}}
        // Expected: Only [1,2,3] is matched
        BsonDocument doc = new BsonDocument();
        BsonArray arr = new BsonArray();
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2))));
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))));
        arr.add(new BsonArray(List.of(new BsonInt32(1))));
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3), new BsonInt32(4))));
        doc.put("data", arr);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.SIZE, 3)
        );

        Map<String, BsonValue> setOps = Map.of("data.$[elem]", new BsonArray(List.of(new BsonInt32(999))));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray data = updatedDoc.getArray("data");
        assertEquals(4, data.size());

        // First: size 2 -> not matched
        assertEquals(2, data.get(0).asArray().size());

        // Second: size 3 -> matched, becomes [999]
        assertEquals(1, data.get(1).asArray().size());
        assertEquals(999, data.get(1).asArray().get(0).asInt32().getValue());

        // Third: size 1 -> not matched
        assertEquals(1, data.get(2).asArray().size());
        assertEquals(1, data.get(2).asArray().get(0).asInt32().getValue());

        // Fourth: size 4 -> not matched
        assertEquals(4, data.get(3).asArray().size());
    }

    @Test
    void shouldNotMatchNonArraysWithSizeOperator() {
        // Behavior: $size operator should not match non-array values.
        // Array: [{count: 3}, {count: 5}]
        // Filter: {"elem.count": {"$size": 3}}
        // Expected: No match (count is scalar, not array)
        String json = "{\"items\": [{\"count\": 3}, {\"count\": 5}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem.count", Operator.SIZE, 3)
        );

        Map<String, BsonValue> setOps = Map.of("items.$[elem].matched", new BsonBoolean(true));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> items = (List<BsonDocument>) updatedDoc.get("items");
        assertEquals(2, items.size());

        // Neither should have matched field
        assertFalse(items.get(0).containsKey("matched"));
        assertFalse(items.get(1).containsKey("matched"));
    }

    // ==================== $exists Operator Tests ====================

    @Test
    void shouldUpdateArrayElementsWithExistsTrue() {
        // Behavior: $exists: true matches document elements where field exists.
        // Array: [{name: "A"}, {name: "B", age: 10}, {age: 20}]
        // Filter: {"elem.age": {"$exists": true}}
        // Expected: Elements with age field are matched
        String json = "{\"people\": [{\"name\": \"A\"}, {\"name\": \"B\", \"age\": 10}, {\"age\": 20}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem.age", Operator.EXISTS, true)
        );

        Map<String, BsonValue> setOps = Map.of("people.$[elem].hasAge", new BsonBoolean(true));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> people = (List<BsonDocument>) updatedDoc.get("people");
        assertEquals(3, people.size());

        // First: no age -> not matched
        assertFalse(people.get(0).containsKey("hasAge"));

        // Second: has age -> matched
        assertTrue(people.get(1).containsKey("hasAge"));
        assertTrue(people.get(1).getBoolean("hasAge").getValue());

        // Third: has age -> matched
        assertTrue(people.get(2).containsKey("hasAge"));
        assertTrue(people.get(2).getBoolean("hasAge").getValue());
    }

    @Test
    void shouldUpdateArrayElementsWithExistsFalse() {
        // Behavior: $exists: false matches elements where field is missing.
        // Array: [{name: "A"}, {name: "B", age: 10}, {age: 20}]
        // Filter: {"elem.age": {"$exists": false}}
        // Expected: Only {name: "A"} is matched
        String json = "{\"people\": [{\"name\": \"A\"}, {\"name\": \"B\", \"age\": 10}, {\"age\": 20}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem.age", Operator.EXISTS, false)
        );

        Map<String, BsonValue> setOps = Map.of("people.$[elem].noAge", new BsonBoolean(true));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> people = (List<BsonDocument>) updatedDoc.get("people");
        assertEquals(3, people.size());

        // First: no age -> matched
        assertTrue(people.get(0).containsKey("noAge"));
        assertTrue(people.get(0).getBoolean("noAge").getValue());

        // Second: has age -> not matched
        assertFalse(people.get(1).containsKey("noAge"));

        // Third: has age -> not matched
        assertFalse(people.get(2).containsKey("noAge"));
    }

    // ==================== Edge Case Tests ====================

    @Test
    void shouldHandleNestedArraysWithFilter() {
        // Behavior: Filter should work on nested arrays.
        // Array: [[1,2,3], [4,5,6], [7,8,9]]
        // Filter: {"elem": {"$size": 3}}
        // All elements have size 3, so all are matched
        BsonDocument doc = new BsonDocument();
        BsonArray arr = new BsonArray();
        arr.add(new BsonArray(List.of(new BsonInt32(1), new BsonInt32(2), new BsonInt32(3))));
        arr.add(new BsonArray(List.of(new BsonInt32(4), new BsonInt32(5), new BsonInt32(6))));
        arr.add(new BsonArray(List.of(new BsonInt32(7), new BsonInt32(8), new BsonInt32(9))));
        doc.put("matrix", arr);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "row", new ArrayFilter("row", Operator.SIZE, 3)
        );

        Map<String, BsonValue> setOps = Map.of("matrix.$[row]", new BsonArray(List.of(new BsonInt32(0))));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray matrix = updatedDoc.getArray("matrix");
        assertEquals(3, matrix.size());

        // All rows should be replaced
        for (int i = 0; i < 3; i++) {
            assertEquals(1, matrix.get(i).asArray().size());
            assertEquals(0, matrix.get(i).asArray().get(0).asInt32().getValue());
        }
    }

    @Test
    void shouldHandleDeepNestedPathWithFilter() {
        // Behavior: Deep paths like a.$[x].b.c should work with filters
        // Doc: {a: [{b: {c: 1, d: 2}}, {b: {c: 3, d: 4}}]}
        String json = "{\"a\": [{\"b\": {\"c\": 1, \"d\": 2}}, {\"b\": {\"c\": 3, \"d\": 4}}]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "x", new ArrayFilter("x.b.c", Operator.GTE, 2)
        );

        Map<String, BsonValue> setOps = Map.of("a.$[x].b.c", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        @SuppressWarnings("unchecked")
        List<BsonDocument> a = (List<BsonDocument>) updatedDoc.get("a");
        assertEquals(2, a.size());

        // First: c=1, not >= 2 -> not matched
        BsonDocument b1 = (BsonDocument) a.get(0).get("b");
        assertEquals(1, b1.getInt32("c").getValue());
        assertEquals(2, b1.getInt32("d").getValue());

        // Second: c=3, >= 2 -> matched, c becomes 100
        BsonDocument b2 = (BsonDocument) a.get(1).get("b");
        assertEquals(100, b2.getInt32("c").getValue());
        assertEquals(4, b2.getInt32("d").getValue());
    }

    @Test
    void shouldHandleMixedTypeArrayWithFilter() {
        // Behavior: Arrays with mixed types should be handled correctly.
        // Array: [1, "two", 3.0, null, true]
        // Filter: {"elem": {"$eq": 1}} matches only integer 1
        BsonDocument doc = new BsonDocument();
        BsonArray arr = new BsonArray();
        arr.add(new BsonInt32(1));
        arr.add(new BsonString("two"));
        arr.add(new BsonDouble(3.0));
        arr.add(BsonNull.VALUE);
        arr.add(new BsonBoolean(true));
        doc.put("values", arr);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.EQ, 1)
        );

        Map<String, BsonValue> setOps = Map.of("values.$[elem]", new BsonInt32(999));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray values = updatedDoc.getArray("values");
        assertEquals(5, values.size());

        // First: 1 -> matched, becomes 999
        assertEquals(999, values.get(0).asInt32().getValue());
        // Second: "two" -> not matched
        assertEquals("two", values.get(1).asString().getValue());
        // Third: 3.0 -> not matched (different type)
        assertEquals(3.0, values.get(2).asDouble().getValue(), 0.001);
        // Fourth: null -> not matched
        assertTrue(values.get(3).isNull());
        // Fifth: true -> not matched
        assertTrue(values.get(4).asBoolean().getValue());
    }

    // ==================== POSITIONAL OPERATOR newValues TESTS ====================

    @Test
    void shouldReturnActualArrayInNewValuesWhenUsingPositionalFilterOperator() {
        // Behavior: When using $[identifier] positional operator, newValues should contain
        // the actual array state after modification, not the operation value.
        BsonDocument doc = new BsonDocument();
        BsonArray scores = new BsonArray();
        scores.add(new BsonInt32(55));
        scores.add(new BsonInt32(60));
        scores.add(new BsonInt32(65));
        scores.add(new BsonInt32(70));
        scores.add(new BsonInt32(75));
        doc.put("name", new BsonString("Alice"));
        doc.put("scores", scores);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Update elements <= 60 to 0
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "low", new ArrayFilter("low", Operator.LTE, 60)
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[low]", new BsonInt32(0));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        // Verify newValues contains the actual array, not the operation value (0)
        assertTrue(result.newValues().containsKey("scores"), "newValues should contain 'scores'");
        BsonValue scoresValue = result.newValues().get("scores");
        assertTrue(scoresValue.isArray(), "newValues['scores'] should be an array");
        BsonArray actualScores = scoresValue.asArray();
        assertEquals(5, actualScores.size(), "Array should still have 5 elements");
        assertEquals(0, actualScores.get(0).asInt32().getValue(), "First element should be 0");
        assertEquals(0, actualScores.get(1).asInt32().getValue(), "Second element should be 0");
        assertEquals(65, actualScores.get(2).asInt32().getValue(), "Third element should be 65");
        assertEquals(70, actualScores.get(3).asInt32().getValue(), "Fourth element should be 70");
        assertEquals(75, actualScores.get(4).asInt32().getValue(), "Fifth element should be 75");
    }

    @Test
    void shouldReturnActualArrayInNewValuesWhenUsingAllPositionalOperator() {
        // Behavior: When using $[] (all positional) operator, newValues should contain
        // the actual array state after modification, not the operation value.
        BsonDocument doc = new BsonDocument();
        BsonArray values = new BsonArray();
        values.add(new BsonInt32(1));
        values.add(new BsonInt32(2));
        values.add(new BsonInt32(3));
        doc.put("values", values);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Update all elements to 100
        Map<String, BsonValue> setOps = Map.of("values.$[]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), null
        );

        // Verify newValues contains the actual array
        assertTrue(result.newValues().containsKey("values"), "newValues should contain 'values'");
        BsonValue valuesResult = result.newValues().get("values");
        assertTrue(valuesResult.isArray(), "newValues['values'] should be an array");
        BsonArray actualValues = valuesResult.asArray();
        assertEquals(3, actualValues.size(), "Array should still have 3 elements");
        assertEquals(100, actualValues.get(0).asInt32().getValue());
        assertEquals(100, actualValues.get(1).asInt32().getValue());
        assertEquals(100, actualValues.get(2).asInt32().getValue());
    }

    @Test
    void shouldReturnActualArrayInNewValuesForNestedPositionalUpdate() {
        // Behavior: When using positional operators on nested paths like items.$[elem].price,
        // newValues should contain the actual items.price array with updated prices.
        BsonDocument doc = new BsonDocument();
        BsonArray items = new BsonArray();
        BsonDocument item1 = new BsonDocument();
        item1.put("name", new BsonString("A"));
        item1.put("price", new BsonInt32(10));
        BsonDocument item2 = new BsonDocument();
        item2.put("name", new BsonString("B"));
        item2.put("price", new BsonInt32(20));
        BsonDocument item3 = new BsonDocument();
        item3.put("name", new BsonString("C"));
        item3.put("price", new BsonInt32(30));
        items.add(item1);
        items.add(item2);
        items.add(item3);
        doc.put("items", items);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Update price to 50 for items where price >= 20
        // Map key "expensive" must match the identifier in $[expensive].
        // ArrayFilter identifier "expensive.price" is the nested path for extracting comparison values.
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "expensive", new ArrayFilter("expensive.price", Operator.GTE, 20)
        );

        Map<String, BsonValue> setOps = Map.of("items.$[expensive].price", new BsonInt32(50));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        // Verify newValues contains the normalized path "items.price" with actual array of prices
        assertTrue(result.newValues().containsKey("items.price"), "newValues should contain 'items.price'");
        BsonValue pricesValue = result.newValues().get("items.price");

        // The SelectorMatcher extracts nested values as an array
        assertTrue(pricesValue.isArray(), "newValues['items.price'] should be an array");
        BsonArray actualPrices = pricesValue.asArray();
        assertEquals(3, actualPrices.size());
        assertEquals(10, actualPrices.get(0).asInt32().getValue(), "First item price unchanged");
        assertEquals(50, actualPrices.get(1).asInt32().getValue(), "Second item price updated");
        assertEquals(50, actualPrices.get(2).asInt32().getValue(), "Third item price updated");
    }

    @Test
    void shouldReturnActualArrayAfterUnsetWithPositionalOperator() {
        // Behavior: When using $unset with $[identifier], newValues should contain the
        // actual array after removing matching elements.
        BsonDocument doc = new BsonDocument();
        BsonArray scores = new BsonArray();
        scores.add(new BsonInt32(55));
        scores.add(new BsonInt32(60));
        scores.add(new BsonInt32(65));
        scores.add(new BsonInt32(70));
        scores.add(new BsonInt32(75));
        doc.put("scores", scores);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Unset (remove) elements <= 60
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "low", new ArrayFilter("low", Operator.LTE, 60)
        );

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, Map.of(), Set.of("scores.$[low]"), arrayFilters
        );

        // Verify newValues contains the actual array after removal
        assertTrue(result.newValues().containsKey("scores"), "newValues should contain 'scores'");
        assertFalse(result.droppedSelectors().contains("scores"), "scores should not be in droppedSelectors");
        BsonValue scoresValue = result.newValues().get("scores");
        assertTrue(scoresValue.isArray(), "newValues['scores'] should be an array");
        BsonArray actualScores = scoresValue.asArray();
        assertEquals(3, actualScores.size(), "Array should have 3 elements after removal");
        assertEquals(65, actualScores.get(0).asInt32().getValue());
        assertEquals(70, actualScores.get(1).asInt32().getValue());
        assertEquals(75, actualScores.get(2).asInt32().getValue());
    }

    @Test
    void shouldAddToDroppedSelectorsWhenUnsetRemovesAllElements() {
        // Behavior: When $unset removes all array elements, newValues should contain
        // the empty array (not droppedSelectors, since array still exists).
        BsonDocument doc = new BsonDocument();
        BsonArray scores = new BsonArray();
        scores.add(new BsonInt32(55));
        scores.add(new BsonInt32(60));
        doc.put("scores", scores);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        // Unset all elements (filter matches all)
        Map<String, ArrayFilter> arrayFilters = Map.of(
                "all", new ArrayFilter("all", Operator.LTE, 100)
        );

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, Map.of(), Set.of("scores.$[all]"), arrayFilters
        );

        // After removing all elements, the array is empty but still exists
        // newValues should contain the empty array
        assertTrue(result.newValues().containsKey("scores"), "newValues should contain 'scores'");
        BsonValue scoresValue = result.newValues().get("scores");
        assertTrue(scoresValue.isArray(), "newValues['scores'] should be an array");
        assertEquals(0, scoresValue.asArray().size(), "Array should be empty");
    }

    @Test
    void shouldHandleMixedPositionalAndNonPositionalOperations() {
        // Behavior: When mixing positional and non-positional operations in the same update,
        // positional operations return actual array values while non-positional return operation values.
        BsonDocument doc = new BsonDocument();
        BsonArray scores = new BsonArray();
        scores.add(new BsonInt32(55));
        scores.add(new BsonInt32(60));
        scores.add(new BsonInt32(65));
        doc.put("name", new BsonString("Alice"));
        doc.put("scores", scores);
        doc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "low", new ArrayFilter("low", Operator.LTE, 60)
        );

        Map<String, BsonValue> setOps = new HashMap<>();
        setOps.put("scores.$[low]", new BsonInt32(0));    // Positional
        setOps.put("age", new BsonInt32(30));             // Non-positional
        setOps.put("status", new BsonString("active"));   // Non-positional (new field)

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        // Verify positional operation returns actual array
        assertTrue(result.newValues().containsKey("scores"));
        BsonArray actualScores = result.newValues().get("scores").asArray();
        assertEquals(3, actualScores.size());
        assertEquals(0, actualScores.get(0).asInt32().getValue());
        assertEquals(0, actualScores.get(1).asInt32().getValue());
        assertEquals(65, actualScores.get(2).asInt32().getValue());

        // Verify non-positional operations return operation values directly
        assertTrue(result.newValues().containsKey("age"));
        assertEquals(30, result.newValues().get("age").asInt32().getValue());

        assertTrue(result.newValues().containsKey("status"));
        assertEquals("active", result.newValues().get("status").asString().getValue());
    }

    @Test
    void shouldDeduplicatePositionalSelectorsInNewValues() {
        // Behavior: Multiple positional operations on the same normalized path should
        // result in a single entry in newValues with the final array state.
        BsonDocument doc = new BsonDocument();
        BsonArray scores = new BsonArray();
        scores.add(new BsonInt32(10));
        scores.add(new BsonInt32(20));
        scores.add(new BsonInt32(30));
        scores.add(new BsonInt32(40));
        doc.put("scores", scores);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "low", new ArrayFilter("low", Operator.LTE, 20),
                "high", new ArrayFilter("high", Operator.GTE, 30)
        );

        // Two positional operations on the same field
        Map<String, BsonValue> setOps = new HashMap<>();
        setOps.put("scores.$[low]", new BsonInt32(0));    // 10, 20 -> 0
        setOps.put("scores.$[high]", new BsonInt32(100)); // 30, 40 -> 100

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        // Verify only one entry for "scores" with final state
        assertTrue(result.newValues().containsKey("scores"));
        BsonArray actualScores = result.newValues().get("scores").asArray();
        assertEquals(4, actualScores.size());
        assertEquals(0, actualScores.get(0).asInt32().getValue());
        assertEquals(0, actualScores.get(1).asInt32().getValue());
        assertEquals(100, actualScores.get(2).asInt32().getValue());
        assertEquals(100, actualScores.get(3).asInt32().getValue());
    }

    @Test
    void shouldHandleUnsetWithAllPositionalOperatorOnNestedField() {
        // Behavior: Using $unset with $[] on nested fields removes that field from
        // all array elements and returns the actual array state.
        BsonDocument doc = new BsonDocument();
        BsonArray tags = new BsonArray();
        BsonDocument tag1 = new BsonDocument();
        tag1.put("name", new BsonString("java"));
        tag1.put("priority", new BsonInt32(1));
        BsonDocument tag2 = new BsonDocument();
        tag2.put("name", new BsonString("kotlin"));
        tag2.put("priority", new BsonInt32(2));
        tags.add(tag1);
        tags.add(tag2);
        doc.put("tags", tags);
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.toBytes(doc));

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, Map.of(), Set.of("tags.$[].name"), null
        );

        // Verify the nested field path is in newValues (with remaining array values - null for removed fields)
        // or droppedSelectors depending on the actual behavior
        assertTrue(result.newValues().containsKey("tags.name") || result.droppedSelectors().contains("tags.name"),
                "tags.name should be in newValues or droppedSelectors");

        // Verify the document structure
        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray updatedTags = updatedDoc.getArray("tags");
        assertEquals(2, updatedTags.size());
        // Each tag should no longer have "name" field
        assertFalse(updatedTags.get(0).asDocument().containsKey("name"));
        assertFalse(updatedTags.get(1).asDocument().containsKey("name"));
        // But priority should remain
        assertEquals(1, updatedTags.get(0).asDocument().getInt32("priority").getValue());
        assertEquals(2, updatedTags.get(1).asDocument().getInt32("priority").getValue());
    }

    @Test
    void shouldUnsetNestedFieldWithPositionalOperator() {
        // Behavior: $unset with $ operator should remove the field from the matched element only.
        BsonDocument doc = new BsonDocument()
                .append("orderId", new BsonString("ORD-001"))
                .append("items", new BsonArray(List.of(
                        new BsonDocument()
                                .append("name", new BsonString("Keep"))
                                .append("price", new BsonInt32(100)),
                        new BsonDocument()
                                .append("name", new BsonString("Remove"))
                                .append("price", new BsonInt32(200)),
                        new BsonDocument()
                                .append("name", new BsonString("Keep2"))
                                .append("price", new BsonInt32(300))
                )));

        ByteBuffer buffer = BSONUtil.toByteBuffer(doc);

        // matchedPositions: the second item (index 1) matched the query
        Map<String, Integer> matchedPositions = Map.of("items", 1);

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                buffer,
                Map.of(),
                Set.of("items.$.price"),
                null,
                matchedPositions
        );

        BsonDocument updatedDoc = BSONUtil.fromBson(result.document());
        BsonArray items = updatedDoc.getArray("items");
        assertEquals(3, items.size(), "Array should still have 3 elements");

        // First item - price should remain
        assertTrue(items.get(0).asDocument().containsKey("price"), "First item should still have price");
        assertEquals(100, items.get(0).asDocument().getInt32("price").getValue());

        // Second item (matched) - price should be removed
        assertFalse(items.get(1).asDocument().containsKey("price"), "Second item's price should be removed");
        assertEquals("Remove", items.get(1).asDocument().getString("name").getValue());

        // Third item - price should remain
        assertTrue(items.get(2).asDocument().containsKey("price"), "Third item should still have price");
        assertEquals(300, items.get(2).asDocument().getInt32("price").getValue());
    }

    @Test
    void shouldThrowWhenMatchedIndexIsNegativeForSet() {
        // Behavior: $set with $ operator should throw when matched index is negative.
        BsonDocument doc = new BsonDocument()
                .append("items", new BsonArray(List.of(
                        new BsonDocument().append("name", new BsonString("A")),
                        new BsonDocument().append("name", new BsonString("B"))
                )));

        ByteBuffer buffer = BSONUtil.toByteBuffer(doc);

        // Create a mutable map with negative index
        Map<String, Integer> matchedPositions = new HashMap<>();
        matchedPositions.put("items", -1);

        assertThrows(IllegalStateException.class, () ->
                BSONUpdateUtil.applyUpdateOperations(
                        buffer,
                        Map.of("items.$.name", new BsonString("Updated")),
                        Set.of(),
                        null,
                        matchedPositions
                ));
    }

    @Test
    void shouldThrowWhenMatchedIndexIsNegativeForUnset() {
        // Behavior: $unset with $ operator should throw when matched index is negative.
        BsonDocument doc = new BsonDocument()
                .append("items", new BsonArray(List.of(
                        new BsonDocument().append("name", new BsonString("A")).append("price", new BsonInt32(100)),
                        new BsonDocument().append("name", new BsonString("B")).append("price", new BsonInt32(200))
                )));

        ByteBuffer buffer = BSONUtil.toByteBuffer(doc);

        // Create a mutable map with negative index
        Map<String, Integer> matchedPositions = new HashMap<>();
        matchedPositions.put("items", -1);

        assertThrows(IllegalStateException.class, () ->
                BSONUpdateUtil.applyUpdateOperations(
                        buffer,
                        Map.of(),
                        Set.of("items.$.price"),
                        null,
                        matchedPositions
                ));
    }

    @Test
    void shouldReportChangedWhenSetModifiesValue() {
        // Behavior: $set writing a different value modifies the document; changed() is true.
        String json = "{\"name\": \"John\", \"age\": 25}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("age", new BsonInt32(30));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        assertTrue(result.changed(), "Writing a different value should report changed");
    }

    @Test
    void shouldReportNotChangedWhenSetWritesIdenticalValue() {
        // Behavior: $set writing the value a field already holds leaves the document
        // byte-identical; changed() is false.
        String json = "{\"name\": \"John\", \"age\": 25}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, BsonValue> setOps = Map.of("age", new BsonInt32(25));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, setOps, Set.of());

        assertFalse(result.changed(), "Writing an identical value should not report changed");
        BsonDocument expectedDoc = BSONUtil.fromBson(ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json)));
        assertEquals(expectedDoc, BSONUtil.fromBson(result.document()), "Document should remain unchanged");
    }

    @Test
    void shouldReportNotChangedWhenPositionalSetTargetsMissingArray() {
        // Behavior: $set with $[identifier] on a document that lacks the target array is a no-op;
        // changed() is false.
        String json = "{\"name\": \"Eve\"}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.GTE, 80)
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[elem]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        assertFalse(result.changed(), "Positional set on a missing array should not report changed");
    }

    @Test
    void shouldReportChangedWhenPositionalSetMatchesArrayElement() {
        // Behavior: $set with $[identifier] that updates a matching array element modifies the
        // document; changed() is true.
        String json = "{\"name\": \"Henry\", \"scores\": [75, 85, 95]}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        Map<String, ArrayFilter> arrayFilters = Map.of(
                "elem", new ArrayFilter("elem", Operator.GTE, 80)
        );

        Map<String, BsonValue> setOps = Map.of("scores.$[elem]", new BsonInt32(100));
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, setOps, Set.of(), arrayFilters
        );

        assertTrue(result.changed(), "Positional set updating a matching element should report changed");
    }

    @Test
    void shouldReportNotChangedWhenUnsetTargetsNonexistentField() {
        // Behavior: $unset on a field the document does not contain is a no-op; changed() is false.
        String json = "{\"name\": \"John\"}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, Map.of(), Set.of("age")
        );

        assertFalse(result.changed(), "Unsetting a nonexistent field should not report changed");
    }

    @Test
    void shouldReportChangedWhenUnsetRemovesExistingField() {
        // Behavior: $unset removing an existing field modifies the document; changed() is true.
        String json = "{\"name\": \"John\", \"age\": 25}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, Map.of(), Set.of("age")
        );

        assertTrue(result.changed(), "Removing an existing field should report changed");
    }

    @Test
    void shouldReportNotChangedWhenSetOpsAndUnsetOpsAreEmpty() {
        // Behavior: an update with empty setOps and unsetOps returns the original document
        // untouched; changed() is false.
        String json = "{\"name\": \"John\"}";
        ByteBuffer originalBuffer = ByteBuffer.wrap(BSONUtil.jsonToDocumentThenBytes(json));

        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(
                originalBuffer, Map.of(), Set.of()
        );

        assertFalse(result.changed(), "Empty update operations should not report changed");
        assertSame(originalBuffer, result.document());
    }
}