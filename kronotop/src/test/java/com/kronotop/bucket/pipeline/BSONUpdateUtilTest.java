package com.kronotop.bucket.pipeline;

import org.bson.*;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
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
    void testApplySetOperationsWithEmptyUpdateOps() {
        // Create original document
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
    void testUpdateExistingStringField() {
        // Create original document
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
    void testAddNewField() {
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
    void testMixedOperationsUpdateAndAdd() {
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
    void testAllBsonDataTypes() {
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
    void testUpdateWithNullValue() {
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
    void testConvertToBsonValueWithAllJavaTypes() {
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
    void testEmptyDocument() {
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
    void testPreservesFieldOrder() {
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
    void testNewValuesTracking() {
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
    void testNewValuesWithAllBsonTypes() {
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
    void testUpdateAndAddFieldsInSameOperation() {
        // Create original document
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
    void testUnsetSingleField() {
        // Create original document
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
    void testUnsetMultipleFields() {
        // Create original document
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
    void testUnsetNonExistentField() {
        // Create original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Try to unset non-existent field
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
    void testSetAndUnsetInSameOperation() {
        // Create original document
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
    void testUnsetAllFields() {
        // Create original document
        BsonDocument originalDoc = new BsonDocument();
        originalDoc.put("name", new BsonString("John"));
        originalDoc.put("age", new BsonInt32(25));
        ByteBuffer originalBuffer = createBsonDocument(originalDoc);

        // Unset all fields
        Set<String> unsetOps = Set.of("name", "age");
        BSONUpdateUtil.DocumentUpdateResult result = BSONUpdateUtil.applyUpdateOperations(originalBuffer, Map.of(), unsetOps);

        // Verify document is empty
        BsonDocument updatedDoc = readBsonDocument(result.document());
        assertTrue(updatedDoc.isEmpty());

        // Verify all fields in droppedSelectors
        assertEquals(Set.of("name", "age"), result.droppedSelectors());
    }
}