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

package com.kronotop.bucket.handlers;

import com.kronotop.KronotopException;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CompoundIndexSchemaTest {

    private CompoundFieldSchema makeField(String selector, BsonType bsonType) {
        CompoundFieldSchema field = new CompoundFieldSchema();
        field.setSelector(selector);
        field.setBsonType(bsonType);
        return field;
    }

    private CompoundFieldSchema makeMultiKeyField(String selector, BsonType bsonType) {
        CompoundFieldSchema field = new CompoundFieldSchema();
        field.setSelector(selector);
        field.setBsonType(bsonType);
        field.setMultiKey(true);
        return field;
    }

    @Test
    void shouldValidateWithTwoFields() {
        // Behavior: A compound index schema with exactly 2 valid fields passes validation.
        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(
                makeField("category", BsonType.STRING),
                makeField("price", BsonType.DOUBLE)
        ));
        assertDoesNotThrow(schema::validate);
    }

    @Test
    void shouldRejectNullFields() {
        // Behavior: A compound index schema with null fields is rejected.
        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(null);
        KronotopException ex = assertThrows(KronotopException.class, schema::validate);
        assertEquals("Compound index requires at least 2 fields", ex.getMessage());
    }

    @Test
    void shouldRejectSingleField() {
        // Behavior: A compound index schema with only 1 field is rejected.
        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(makeField("category", BsonType.STRING)));
        KronotopException ex = assertThrows(KronotopException.class, schema::validate);
        assertEquals("Compound index requires at least 2 fields", ex.getMessage());
    }

    @Test
    void shouldRejectMissingSelector() {
        // Behavior: A field with null selector is rejected.
        CompoundFieldSchema bad = new CompoundFieldSchema();
        bad.setBsonType(BsonType.STRING);

        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(makeField("category", BsonType.STRING), bad));
        KronotopException ex = assertThrows(KronotopException.class, schema::validate);
        assertEquals("Each compound index field must have a 'selector'", ex.getMessage());
    }

    @Test
    void shouldRejectEmptySelector() {
        // Behavior: A field with an empty string selector is rejected.
        CompoundFieldSchema bad = new CompoundFieldSchema();
        bad.setSelector("");
        bad.setBsonType(BsonType.STRING);

        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(makeField("category", BsonType.STRING), bad));
        KronotopException ex = assertThrows(KronotopException.class, schema::validate);
        assertEquals("Each compound index field must have a 'selector'", ex.getMessage());
    }

    @Test
    void shouldRejectMissingBsonType() {
        // Behavior: A field with null bson_type is rejected.
        CompoundFieldSchema bad = new CompoundFieldSchema();
        bad.setSelector("price");

        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(makeField("category", BsonType.STRING), bad));
        KronotopException ex = assertThrows(KronotopException.class, schema::validate);
        assertEquals("Each compound index field must have a 'bson_type'", ex.getMessage());
    }

    @Test
    void shouldRejectDuplicateSelectors() {
        // Behavior: Duplicate selectors within a compound index are rejected.
        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(
                makeField("category", BsonType.STRING),
                makeField("category", BsonType.INT32)
        ));
        KronotopException ex = assertThrows(KronotopException.class, schema::validate);
        assertEquals("Duplicate selector in compound index: 'category'", ex.getMessage());
    }

    @Test
    void shouldAcceptThreeOrMoreFields() {
        // Behavior: A compound index with more than 2 fields is valid.
        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(
                makeField("a", BsonType.STRING),
                makeField("b", BsonType.INT32),
                makeField("c", BsonType.DOUBLE)
        ));
        assertDoesNotThrow(schema::validate);
    }

    @Test
    void shouldAcceptOneMultiKeyField() {
        // Behavior: A compound index with exactly one multi-key field passes validation.
        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(
                makeField("category", BsonType.STRING),
                makeMultiKeyField("tags", BsonType.STRING)
        ));
        assertDoesNotThrow(schema::validate);
    }

    @Test
    void shouldRejectMultipleMultiKeyFields() {
        // Behavior: A compound index with more than one multi-key field is rejected.
        CompoundIndexSchema schema = new CompoundIndexSchema();
        schema.setFields(List.of(
                makeMultiKeyField("tags", BsonType.STRING),
                makeMultiKeyField("categories", BsonType.STRING)
        ));
        KronotopException ex = assertThrows(KronotopException.class, schema::validate);
        assertEquals("Compound index allows at most one multi-key field", ex.getMessage());
    }
}
