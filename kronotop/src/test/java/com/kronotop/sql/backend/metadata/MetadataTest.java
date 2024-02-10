/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.backend.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MetadataTest {
    protected Metadata root;
    protected String testSchema = "TestSchema";

    @BeforeEach
    public void setUp() {
        root = new Metadata();
    }

    @Test
    public void testPut() {
        SchemaMetadata testMetadata = new SchemaMetadata(testSchema);

        // Testing regular flow of function
        assertDoesNotThrow(() -> root.put(testSchema, testMetadata));

        // Testing if SchemaAlreadyExistsException is thrown when a schema is inserted that already exists in metadata store
        SchemaMetadata duplicateMetadata = new SchemaMetadata(testSchema);
        assertThrows(SchemaAlreadyExistsException.class, () -> root.put(testSchema, duplicateMetadata));
    }

    @Test
    public void testGet_existingSchemaMetadata() throws SchemaAlreadyExistsException, SchemaNotExistsException {
        SchemaMetadata metadata = new SchemaMetadata(testSchema);
        String schema = "test";
        root.put(schema, metadata);
        assertEquals(metadata, root.get(schema));
    }

    @Test
    public void testGetNonExistingSchemaMetadata() {
        assertThrows(SchemaNotExistsException.class, () -> {
            root.get("foobar");
        });
    }

    @Test
    public void testRemoveSchemaDoesNotExistsException() {
        assertThrows(SchemaNotExistsException.class, () -> {
            root.remove("foobar");
        });
    }

    @Test
    public void testRemoveSchemaSuccess() throws SchemaAlreadyExistsException, SchemaNotExistsException {
        String schemaToRemove = "foobar";

        // Add a schema to the metadata store
        root.put(schemaToRemove, new SchemaMetadata(testSchema));

        // Assert that the schema was added to the metadata store
        assertTrue(root.has(schemaToRemove));

        // Remove the schema from the metadata store
        root.remove(schemaToRemove);

        // Assert that the schema was removed from the metadata store
        assertFalse(root.has(schemaToRemove));
    }

    @Test
    public void testHasExistingSchema() throws SchemaAlreadyExistsException {
        root.put("test_schema", new SchemaMetadata(testSchema));

        assertTrue(root.has("test_schema"));
    }

    @Test
    public void testHasNonExistingSchema() {
        assertFalse(root.has("test_schema"));
    }

    @Test
    public void testHasRemovedSchema() throws SchemaAlreadyExistsException, SchemaNotExistsException {
        root.put("test_schema", new SchemaMetadata(testSchema));
        root.remove("test_schema");

        assertFalse(root.has("test_schema"));
    }
}