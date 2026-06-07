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

import com.kronotop.bucket.index.DistanceFunction;
import com.kronotop.internal.JSONUtil;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class IndexSchemaPayloadTest {

    private IndexSchemaPayload parse(String json) {
        return JSONUtil.readValue(json.getBytes(StandardCharsets.UTF_8), IndexSchemaPayload.class);
    }

    @Test
    void shouldParseSingleFieldOnly() {
        // Behavior: A payload with only single-field indexes parses correctly.
        IndexSchemaPayload payload = parse("{\"username\": {\"bson_type\": \"string\"}, \"age\": {\"bson_type\": \"int32\"}}");
        assertEquals(2, payload.getSingleFieldSchemas().size());
        assertNull(payload.getCompoundSchemas());
        assertEquals(BsonType.STRING, payload.getSingleFieldSchemas().get("username").getBsonType());
        assertEquals(BsonType.INT32, payload.getSingleFieldSchemas().get("age").getBsonType());
    }

    @Test
    void shouldParseCompoundOnly() {
        // Behavior: A payload with only a $compound directive parses correctly.
        String json = """
                {"$compound": [{"fields": [{"selector": "category", "bson_type": "string"}, {"selector": "price", "bson_type": "double"}]}]}
                """;
        IndexSchemaPayload payload = parse(json);
        assertTrue(payload.getSingleFieldSchemas().isEmpty());
        assertNotNull(payload.getCompoundSchemas());
        assertEquals(1, payload.getCompoundSchemas().size());
        assertEquals(2, payload.getCompoundSchemas().get(0).getFields().size());
    }

    @Test
    void shouldParseMixed() {
        // Behavior: A payload with both single-field and compound indexes parses correctly.
        String json = """
                {
                    "username": {"bson_type": "string"},
                    "$compound": [{"fields": [{"selector": "a", "bson_type": "string"}, {"selector": "b", "bson_type": "int32"}]}]
                }
                """;
        IndexSchemaPayload payload = parse(json);
        assertEquals(1, payload.getSingleFieldSchemas().size());
        assertNotNull(payload.getCompoundSchemas());
        assertEquals(1, payload.getCompoundSchemas().size());
    }

    @Test
    void shouldRejectUnknownDollarDirective() {
        // Behavior: Unknown $ directives are rejected with an error.
        String json = "{\"$unknown\": []}";
        Exception ex = assertThrows(Exception.class, () -> parse(json));
        assertTrue(ex.getMessage().contains("Unknown directive: $unknown") ||
                (ex.getCause() != null && ex.getCause().getMessage().contains("Unknown directive: $unknown")));
    }

    @Test
    void shouldRejectEmptyCompoundArray() {
        // Behavior: An empty $compound array is rejected.
        String json = "{\"$compound\": []}";
        Exception ex = assertThrows(Exception.class, () -> parse(json));
        assertTrue(ex.getMessage().contains("$compound array must not be empty") ||
                (ex.getCause() != null && ex.getCause().getMessage().contains("$compound array must not be empty")));
    }

    @Test
    void shouldParseCompoundWithName() {
        // Behavior: A compound schema with an explicit name is preserved.
        String json = """
                {"$compound": [{"name": "my_idx", "fields": [{"selector": "a", "bson_type": "string"}, {"selector": "b", "bson_type": "int32"}]}]}
                """;
        IndexSchemaPayload payload = parse(json);
        assertEquals("my_idx", payload.getCompoundSchemas().get(0).getName());
    }

    @Test
    void shouldParseVectorSchema() {
        // Behavior: A payload with a $vector directive parses correctly.
        String json = """
                {"$vector": {"field": "embedding", "dimensions": 1536, "distance": "cosine"}}
                """;
        IndexSchemaPayload payload = parse(json);
        assertTrue(payload.getSingleFieldSchemas().isEmpty());
        assertNull(payload.getCompoundSchemas());
        assertNotNull(payload.getVectorSchema());
        assertEquals("embedding", payload.getVectorSchema().getField());
        assertEquals(1536, payload.getVectorSchema().getDimensions());
        assertEquals(DistanceFunction.COSINE, payload.getVectorSchema().getDistance());
    }

    @Test
    void shouldParseVectorSchemaWithName() {
        // Behavior: A vector schema with an explicit name is preserved.
        String json = """
                {"$vector": {"name": "my_vec_idx", "field": "embedding", "dimensions": 1536, "distance": "euclidean"}}
                """;
        IndexSchemaPayload payload = parse(json);
        assertNotNull(payload.getVectorSchema());
        assertEquals("my_vec_idx", payload.getVectorSchema().getName());
        assertEquals(DistanceFunction.EUCLIDEAN, payload.getVectorSchema().getDistance());
    }

    @Test
    void shouldParseCompoundWithMultiKey() {
        // Behavior: A compound field with multi_key set to true is preserved.
        String json = """
                {"$compound": [{"fields": [{"selector": "tags", "bson_type": "string", "multi_key": true}, {"selector": "price", "bson_type": "double"}]}]}
                """;
        IndexSchemaPayload payload = parse(json);
        assertTrue(payload.getCompoundSchemas().get(0).getFields().get(0).getMultiKey());
        assertFalse(payload.getCompoundSchemas().get(0).getFields().get(1).getMultiKey());
    }
}
