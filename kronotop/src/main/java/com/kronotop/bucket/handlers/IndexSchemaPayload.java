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

import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.annotation.JsonDeserialize;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deserialization target for index schemas that supports both single-field indexes
 * and compound indexes via the {@code $compound} directive.
 */
@JsonDeserialize(using = IndexSchemaPayload.Deserializer.class)
class IndexSchemaPayload {
    private final Map<String, IndexSchema> singleFieldSchemas;
    private final List<CompoundIndexSchema> compoundSchemas;
    private final VectorIndexSchema vectorSchema;

    IndexSchemaPayload(Map<String, IndexSchema> singleFieldSchemas, List<CompoundIndexSchema> compoundSchemas, VectorIndexSchema vectorSchema) {
        this.singleFieldSchemas = singleFieldSchemas;
        this.compoundSchemas = compoundSchemas;
        this.vectorSchema = vectorSchema;
    }

    public Map<String, IndexSchema> getSingleFieldSchemas() {
        return singleFieldSchemas;
    }

    public List<CompoundIndexSchema> getCompoundSchemas() {
        return compoundSchemas;
    }

    public VectorIndexSchema getVectorSchema() {
        return vectorSchema;
    }

    static class Deserializer extends ValueDeserializer<IndexSchemaPayload> {
        @Override
        public IndexSchemaPayload deserialize(JsonParser p, DeserializationContext ctxt) {
            Map<String, IndexSchema> singleFieldSchemas = new LinkedHashMap<>();
            List<CompoundIndexSchema> compoundSchemas = null;
            VectorIndexSchema vectorSchema = null;

            if (p.currentToken() != JsonToken.START_OBJECT) {
                p.nextToken();
            }

            while (p.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = p.currentName();
                p.nextToken();

                if (fieldName.startsWith("$")) {
                    if ("$compound".equals(fieldName)) {
                        compoundSchemas = parseCompoundArray(p, ctxt);
                    } else if ("$vector".equals(fieldName)) {
                        vectorSchema = ctxt.readValue(p, VectorIndexSchema.class);
                    } else {
                        throw new IllegalArgumentException("Unknown directive: " + fieldName);
                    }
                } else {
                    IndexSchema schema = ctxt.readValue(p, IndexSchema.class);
                    singleFieldSchemas.put(fieldName, schema);
                }
            }

            return new IndexSchemaPayload(singleFieldSchemas, compoundSchemas, vectorSchema);
        }

        private List<CompoundIndexSchema> parseCompoundArray(JsonParser p, DeserializationContext ctxt) {
            if (p.currentToken() != JsonToken.START_ARRAY) {
                throw new IllegalArgumentException("$compound value must be an array");
            }

            List<CompoundIndexSchema> schemas = new ArrayList<>();
            while (p.nextToken() != JsonToken.END_ARRAY) {
                CompoundIndexSchema schema = ctxt.readValue(p, CompoundIndexSchema.class);
                schemas.add(schema);
            }

            if (schemas.isEmpty()) {
                throw new IllegalArgumentException("$compound array must not be empty");
            }

            return schemas;
        }
    }
}
