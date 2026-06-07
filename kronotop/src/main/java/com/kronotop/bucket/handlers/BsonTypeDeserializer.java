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

import org.bson.BsonType;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

public class BsonTypeDeserializer extends ValueDeserializer<BsonType> {
    @Override
    public BsonType deserialize(JsonParser p, DeserializationContext ignored) {
        String type = p.getValueAsString().toLowerCase();
        return switch (type) {
            case "double" -> BsonType.DOUBLE;
            case "string" -> BsonType.STRING;
            case "binary" -> BsonType.BINARY;
            case "boolean" -> BsonType.BOOLEAN;
            case "datetime" -> BsonType.DATE_TIME;
            case "int32" -> BsonType.INT32;
            case "timestamp" -> BsonType.TIMESTAMP;
            case "int64" -> BsonType.INT64;
            case "decimal128" -> BsonType.DECIMAL128;
            case "objectid" -> BsonType.OBJECT_ID;
            default -> throw new IllegalArgumentException("Unknown BSON type: " + type);
        };
    }
}