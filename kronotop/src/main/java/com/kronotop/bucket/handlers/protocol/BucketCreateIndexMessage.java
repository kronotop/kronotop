/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.handlers.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.kronotop.KronotopException;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import org.bson.BsonType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketCreateIndexMessage extends AbstractBucketMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "BUCKET.CREATE-INDEX";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    private final Request request;
    private String bucket;
    private HashMap<String, IndexDefinition> definitions;

    public BucketCreateIndexMessage(Request request) {
        this.request = request;
        parse();
        validate();
    }

    private void validate() {
        if (definitions.isEmpty()) {
            throw new KronotopException("Index definitions cannot be empty");
        }
        for (Map.Entry<String, BucketCreateIndexMessage.IndexDefinition> entry : definitions.entrySet()) {
            if (entry.getValue().getBsonType() == null) {
                throw new KronotopException("'bson_type' cannot be null");
            }
        }
    }

    private void parse() {
        bucket = ProtocolMessageUtil.readAsString(request.getParams().get(0));
        try {
            definitions = JSONUtil.readValue(
                    ProtocolMessageUtil.readAsByteArray(request.getParams().get(1)),
                    IndexDefinitions.class
            );
        } catch (KronotopException e) {
            if (e.getCause() instanceof JsonMappingException jsonException) {
                if (jsonException.getCause() instanceof IllegalArgumentException illegalArgumentException) {
                    throw new KronotopException(illegalArgumentException);
                }
            }
            throw new KronotopException("Invalid index definition");
        }
    }

    public Map<String, IndexDefinition> getDefinitions() {
        return definitions;
    }

    public String getBucket() {
        return bucket;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return List.of();
    }

    public static class IndexDefinitions extends HashMap<String, IndexDefinition> {

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IndexDefinition {
        private String name;

        @JsonProperty("bson_type")
        @JsonDeserialize(using = BsonTypeDeserializer.class)
        private BsonType bsonType;


        IndexDefinition() {
        }

        public BsonType getBsonType() {
            return bsonType;
        }

        public void setBsonType(BsonType bsonType) {
            this.bsonType = bsonType;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private static class BsonTypeDeserializer extends JsonDeserializer<BsonType> {
        @Override
        public BsonType deserialize(JsonParser p, DeserializationContext ignored) throws IOException {
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
                default -> throw new IllegalArgumentException("Unknown BSON type: " + type);
            };
        }
    }

}
