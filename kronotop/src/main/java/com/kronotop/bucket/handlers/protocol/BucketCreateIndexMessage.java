// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.kronotop.KronotopException;
import com.kronotop.bucket.index.SortOrder;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import org.bson.BsonType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketCreateIndexMessage extends BaseBucketMessage implements ProtocolMessage<Void> {
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
            if (entry.getValue().getSortOrder() == null) {
                throw new KronotopException("'sort_order' cannot be null");
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

        @JsonProperty("sort_order")
        @JsonDeserialize(using = SortOrderDeserializer.class)
        private SortOrder sortOrder;

        IndexDefinition() {
        }

        public SortOrder getSortOrder() {
            return sortOrder;
        }

        public void setSortOrder(SortOrder sortOrder) {
            this.sortOrder = sortOrder;
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

    private static class SortOrderDeserializer extends JsonDeserializer<SortOrder> {
        @Override
        public SortOrder deserialize(JsonParser p, DeserializationContext ignored) throws IOException {
            String sortOrder = p.getValueAsString().toLowerCase();
            return switch (sortOrder) {
                case "asc", "ascending" -> SortOrder.ASCENDING;
                case "desc", "descending" -> SortOrder.DESCENDING;
                default -> throw new IllegalArgumentException("Unknown SortOrder: " + sortOrder);
            };
        }
    }
}
