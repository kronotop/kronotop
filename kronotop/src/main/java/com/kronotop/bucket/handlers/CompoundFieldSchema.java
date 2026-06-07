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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.BsonType;
import tools.jackson.databind.annotation.JsonDeserialize;

/**
 * Jackson POJO representing a single field entry in a compound index definition.
 */
class CompoundFieldSchema {
    @JsonProperty("selector")
    private String selector;

    @JsonProperty("bson_type")
    @JsonDeserialize(using = BsonTypeDeserializer.class)
    private BsonType bsonType;

    @JsonProperty("multi_key")
    private boolean multiKey;

    CompoundFieldSchema() {
    }

    public String getSelector() {
        return selector;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public BsonType getBsonType() {
        return bsonType;
    }

    public void setBsonType(BsonType bsonType) {
        this.bsonType = bsonType;
    }

    public boolean getMultiKey() {
        return multiKey;
    }

    public void setMultiKey(boolean multiKey) {
        this.multiKey = multiKey;
    }
}
