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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kronotop.KronotopException;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollationTypeMismatchException;
import org.bson.BsonType;
import tools.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexSchema {
    private String name;

    @JsonProperty("bson_type")
    @JsonDeserialize(using = BsonTypeDeserializer.class)
    private BsonType bsonType;

    /**
     * When true, creates a multi-key index for array fields. Each array element
     * generates a separate index entry. Note: result ordering is undefined with
     * multi-key indexes.
     */
    @JsonProperty("multi_key")
    private boolean multiKey;

    private Collation collation;

    IndexSchema() {
    }

    public BsonType getBsonType() {
        return bsonType;
    }

    public void setBsonType(BsonType bsonType) {
        this.bsonType = bsonType;
    }

    public Boolean getMultiKey() {
        return multiKey;
    }

    public void setMultiKey(boolean multiKey) {
        this.multiKey = multiKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Collation getCollation() {
        return collation;
    }

    public void setCollation(Collation collation) {
        this.collation = collation;
    }

    @JsonIgnore
    public void validate() {
        if (getBsonType() == null) {
            throw new KronotopException("'bson_type' cannot be null");
        }
        if (collation != null) {
            if (bsonType != BsonType.STRING) {
                throw CollationTypeMismatchException.singleField();
            }
            collation.validate();
        }
    }
}