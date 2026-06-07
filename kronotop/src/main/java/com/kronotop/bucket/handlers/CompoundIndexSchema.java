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

import com.kronotop.KronotopException;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollationTypeMismatchException;
import org.bson.BsonType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Jackson POJO representing one compound index entry from the {@code $compound} array.
 */
class CompoundIndexSchema {
    private String name;
    private List<CompoundFieldSchema> fields;
    private Collation collation;

    CompoundIndexSchema() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<CompoundFieldSchema> getFields() {
        return fields;
    }

    public void setFields(List<CompoundFieldSchema> fields) {
        this.fields = fields;
    }

    public Collation getCollation() {
        return collation;
    }

    public void setCollation(Collation collation) {
        this.collation = collation;
    }

    public void validate() {
        if (fields == null || fields.size() < 2) {
            throw new KronotopException("Compound index requires at least 2 fields");
        }
        if (fields.size() > 32) {
            throw new KronotopException("Compound index supports at most 32 fields");
        }

        Set<String> selectors = new HashSet<>();
        int multiKeyCount = 0;
        for (CompoundFieldSchema field : fields) {
            if (field.getSelector() == null || field.getSelector().isEmpty()) {
                throw new KronotopException("Each compound index field must have a 'selector'");
            }
            if (field.getBsonType() == null) {
                throw new KronotopException("Each compound index field must have a 'bson_type'");
            }
            if (!selectors.add(field.getSelector())) {
                throw new KronotopException("Duplicate selector in compound index: '" + field.getSelector() + "'");
            }
            if (field.getMultiKey()) {
                multiKeyCount++;
            }
        }
        if (multiKeyCount > 1) {
            throw new KronotopException("Compound index allows at most one multi-key field");
        }

        if (collation != null) {
            boolean hasStringField = fields.stream().anyMatch(f -> f.getBsonType() == BsonType.STRING);
            if (!hasStringField) {
                throw CollationTypeMismatchException.compoundIndex();
            }
            collation.validate();
        }
    }
}
