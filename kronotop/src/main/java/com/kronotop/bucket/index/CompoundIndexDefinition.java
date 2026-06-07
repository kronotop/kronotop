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

package com.kronotop.bucket.index;

import com.kronotop.bucket.Collation;
import com.kronotop.internal.UUIDUtil;

import java.util.List;
import java.util.UUID;

/**
 * Represents an immutable compound index definition spanning multiple document fields.
 *
 * @param id        unique identifier generated from UUID hash using SipHash24 algorithm
 * @param name      a human-readable index name must be unique within a bucket
 * @param fields    ordered list of fields that make up the compound index (minimum 2)
 * @param status    current operational status of the index
 * @param collation optional collation for locale-aware string ordering, null means inherit bucket-level or binary default
 */
public record CompoundIndexDefinition(long id, String name, List<CompoundIndexField> fields,
                                      IndexStatus status, Collation collation) implements IndexDefinition {

    public CompoundIndexDefinition {
        fields = List.copyOf(fields);
    }

    /**
     * Creates a new compound index definition with the specified status.
     *
     * @param name   the human-readable name for the index
     * @param fields the ordered list of fields (minimum 2)
     * @param status the initial status assigned to the index
     * @return a new CompoundIndexDefinition instance
     * @throws IllegalArgumentException if fewer than 2 fields are provided
     */
    public static CompoundIndexDefinition create(String name, List<CompoundIndexField> fields, IndexStatus status) {
        return create(name, fields, status, null);
    }

    public static CompoundIndexDefinition create(String name, List<CompoundIndexField> fields, IndexStatus status, Collation collation) {
        if (fields.size() < 2) {
            throw new IllegalArgumentException("Compound index requires at least 2 fields, got " + fields.size());
        }
        if (fields.size() > 32) {
            throw new IllegalArgumentException("Compound index supports at most 32 fields, got " + fields.size());
        }
        UUID uuid = UUID.randomUUID();
        long id = UUIDUtil.hash(uuid).asLong();
        return new CompoundIndexDefinition(id, name, fields, status, collation);
    }

    @Override
    public CompoundIndexDefinition updateStatus(IndexStatus status) {
        if (status != IndexStatus.DROPPED && status() == IndexStatus.DROPPED) {
            throw new IllegalStateException("Index '" + name + "' is already dropped and its status cannot be modified.");
        }
        return new CompoundIndexDefinition(id, name, fields, status, collation);
    }
}
