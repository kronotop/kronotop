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

package com.kronotop.bucket.index;

import org.bson.BsonType;

import javax.annotation.Nonnull;

import static com.google.common.hash.Hashing.sipHash24;

/**
 * Represents the definition of an index used in the system. This definition includes the unique
 * identifier of the index, its name, the field it targets, its sort order, and BSON type.
 * <p>
 * The IndexDefinition is designed as an immutable record and provides utility for creating a new
 * instance with an auto-generated identifier based on the name, as well as overrides for standard
 * Object methods like toString and hashCode.
 * <p>
 * Key Components:
 * - id: A unique identifier for the index, derived from the hash of the index name.
 * - name: The name of the index.
 * - field: The field associated with the index.
 * - sortOrder: The sort order (e.g., ascending or descending) of the index.
 * - bsonType: The BSON type of the field being indexed.
 * <p>
 * The main usage scenarios include defining new indexes for a database and providing sufficient
 * metadata for index-related operations.
 */
public record IndexDefinition(long id, String name, String field, SortOrder sortOrder, BsonType bsonType) {

    public static IndexDefinition create(String name, String field, BsonType bsonType, SortOrder sortOrder) {
        long id = sipHash24().hashBytes(name.getBytes()).asLong();
        return new IndexDefinition(id, name, field, sortOrder, bsonType);
    }

    public static IndexDefinition create(String field, BsonType bsonType, SortOrder sortOrder) {
        String name = IndexNameGenerator.generate(field, bsonType, sortOrder);
        return create(name, field, bsonType, sortOrder);
    }

    @Override
    @Nonnull
    public String toString() {
        return "IndexDefinition { name=" + name + ", field=" + field + ", bsonType=" + bsonType + ", sortOrder=" + sortOrder + " }";
    }
}
