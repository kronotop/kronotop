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

import com.kronotop.NotImplementedException;
import com.kronotop.bucket.Collation;
import com.kronotop.internal.UUIDUtil;
import org.bson.BsonType;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * Describes a single field index on a BSON document field. Instances are immutable.
 * When {@code multiKey} is true, an array field produces one index entry per array element.
 * A unique index cannot be multi-key.
 *
 * @param id            unique identifier of the index
 * @param name          index name, must be unique within the bucket
 * @param selector      field path in dot notation, for example "field.subfield"
 * @param bsonType      BSON type of the indexed values
 * @param sortDirection order of the index entries, ascending or descending
 * @param nullPosition  place of null values in the index order, first or last
 * @param multiKey      true to index each array element separately
 * @param status        current index status
 * @param collation     collation for locale-aware string ordering, or null to use the bucket default
 *                      or binary ordering
 * @param unique        true to enforce unique values, cannot be combined with {@code multiKey}
 * @see SingleFieldIndexUtil#create(com.apple.foundationdb.Transaction, com.apple.foundationdb.directory.DirectorySubspace, SingleFieldIndexDefinition)
 * @see IndexNameGenerator#generate(String, BsonType)
 * @see IndexStatus
 */
public record SingleFieldIndexDefinition(long id,
                                         String name,
                                         String selector,
                                         BsonType bsonType,
                                         SortDirection sortDirection,
                                         NullPosition nullPosition,
                                         boolean multiKey,
                                         IndexStatus status,
                                         Collation collation,
                                         boolean unique)
        implements IndexDefinition {

    public SingleFieldIndexDefinition {
        // Uniqueness reads assume a document contributes at most one entry per value. A multi-key
        // index would break that invariant, so it must never combine with unique. Enforced here in
        // the canonical constructor so no path (factory, direct construction, deserialization) bypasses it.
        if (unique && multiKey) {
            throw new IllegalArgumentException("A unique index cannot be multi-key");
        }
    }

    public static SingleFieldIndexDefinition create(String name,
                                                    String selector,
                                                    BsonType bsonType,
                                                    SortDirection sortDirection,
                                                    NullPosition nullPosition,
                                                    boolean multiKey,
                                                    IndexStatus status
    ) {
        return create(name, selector, bsonType, sortDirection, nullPosition, multiKey, status, null);
    }

    public static SingleFieldIndexDefinition create(String name,
                                                    String selector,
                                                    BsonType bsonType,
                                                    SortDirection sortDirection,
                                                    NullPosition nullPosition,
                                                    boolean multiKey,
                                                    IndexStatus status,
                                                    Collation collation
    ) {
        return create(name, selector, bsonType, sortDirection, nullPosition, multiKey, status, collation, false);
    }

    /**
     * Creates an index definition with a newly generated id.
     *
     * @param name          index name, must be unique within the bucket
     * @param selector      field path in dot notation, for example "user.address.city"
     * @param bsonType      BSON type of the indexed values
     * @param sortDirection order of the index entries, ascending or descending
     * @param nullPosition  place of null values in the index order, first or last
     * @param multiKey      true to index each array element separately
     * @param status        initial index status
     * @param collation     collation for locale-aware string ordering, or null to use the bucket default
     *                      or binary ordering
     * @param unique        true to enforce unique values
     * @return the new definition
     * @throws NotImplementedException  if {@code bsonType} is DECIMAL128
     * @throws IllegalArgumentException if both {@code unique} and {@code multiKey} are true
     */
    public static SingleFieldIndexDefinition create(String name,
                                                    String selector,
                                                    BsonType bsonType,
                                                    SortDirection sortDirection,
                                                    NullPosition nullPosition,
                                                    boolean multiKey,
                                                    IndexStatus status,
                                                    Collation collation,
                                                    boolean unique) {
        if (bsonType.equals(BsonType.DECIMAL128)) {
            throw new NotImplementedException("Creating indexes on DECIMAL128 fields not implemented yet");
        }
        UUID uuid = UUID.randomUUID();
        long id = UUIDUtil.hash(uuid).asLong();
        return new SingleFieldIndexDefinition(id, name, selector, bsonType, sortDirection,
                nullPosition, multiKey, status, collation, unique);
    }

    /**
     * Returns a copy of this definition with the given status.
     *
     * @param status the new status
     * @return a new definition with the given status
     * @throws IllegalStateException if this index is DROPPED and {@code status} is not DROPPED
     * @see IndexStatus
     */
    public SingleFieldIndexDefinition updateStatus(IndexStatus status) {
        if (status != IndexStatus.DROPPED && status() == IndexStatus.DROPPED) {
            throw new IllegalStateException("Index '" + name + "' is already dropped and its status cannot be modified");
        }
        return new SingleFieldIndexDefinition(id, name, selector, bsonType, sortDirection,
                nullPosition, multiKey, status, collation, unique);
    }

    @Override
    @Nonnull
    public String toString() {
        return "IndexDefinition { id=" + id + ", name=" + name +
                ", selector=" + selector + ", bsonType=" + bsonType +
                ", sortDirection=" + sortDirection + ", nullPosition=" + nullPosition +
                ", multiKey=" + multiKey + ", status=" + status +
                ", collation=" + collation + ", unique=" + unique + " }";
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }
}
