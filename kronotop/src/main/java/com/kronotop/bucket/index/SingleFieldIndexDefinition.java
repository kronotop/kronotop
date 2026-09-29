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
 * Immutable definition of a single field index on a BSON document field.
 * <p>
 * Holds the metadata needed to create and manage the index: a unique id, a name that is
 * unique within the bucket, the field selector in dot notation, the indexed BSON type,
 * whether the field is multi-key, the current status, an optional collation, and whether
 * the index is unique.
 *
 * <p>
 * When {@code multiKey} is true, an array field produces one index entry per array element.
 * A unique index cannot be multi-key.
 * Indexes on DECIMAL128 fields are not implemented yet, so {@code create} rejects them.
 * <p>
 * Use {@link #updateStatus(IndexStatus)} to derive a new instance with a changed status.
 * A DROPPED index cannot move to any other status.
 *
 * @param id        unique identifier, a SipHash24 hash of a random UUID
 * @param name      index name, must be unique within a bucket
 * @param selector  document field path in dot notation (e.g., "field.subfield")
 * @param bsonType  BSON type of the indexed field values
 * @param multiKey  if true, indexes array elements individually
 * @param status    current index status
 * @param collation optional collation for locale-aware string ordering, null inherits the bucket default or binary ordering
 * @param unique    if true, the index enforces unique values; cannot be combined with {@code multiKey}
 * @see SingleFieldIndexUtil#create(com.apple.foundationdb.Transaction, com.apple.foundationdb.directory.DirectorySubspace, SingleFieldIndexDefinition)
 * @see IndexNameGenerator#generate(String, BsonType)
 * @see IndexStatus
 */
public record SingleFieldIndexDefinition(long id, String name, String selector, BsonType bsonType, boolean multiKey,
                                         IndexStatus status, Collation collation,
                                         boolean unique) implements IndexDefinition {

    public SingleFieldIndexDefinition {
        // Uniqueness reads assume a document contributes at most one entry per value. A multi-key
        // index would break that invariant, so it must never combine with unique. Enforced here in
        // the canonical constructor so no path (factory, direct construction, deserialization) bypasses it.
        if (unique && multiKey) {
            throw new IllegalArgumentException("A unique index cannot be multi-key");
        }
    }

    /**
     * Creates a new index definition with a new unique ID.
     *
     * @param name     index name, must be unique within a bucket
     * @param selector document field path in dot notation (e.g., "user.address.city")
     * @param bsonType BSON type of the indexed field values
     * @param multiKey if true, indexes array elements individually
     * @param status   initial index status
     * @return a new definition with the given attributes
     * @throws NotImplementedException if bsonType is DECIMAL128
     */
    public static SingleFieldIndexDefinition create(String name, String selector, BsonType bsonType, boolean multiKey, IndexStatus status) {
        return create(name, selector, bsonType, multiKey, status, null);
    }

    public static SingleFieldIndexDefinition create(String name, String selector, BsonType bsonType, boolean multiKey, IndexStatus status, Collation collation) {
        return create(name, selector, bsonType, multiKey, status, collation, false);
    }

    public static SingleFieldIndexDefinition create(String name, String selector, BsonType bsonType, boolean multiKey, IndexStatus status, Collation collation, boolean unique) {
        if (bsonType.equals(BsonType.DECIMAL128)) {
            throw new NotImplementedException("Creating indexes on DECIMAL128 fields not implemented yet");
        }
        UUID uuid = UUID.randomUUID();
        long id = UUIDUtil.hash(uuid).asLong();
        return new SingleFieldIndexDefinition(id, name, selector, bsonType, multiKey, status, collation, unique);
    }

    /**
     * Returns a new instance with the given status, keeping all other fields.
     * Used during index lifecycle changes, such as marking an index BUILDING while it is
     * built in the background, or FAILED on error.
     * <p>
     * A {@link IndexStatus#DROPPED} index cannot move to any other status. Setting DROPPED
     * again on a dropped index is allowed and does nothing.
     *
     * @param status the new status to assign to the index
     * @return a new instance with the updated status
     * @throws IllegalStateException if the current status is DROPPED and the new status is not DROPPED
     * @see IndexStatus
     */
    public SingleFieldIndexDefinition updateStatus(IndexStatus status) {
        if (status != IndexStatus.DROPPED && status() == IndexStatus.DROPPED) {
            throw new IllegalStateException("Index '" + name + "' is already dropped and its status cannot be modified.");
        }
        return new SingleFieldIndexDefinition(id, name, selector, bsonType, multiKey, status, collation, unique);
    }

    @Override
    @Nonnull
    public String toString() {
        return "IndexDefinition { id=" + id + ", name=" + name +
                ", selector=" + selector + ", bsonType=" + bsonType +
                ", multiKey=" + multiKey + ", status=" + status +
                ", collation=" + collation + ", unique=" + unique + " }";
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }
}
