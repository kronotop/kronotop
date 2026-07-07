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
 * whether the field is multi-key, the current status, and an optional collation.
 *
 * <h3>Multi-key indexes</h3>
 * When {@code multiKey} is true, an array field produces one index entry per array element.
 *
 * <h3>Supported types</h3>
 * All BSON types are supported except DECIMAL128, which is not implemented yet.
 *
 * <h3>Status</h3>
 * The status tracks the index state (READY, BUILDING, FAILED, and so on) and drives
 * background building. Use {@link #updateStatus(IndexStatus)} to derive a new instance
 * with a changed status. A DROPPED index cannot move to any other status.
 *
 * @param id        unique identifier generated from UUID hash using SipHash24 algorithm
 * @param name      index name, must be unique within a bucket
 * @param selector  document field path in dot notation (e.g., "field.subfield")
 * @param bsonType  BSON type of the indexed field values
 * @param multiKey  if true, indexes array elements individually
 * @param status    current index status
 * @param collation optional collation for locale-aware string ordering, null inherits the bucket default or binary ordering
 * @see SingleFieldIndexUtil#create(com.apple.foundationdb.Transaction, com.apple.foundationdb.directory.DirectorySubspace, SingleFieldIndexDefinition)
 * @see IndexNameGenerator#generate(String, BsonType)
 * @see IndexStatus
 */
public record SingleFieldIndexDefinition(long id, String name, String selector, BsonType bsonType, boolean multiKey,
                                         IndexStatus status, Collation collation) implements IndexDefinition {

    /**
     * Creates a new index definition with a unique ID and the given attributes.
     * The ID is a SipHash24 hash of a random UUID.
     *
     * @param name     the human-readable name for the index, must be unique within a bucket
     * @param selector the document field path to index using dot notation (e.g., "user.address.city")
     * @param bsonType the expected BSON data type of the indexed field values
     * @param multiKey if true, array elements are indexed individually
     * @param status   the initial status assigned to the index
     * @return a new IndexDefinition instance with a unique ID and the specified attributes
     * @throws NotImplementedException if bsonType is DECIMAL128, which is not currently supported
     */
    public static SingleFieldIndexDefinition create(String name, String selector, BsonType bsonType, boolean multiKey, IndexStatus status) {
        return create(name, selector, bsonType, multiKey, status, null);
    }

    public static SingleFieldIndexDefinition create(String name, String selector, BsonType bsonType, boolean multiKey, IndexStatus status, Collation collation) {
        if (bsonType.equals(BsonType.DECIMAL128)) {
            throw new NotImplementedException("Creating indexes on DECIMAL128 fields not implemented yet");
        }
        UUID uuid = UUID.randomUUID();
        long id = UUIDUtil.hash(uuid).asLong();
        return new SingleFieldIndexDefinition(id, name, selector, bsonType, multiKey, status, collation);
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
        return new SingleFieldIndexDefinition(id, name, selector, bsonType, multiKey, status, collation);
    }

    @Override
    @Nonnull
    public String toString() {
        return "IndexDefinition { id=" + id + ", name=" + name +
                ", selector=" + selector + ", bsonType=" + bsonType +
                ", multiKey=" + multiKey + ", status=" + status +
                ", collation=" + collation + " }";
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }
}
