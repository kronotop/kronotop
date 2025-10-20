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

import com.kronotop.NotImplementedException;
import org.bson.BsonType;

import javax.annotation.Nonnull;
import java.util.UUID;

import static com.google.common.hash.Hashing.sipHash24;

/**
 * Represents an immutable index definition for document fields in Kronotop buckets.
 * <p>
 * This record encapsulates all metadata required to create and manage secondary indexes on BSON document
 * fields. Each index definition contains a unique identifier, human-readable name, field selector path,
 * the expected BSON data type of the indexed field, and the current operational status.
 *
 * <h3>Index Identification</h3>
 * Each index is uniquely identified by a cryptographically secure hash-based ID generated from a random UUID.
 * This ensures global uniqueness across all buckets while avoiding collisions.
 *
 * <h3>Field Selection</h3>
 * The selector specifies the document field path to be indexed using dot notation (e.g., "user.profile.age").
 * Only fields matching the specified BSON type will be included in the index.
 *
 * <h3>Index Status Management</h3>
 * The status field tracks the operational state of the index (e.g., READY, BUILDING, FAILED).
 * This enables background index building and status monitoring. Use {@link #updateStatus(IndexStatus)}
 * to create a new instance with updated status while preserving immutability.
 *
 * <h3>Supported Data Types</h3>
 * Most BSON types are supported for indexing, with the exception of DECIMAL128 which is not yet implemented.
 * The type constraint ensures index consistency and enables type-specific optimizations.
 *
 * <h3>Usage Examples</h3>
 * <pre>{@code
 * // Create index with explicit name (defaults to READY status)
 * IndexDefinition userAgeIndex = IndexDefinition.create("user_age_idx", "user.age", BsonType.INT32);
 *
 * // Create index with auto-generated name
 * IndexDefinition emailIndex = IndexDefinition.create("email", BsonType.STRING);
 *
 * // Update index status
 * IndexDefinition buildingIndex = userAgeIndex.updateStatus(IndexStatus.BUILDING);
 * }</pre>
 *
 * <h3>Index Scope</h3>
 * Index definitions are scoped to individual buckets. The same index definition can be safely created
 * on multiple buckets without conflicts, as each bucket maintains its own index namespace.
 *
 * @param id       Unique identifier generated from UUID hash using SipHash24 algorithm
 * @param name     Human-readable index name, must be unique within a bucket
 * @param selector Document field path in dot notation (e.g., "field.subfield")
 * @param bsonType Expected BSON data type of the indexed field values
 * @param status   Current operational status of the index (READY, BUILDING, FAILED, etc.)
 * @see IndexUtil#create(com.apple.foundationdb.Transaction, com.apple.foundationdb.directory.DirectorySubspace, IndexDefinition)
 * @see IndexNameGenerator#generate(String, BsonType)
 * @see IndexStatus
 */
public record IndexDefinition(long id, String name, String selector, BsonType bsonType, IndexStatus status) {

    /**
     * Creates a new index definition with the specified name, selector, and BSON type.
     * <p>
     * This factory method generates a unique identifier using SipHash24 on a random UUID,
     * ensuring cryptographically secure uniqueness across all index definitions.
     * The index is created with {@link IndexStatus#READY} status by default.
     *
     * @param name     the human-readable name for the index, must be unique within a bucket
     * @param selector the document field path to index using dot notation (e.g., "user.email")
     * @param bsonType the expected BSON data type of the indexed field values
     * @return a new IndexDefinition instance with generated unique ID and READY status
     * @throws NotImplementedException if bsonType is DECIMAL128 (not yet supported)
     */
    public static IndexDefinition create(String name, String selector, BsonType bsonType) {
        return create(name, selector, bsonType, IndexStatus.WAITING);
    }

    /**
     * Creates a new index definition with a unique ID, specified name, selector, BSON type, and status.
     * Generates a unique identifier using SipHash24 on a random UUID.
     * Throws an exception if the BSON type is DECIMAL128, as it is not implemented.
     *
     * @param name     the human-readable name for the index, must be unique within a bucket
     * @param selector the document field path to index using dot notation (e.g., "user.address.city")
     * @param bsonType the expected BSON data type of the indexed field values
     * @param status   the initial status assigned to the index
     * @return a new IndexDefinition instance with a unique ID and the specified attributes
     * @throws NotImplementedException if bsonType is DECIMAL128, which is not currently supported
     */
    public static IndexDefinition create(String name, String selector, BsonType bsonType, IndexStatus status) {
        if (bsonType.equals(BsonType.DECIMAL128)) {
            throw new NotImplementedException("Creating indexes on DECIMAL128 fields not implemented yet");
        }
        UUID uuid = UUID.randomUUID();
        long id = sipHash24().hashBytes(uuid.toString().getBytes()).asLong();
        return new IndexDefinition(id, name, selector, bsonType, status);
    }

    /**
     * Creates a new index definition with an auto-generated name based on the selector and BSON type.
     * <p>
     * The index name is automatically generated using {@link IndexNameGenerator#generate(String, BsonType)}
     * to create a consistent naming convention based on the field path and data type.
     * The index is created with {@link IndexStatus#READY} status by default.
     *
     * @param selector the document field path to index using dot notation (e.g., "user.age")
     * @param bsonType the expected BSON data type of the indexed field values
     * @return a new IndexDefinition instance with auto-generated name, unique ID, and READY status
     * @throws NotImplementedException if bsonType is DECIMAL128 (not yet supported)
     * @see IndexNameGenerator#generate(String, BsonType)
     */
    public static IndexDefinition create(String selector, BsonType bsonType) {
        String name = IndexNameGenerator.generate(selector, bsonType);
        return create(name, selector, bsonType);
    }

    /**
     * Creates a new IndexDefinition instance with the specified status while preserving all other fields.
     * <p>
     * This method maintains immutability by creating a new instance rather than modifying the existing one.
     * It is commonly used during index lifecycle management, such as marking an index as BUILDING during
     * background index construction or FAILED if an error occurs.
     * <p>
     * Once an index is marked as {@link IndexStatus#DROPPED}, its status cannot be changed to any other status
     * to prevent accidental reactivation of dropped indexes. However, a dropped index can be updated to DROPPED
     * again without error (idempotent operation).
     *
     * @param status the new status to assign to the index
     * @return a new IndexDefinition instance with the updated status
     * @throws IllegalStateException if the current status is DROPPED and the new status is not DROPPED
     * @see IndexStatus
     */
    public IndexDefinition updateStatus(IndexStatus status) {
        if (status != IndexStatus.DROPPED && status() == IndexStatus.DROPPED) {
            throw new IllegalStateException("Index '" + name + "' is already dropped and its status cannot be modified.");
        }
        return new IndexDefinition(id, name, selector, bsonType, status);
    }

    @Override
    @Nonnull
    public String toString() {
        return "IndexDefinition { id=" + id + ", name=" + name +
                ", selector=" + selector + ", bsonType="
                + bsonType + ", status=" + status + " }";
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }
}
