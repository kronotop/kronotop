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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import org.bson.types.ObjectId;

import java.util.UUID;

/**
 * A vector node operation that failed on the graph indexes and is kept for a later retry.
 * Holds the fields of an ADD or a DELETE. Only an ADD carries a {@code collectedVector}.
 *
 * @param namespace       the namespace of the bucket
 * @param bucket          the bucket name
 * @param bucketUuid      the bucket UUID at the time of the operation, used to skip the entry if the bucket UUID changes
 * @param versionstamp    the versionstamp of the operation
 * @param kind            ADD or DELETE
 * @param objectId        the object id of the vector node
 * @param vectorIndexId   the id of the vector index
 * @param collectedVector the vector and document metadata to add again, null for DELETE
 */
public record RetryEntry(
        String namespace,
        String bucket,
        UUID bucketUuid,
        Versionstamp versionstamp,
        Kind kind,
        ObjectId objectId,
        long vectorIndexId,
        CollectedVector collectedVector) {

    /**
     * Creates a retry entry for a failed add.
     */
    public static RetryEntry add(BucketMetadata metadata, Versionstamp versionstamp, CollectedVector cv) {
        return new RetryEntry(metadata.namespace(), metadata.name(), metadata.uuid(), versionstamp,
                Kind.ADD, cv.objectId(), cv.vectorIndexId(), cv);
    }

    /**
     * Creates a retry entry for a failed delete.
     */
    public static RetryEntry delete(BucketMetadata metadata, Versionstamp versionstamp, long vectorIndexId, ObjectId objectId) {
        return new RetryEntry(metadata.namespace(), metadata.name(), metadata.uuid(), versionstamp,
                Kind.DELETE, objectId, vectorIndexId, null);
    }

    public boolean isAdd() {
        return kind == Kind.ADD;
    }

    public boolean isDelete() {
        return kind == Kind.DELETE;
    }

    /**
     * The kind of the failed operation.
     */
    public enum Kind {
        ADD,
        DELETE
    }
}
