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

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.CollatorCache;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

// The background index builder determines its scan boundaries from the primary index VOLUME_POINTER
// subspace: the first entry is the inclusive lower bound and the last entry (reverse range read) gives
// the exclusive upper bound. The build always covers the full range and re-sets entries idempotently.

public class IndexMaintainer {
    /**
     * Placeholder value for back pointer entries.
     */
    protected final static byte[] NULL_VALUE = new byte[]{0};

    public static Collation resolveCollation(IndexDefinition definition, BucketMetadata metadata) {
        Collation c = definition.collation();
        return c != null ? c : metadata.collation();
    }

    public static Object applyCollation(Object value, Collation collation, CollatorCache cache) {
        if (value instanceof String str && collation != null) {
            return cache.acquire(collation).getCollationKey(str).toByteArray();
        }
        return value;
    }

    /**
     * Returns which of the given ObjectIds already have at least one back pointer in this index.
     *
     * <p>Issues one point request per ObjectId against the {@code (BACK_POINTER, ObjectId, ...)}
     * keyspace. This layout is shared by single field and compound indexes, so the probe is index
     * type agnostic. All requests are dispatched before any is awaited, so the FoundationDB client
     * pipelines them over a single connection, giving a few round trips instead of one per ObjectId.
     * Each read is not a snapshot read, so it registers a narrow read conflict range over exactly that
     * ObjectId's back pointers. A concurrent writer that adds or removes an entry for one of these
     * ObjectIds forces this transaction to conflict on commit. This lets the background index builder
     * skip ObjectIds already indexed by online writers without double counting cardinality.
     *
     * @param tr            the FoundationDB transaction
     * @param indexSubspace the index's directory subspace
     * @param objectIds     the ObjectIds to probe, each as bytes
     * @return the subset of the given ObjectIds that are already indexed
     */
    public static Set<ObjectId> findIndexedObjectIds(
            Transaction tr,
            DirectorySubspace indexSubspace,
            List<byte[]> objectIds
    ) {
        if (objectIds.isEmpty()) {
            return Set.of();
        }

        // Dispatch all point requests without awaiting so the client pipelines them.
        List<CompletableFuture<List<KeyValue>>> futures = new ArrayList<>(objectIds.size());
        for (byte[] objectIdBytes : objectIds) {
            byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.BACK_POINTER.getValue(), objectIdBytes));
            futures.add(tr.getRange(prefix, ByteArrayUtil.strinc(prefix), 1).asList());
        }

        Set<ObjectId> indexed = new HashSet<>();
        for (int i = 0; i < objectIds.size(); i++) {
            if (!futures.get(i).join().isEmpty()) {
                indexed.add(new ObjectId(objectIds.get(i)));
            }
        }
        return indexed;
    }
}
