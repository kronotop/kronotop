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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.MutationLogValue;
import com.kronotop.bucket.index.VectorIndexValue;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Recovers an on-heap vector graph index by replaying mutation log entries that occurred
 * after the last flushed on-disk snapshot.
 */
public final class VectorIndexCrashRecovery {

    private VectorIndexCrashRecovery() {
    }

    /**
     * Scans the mutation log for entries newer than the latest on-disk versionstamp and
     * replays them into a fresh on-heap index with PQ configuration.
     *
     * @return a recovered on-heap index, or null if the mutation log has no applicable entries
     */
    public static OnHeapVectorGraphIndex recover(
            Database db,
            DirectorySubspace indexSubspace,
            VectorGraphIndexGroup group,
            int dimensions,
            VectorSimilarityFunction similarityFunction,
            ExecutorService executor,
            int pqTrainingThreshold,
            int pqSubspaceDivisor
    ) {
        // Find max versionstamp across all on-disk indexes
        Versionstamp maxVersionstamp = null;
        for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
            Versionstamp vs = onDisk.getLatestVersionstamp();
            if (vs != null && (maxVersionstamp == null || vs.compareTo(maxVersionstamp) > 0)) {
                maxVersionstamp = vs;
            }
        }

        // Build range scan boundaries
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
        KeySelector begin;
        if (maxVersionstamp != null) {
            byte[] startKey = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue(), maxVersionstamp));
            begin = KeySelector.firstGreaterThan(startKey);
        } else {
            begin = KeySelector.firstGreaterOrEqual(prefix);
        }
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        // Read mutation log entries
        List<KeyValue> entries;
        try (Transaction tr = db.createTransaction()) {
            entries = tr.getRange(begin, end).asList().join();
        }

        if (entries.isEmpty()) {
            return null;
        }

        // Create a fresh on-heap index and replay entries
        OnHeapVectorGraphIndex onHeap = new OnHeapVectorGraphIndex(dimensions, similarityFunction,
                pqTrainingThreshold, pqSubspaceDivisor);

        for (KeyValue kv : entries) {
            MutationLogValue logValue = MutationLogValue.decode(kv.getValue());
            ObjectId objectId = new ObjectId(logValue.objectIdBytes());
            Versionstamp versionstamp = indexSubspace.unpack(kv.getKey()).getVersionstamp(1);

            switch (logValue.marker()) {
                case INSERT, UPDATE -> {
                    VectorIndexValue payload = logValue.vectorPayload();
                    IndexEntry indexEntry = payload.indexEntry();
                    onHeap.addGraphNode(
                            objectId,
                            indexEntry.shardId(),
                            com.kronotop.volume.EntryMetadata.decode(indexEntry.entryMetadata()),
                            payload.vector(),
                            executor
                    ).join();
                }
                case DELETE -> {
                    int ordinal = onHeap.getMetadata().findOrdinal(objectId);
                    if (ordinal >= 0) {
                        onHeap.markNodeDeleted(objectId, ordinal);
                    }
                    for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
                        int diskOrdinal = onDisk.getMetadata().findOrdinal(objectId);
                        if (diskOrdinal >= 0) {
                            onDisk.markNodeDeleted(diskOrdinal);
                        }
                    }
                }
            }

            onHeap.advanceVersionstamp(versionstamp);
        }

        // Flush any on-disk metadata changes made during recovery
        for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
            onDisk.flushMetadata();
        }

        return onHeap;
    }
}
