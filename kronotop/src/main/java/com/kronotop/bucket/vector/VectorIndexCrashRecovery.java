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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.MutationLogValue;
import com.kronotop.bucket.index.VectorIndexValue;
import com.kronotop.volume.EntryMetadata;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.bson.types.ObjectId;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Recovers on-heap vector graph indexes by replaying the mutation log. Each entry goes to the
 * on-heap index of the generation stored in its key. Nodes that are already on disk are skipped.
 */
public final class VectorIndexCrashRecovery {
    static final int PAGE_SIZE = 1000;

    private VectorIndexCrashRecovery() {
    }

    /**
     * Replays mutation log entries into the on-heap index of the generation stored in each log key.
     * Deletes are applied to every on-heap and on-disk index of the group.
     *
     * @return the generation ids seen in the entries
     */
    static Set<ObjectId> replayMutationLog(
            VectorGraphIndexGroup group,
            List<KeyValue> entries,
            DirectorySubspace indexSubspace,
            int dimensions,
            VectorSimilarityFunction similarityFunction,
            ExecutorService executor,
            int pqTrainingThreshold,
            int pqSubspaceDivisor
    ) {
        Set<ObjectId> generations = new LinkedHashSet<>();
        for (KeyValue kv : entries) {
            MutationLogValue logValue = MutationLogValue.decode(kv.getValue());
            ObjectId objectId = new ObjectId(logValue.objectIdBytes());
            Tuple key = indexSubspace.unpack(kv.getKey());
            byte[] generationId = key.getBytes(1);
            Versionstamp versionstamp = key.getVersionstamp(2);
            generations.add(new ObjectId(generationId));

            switch (logValue.kind()) {
                case INSERT, UPDATE -> {
                    if (group.hasNodeOnDisk(objectId, versionstamp)) {
                        // Flushed before the crash, the log was not truncated yet.
                        continue;
                    }
                    OnHeapVectorGraphIndex onHeap = group.getOrCreateOnHeap(
                            generationId, dimensions, similarityFunction, pqTrainingThreshold, pqSubspaceDivisor);
                    VectorIndexValue payload = logValue.vectorPayload();
                    IndexEntry indexEntry = payload.indexEntry();
                    EntryMetadata metadata = EntryMetadata.decode(indexEntry.entryMetadata());
                    onHeap.addGraphNode(
                            objectId,
                            versionstamp,
                            indexEntry.shardId(),
                            metadata,
                            payload.vector(),
                            executor
                    ).join();
                }
                case DELETE -> group.markNodeDeleted(objectId, versionstamp);
            }
        }
        return generations;
    }

    /**
     * Replays the whole mutation log of the index in pages and queues a flush task for every
     * generation seen in the log, so the flush maintainer writes it to disk and truncates its log.
     */
    public static void recover(
            Context context,
            DirectorySubspace indexSubspace,
            VectorGraphIndexGroup group,
            int dimensions,
            VectorSimilarityFunction similarityFunction,
            ExecutorService executor,
            int pqTrainingThreshold,
            int pqSubspaceDivisor
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.MUTATION_LOG.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        Set<ObjectId> generations = new LinkedHashSet<>();
        while (true) {
            List<KeyValue> page;
            try (Transaction tr = context.getTransactionManager().create()) {
                page = tr.getRange(begin, end, PAGE_SIZE).asList().join();
            }

            generations.addAll(replayMutationLog(group, page, indexSubspace, dimensions, similarityFunction,
                    executor, pqTrainingThreshold, pqSubspaceDivisor));

            if (page.size() < PAGE_SIZE) {
                break;
            }
            begin = KeySelector.firstGreaterThan(page.getLast().getKey());
        }

        for (ObjectId generationId : generations) {
            group.enqueueGenerationForFlush(generationId, false);
        }
    }
}
