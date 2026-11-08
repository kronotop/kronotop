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
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.bucket.index.VectorIndexMaintainer;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Queues a sync task for every flushed vector graph that never reached Volume. A graph whose files are
 * gone from the local disk cannot be synced and is skipped.
 */
public final class VectorIndexSyncBacklog {
    static final int PAGE_SIZE = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexSyncBacklog.class);

    private VectorIndexSyncBacklog() {
    }

    /**
     * Queues a sync task for every flushed graph of the index that is not synced and still complete on disk.
     *
     * @param context       the server context
     * @param metadata      the metadata of the bucket that owns the index
     * @param vectorIndex   the vector index
     * @param bucketDataDir the root of the bucket data directory
     * @return the number of sync tasks queued
     */
    public static int enqueue(
            Context context,
            BucketMetadata metadata,
            VectorIndex vectorIndex,
            Path bucketDataDir
    ) {
        BucketService service = context.getService(BucketService.NAME);
        long indexId = vectorIndex.definition().id();
        Path indexDir = VectorGraphIndexGroup.resolveIndexDir(bucketDataDir, metadata, indexId);
        DirectorySubspace indexSubspace = vectorIndex.subspace();

        byte[] flushedGraphsRange = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.GRAPH_FLUSHED.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(flushedGraphsRange);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(flushedGraphsRange));
        int queued = 0;
        while (true) {
            List<KeyValue> page;
            List<ObjectId> unsynced = new ArrayList<>();
            try (Transaction tr = context.getTransactionManager().create()) {
                page = tr.getRange(begin, end, PAGE_SIZE).asList().join();
                List<ObjectId> graphIds = new ArrayList<>(page.size());
                List<CompletableFuture<byte[]>> syncedKeys = new ArrayList<>(page.size());
                for (KeyValue kv : page) {
                    ObjectId graphId = new ObjectId(indexSubspace.unpack(kv.getKey()).getBytes(1));
                    graphIds.add(graphId);
                    syncedKeys.add(tr.get(VectorIndexMaintainer.graphSyncedKey(indexSubspace, graphId)));
                }
                for (int i = 0; i < graphIds.size(); i++) {
                    if (syncedKeys.get(i).join() == null) {
                        unsynced.add(graphIds.get(i));
                    }
                }
            }
            for (ObjectId graphId : unsynced) {
                if (!Files.exists(indexDir.resolve(graphId.toHexString() + ".complete"))) {
                    LOGGER.error("Vector sync backlog skipped, flushed index files are missing: indexId={}, dir={}",
                            graphId, indexDir);
                    service.getMetrics().incrementVectorSyncFailures();
                    continue;
                }
                service.getVectorSyncQueue().add(
                        new VectorSyncTask(metadata.namespace(), metadata.name(), indexId, graphId)
                );
                queued++;
            }
            if (page.size() < PAGE_SIZE) {
                return queued;
            }
            begin = KeySelector.firstGreaterThan(page.getLast().getKey());
        }
    }
}
