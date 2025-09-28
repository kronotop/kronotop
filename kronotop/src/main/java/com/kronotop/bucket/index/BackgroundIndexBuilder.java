/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.util.List;

public class BackgroundIndexBuilder implements Runnable {
    private final Context context;
    private final DirectorySubspace subspace;
    private final int shardId;
    private final Versionstamp taskId;
    private final IndexBuildTask task;
    private final BucketService service;
    private final boolean doNotWaitTxLimit;

    public BackgroundIndexBuilder(
            Context context,
            DirectorySubspace subspace,
            int shardId,
            Versionstamp taskId,
            IndexBuildTask task
    ) {
        this(context, subspace, shardId, taskId, task, false);
    }

    BackgroundIndexBuilder(
            Context context,
            DirectorySubspace subspace,
            int sharId,
            Versionstamp taskId,
            IndexBuildTask task,
            boolean doNotWaitTxLimit
    ) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.shardId = sharId;
        this.task = task;
        this.service = context.getService(BucketService.NAME);
        this.doNotWaitTxLimit = doNotWaitTxLimit;
    }

    private BucketMetadata refreshAndLoadBucketMetadata() throws InterruptedException {
        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Open the BucketMetadata and refresh the caches
            metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new KronotopException("index with id '" + task.getIndexId() + "' could not be found");
            }

            /*
             * A potential stop-the-world pause (e.g., JVM GC) during the sleep interval
             * does not break the logic here. Once the transaction is created, it already
             * holds a stable read version from FoundationDB. If the pause extends beyond
             * the transaction lifetime, this transaction will simply fail with "too old"
             * and the task will be marked as failed. In that case, a manual or KCP trigger
             * is required to retry. This design ensures correctness is preserved even under
             * GC pauses; the worst case is a delayed or failed task, never inconsistent state.
             */
            if (!doNotWaitTxLimit) {
                // FoundationDB transactions cannot live beyond 5s.
                // Sleeping 6s ensures that any previously opened transactions are expired.
                Thread.sleep(6000);
            }
            // Now all transactions either committed or died.
            return metadata;
        }
    }

    private void markIndexBuildTaskFailed(Throwable th) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexBuildTaskState.setError(tr, subspace, taskId, th.getMessage());
            IndexBuildTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            tr.commit().join();
        }
    }

    private Versionstamp findOutCursorVersionstamp(Transaction tr, Index primaryIndex, byte[] begin, byte[] end) {
        List<KeyValue> entries = tr.getRange(begin, end, 1).asList().join();

        KeyValue entry = entries.getFirst();
        Tuple parsedKey = primaryIndex.subspace().unpack(entry.getKey());
        return (Versionstamp) parsedKey.get(1);
    }

    private Versionstamp findOutHighestVersionstamp(Transaction tr, Index primaryIndex, byte[] begin, byte[] end) {
        List<KeyValue> entries = tr.getRange(begin, end, 1, true).asList().join();

        KeyValue entry = entries.getFirst();
        Tuple parsedKey = primaryIndex.subspace().unpack(entry.getKey());
        return (Versionstamp) parsedKey.get(1);
    }

    private void findOutBoundaries() throws InterruptedException {
        IndexBuildTaskState state = context.getFoundationDB().run(tr -> IndexBuildTaskState.load(tr, subspace, taskId));
        if (state.cursorVersionstamp() != null && state.highestVersionstamp() != null) {
            return;
        }

        BucketMetadata metadata = refreshAndLoadBucketMetadata();
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.ALL);
        byte[] begin = primaryIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        byte[] end = ByteArrayUtil.strinc(begin);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Versionstamp cursor = findOutCursorVersionstamp(tr, primaryIndex, begin, end);
            IndexBuildTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);
            Versionstamp highest = findOutHighestVersionstamp(tr, primaryIndex, begin, end);
            IndexBuildTaskState.setHighestVersionstamp(tr, subspace, taskId, highest);
            tr.commit().join();
        }
    }

    private void validateIndexBuildTask() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new IndexTaskException("no index found with id " + task.getIndexId(), true);
            }

            if (index.definition().status() == IndexStatus.READY) {
                IndexBuildTaskState.setError(tr, subspace, taskId, String.format(
                        "index with selector=%s, id=%d is ready to query",
                        index.definition().selector(),
                        index.definition().id()
                ));
                // set state
                IndexBuildTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            } else if (index.definition().status() == IndexStatus.DROPPED) {
                IndexBuildTaskState.setError(tr, subspace, taskId, String.format(
                        "index with selector=%s, id=%d is dropped",
                        index.definition().selector(),
                        index.definition().id()
                ));
                IndexBuildTaskState.setStatus(tr, subspace, taskId, IndexTaskStatus.FAILED);
            }

            IndexBuildTaskState state = IndexBuildTaskState.load(tr, subspace, taskId);
            if (state.status() == IndexTaskStatus.FAILED) {
                tr.commit().join();
                throw new IndexTaskException(state.error(), true);
            }

            // Three possibilities for IndexStatus: WAITING, BUILDING, FAILED

            if (index.definition().status() != IndexStatus.BUILDING) {
                IndexDefinition definition = index.definition().updateStatus(IndexStatus.BUILDING);
                IndexUtil.saveIndexDefinition(tr, definition, index.subspace());
                tr.commit().join();
            }
        }
    }

    @Override
    public void run() {
        try {
            findOutBoundaries();
            validateIndexBuildTask();
            scanPrimaryIndex();
        } catch (InterruptedException e) {
            // Do not mark the task as failed. Program has stopped and this task
            // can be retried.
            throw new RuntimeException(e);
        } catch (IndexTaskException exp) {
            if (exp.isFailed()) {
                markIndexBuildTaskFailed(exp);
            }
            throw exp;
        }
    }

    private void scanPrimaryIndex() {
        BucketMetadata metadata = context.getFoundationDB().run(tr ->
                BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket())
        );
        Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.READWRITE);
        BucketShard shard = service.getShard(shardId);

        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexBuildTaskState state = IndexBuildTaskState.load(tr, subspace, taskId);
                if (state.cursorVersionstamp().equals(state.highestVersionstamp())) {
                    break;
                }

                VersionstampedKeySelector begin = VersionstampedKeySelector.firstGreaterOrEqual(state.cursorVersionstamp());
                VersionstampedKeySelector end = VersionstampedKeySelector.firstGreaterThan(state.highestVersionstamp());
                VolumeSession session = new VolumeSession(tr, metadata.volumePrefix());

                Iterable<VolumeEntry> entries = shard.volume().getRange(session, begin, end, 100);
                Versionstamp cursor = null;
                for (VolumeEntry pair : entries) {
                    Object indexValue = null;
                    BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), pair.entry());
                    if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                        indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                        if (indexValue == null) {
                            // Type mismatch, continue
                            continue;
                        }
                    }
                    IndexBuilder.insertIndexEntry(tr, index.definition(), metadata, pair.key(), indexValue, pair.metadata());
                    cursor = pair.key();
                }
                if (cursor != null) {
                    IndexBuildTaskState.setCursorVersionstamp(tr, subspace, taskId, cursor);
                }
                tr.commit().join();
            }
        }
    }
}
