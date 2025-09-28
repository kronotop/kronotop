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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.internal.JSONUtil;

import java.util.List;

public class BackgroundIndexBuilder implements Runnable {
    private final Context context;
    private final DirectorySubspace subspace;
    private final Versionstamp taskId;
    private final IndexBuildTask task;

    public BackgroundIndexBuilder(
            Context context,
            DirectorySubspace subspace,
            Versionstamp taskId,
            IndexBuildTask task
    ) {
        this.context = context;
        this.subspace = subspace;
        this.taskId = taskId;
        this.task = task;
    }

    private void saveIndexTask() {
        context.getFoundationDB().run((tr) -> {
            byte[] key = subspace.pack(taskId);
            tr.set(key, JSONUtil.writeValueAsBytes(task));
            return null;
        });
    }

    private void markTaskFailed(Throwable e) {
        task.setError(e.getMessage());
        task.setStatus(IndexTaskStatus.FAILED);
        saveIndexTask();
    }

    private BucketMetadata refreshAndLoadBucketMetadata() {
        BucketMetadata metadata;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (index == null) {
                throw new KronotopException("index with id '" + task.getIndexId() + "' could not be found");
            }

            // Metadata has refreshed or it was already fresh. Wait for 6000ms
            Thread.sleep(10);
            // Now all transactions either committed or died.
            return metadata;
        } catch (InterruptedException e) {
            // Do not mark the task as failed. Program has stopped and this task
            // can be retried.
            throw new RuntimeException(e);
        } catch (Exception e) {
            markTaskFailed(e);
            throw e;
        }
    }

    private void findOutHighestVersionstamp() {
        if (task.getHighestVersionstamp() != null) {
            // Task already has the highest versionstamp record.
            return;
        }

        BucketMetadata metadata = refreshAndLoadBucketMetadata();

        // Find the highest versionstamp
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.ALL);
        byte[] begin = primaryIndex.subspace().pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        byte[] end = ByteArrayUtil.strinc(begin);

        // This will retry the business logic if FDB raises a conflict
        context.getFoundationDB().run(tr -> {
            List<KeyValue> entries = tr.getRange(begin, end, 1, true).asList().join();

            KeyValue entry = entries.getFirst();
            Tuple parsedKey = primaryIndex.subspace().unpack(entry.getKey());
            Versionstamp versionstamp = (Versionstamp) parsedKey.get(1);
            task.setHighestVersionstamp(versionstamp);

            tr.set(subspace.pack(taskId), JSONUtil.writeValueAsBytes(task));
            return null;
        });
    }

    @Override
    public void run() {
        findOutHighestVersionstamp();

        Result result = context.getFoundationDB().run(tr -> {
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
            Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
            if (index == null) {
                KronotopException exp = new KronotopException("no index found with id " + task.getIndexId());
                markTaskFailed(exp);
                throw exp;
            }

            if (index.definition().status() == IndexStatus.READY) {
                task.setError(String.format(
                        "index with selector=%s, id=%d is ready to query",
                        index.definition().selector(),
                        index.definition().id()
                ));
                task.setStatus(IndexTaskStatus.FAILED);
            } else if (index.definition().status() == IndexStatus.DROPPED) {
                task.setError(String.format(
                        "index with selector=%s, id=%d is dropped",
                        index.definition().selector(),
                        index.definition().id()
                ));
                task.setStatus(IndexTaskStatus.FAILED);
            }

            if (task.getStatus() == IndexTaskStatus.FAILED) {
                KronotopException exp = new KronotopException(task.getError());
                markTaskFailed(exp);
                throw exp;
            }


            // Three possibilities for IndexStatus: WAITING, BUILDING, FAILED

            if (index.definition().status() != IndexStatus.BUILDING) {
                // db.run wil retry the business logic if FDB raises a conflict.
                index.definition().updateStatus(IndexStatus.BUILDING);
                IndexUtil.saveIndexDefinition(tr, index.definition(), index.subspace());
            }
            return new Result(metadata, index);
        });


    }

    record Result(BucketMetadata metadata, Index index) {
    }
}
