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
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.NoSuchBucketException;

import java.util.List;

public class BackgroundIndexBuilder implements Runnable {
    private final Context context;
    private final IndexMaintenanceTask task;

    public BackgroundIndexBuilder(Context context, IndexMaintenanceTask task) {
        this.context = context;
        this.task = task;
    }

    @Override
    public void run() {
        if (task.getHighestVersionstamp() == null) {
            BucketMetadata metadata;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                metadata = BucketMetadataUtil.open(context, tr, task.getNamespace(), task.getBucket());
                Index index = metadata.indexes().getIndexById(task.getIndexId(), IndexSelectionPolicy.ALL);
                if (index == null) {
                    // TODO: Index is probably removed, mark the task obsolete.
                    throw new KronotopException("Index with id '" + task.getIndexId() + "' could not be found");
                }
                // Metadata has refreshed or it was already fresh. Wait for 6000ms
                Thread.sleep(6000);
                // Now all transactions either committed or died.
            } catch (NoSuchBucketException e) {
                // TODO: Bucket or namespace is probably removed, mark the task obsolete.
                throw new KronotopException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.ALL);
            byte[] begin = primaryIndex.subspace().pack();
            byte[] end = ByteArrayUtil.strinc(begin);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                List<KeyValue> items = tr.getRange(begin, end, 1, true).asList().join();
                KeyValue keyValue = items.getFirst();
                Tuple tuple = primaryIndex.subspace().unpack(keyValue.getKey());
                Versionstamp versionstamp = (Versionstamp) tuple.get(1);
                task.setHighestVersionstamp(versionstamp);
            }
        }
    }
}
