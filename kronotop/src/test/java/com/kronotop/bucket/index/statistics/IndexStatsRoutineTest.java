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

package com.kronotop.bucket.index.statistics;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

class IndexStatsRoutineTest extends BaseBucketHandlerTest {
    @Test
    void test() throws InterruptedException {
        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "age",
                BsonType.INT32,
                IndexStatus.WAITING
        );

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
            IndexUtil.create(tr, metadata.subspace(), definition);
            tr.commit().join();
        }

        IndexStatsTask task = new IndexStatsTask(TEST_NAMESPACE, TEST_BUCKET, definition.id());
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));
        System.out.println(task);
        System.out.println(taskId);
        Thread.sleep(3000);
    }
}