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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import org.bson.BsonType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

class IndexStatsRoutineTest extends BaseBucketHandlerTest {

    protected List<byte[]> makeDummyDocumentInt(int number) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            String document = String.format("{\"numeric\": %d}", i);
            result.add(BSONUtil.jsonToDocumentThenBytes(document));
        }
        return result;
    }

    @Test
    void test() throws InterruptedException {
        IndexDefinition definition = IndexDefinition.create(
                "test-index",
                "numeric",
                BsonType.INT32,
                IndexStatus.WAITING
        );

        List<byte[]> documents = makeDummyDocumentInt(1000);
        Map<String, byte[]> documentsWithVersionstamp = insertDocuments(documents, 50);
        System.out.println(documentsWithVersionstamp.size());

        createIndexThenWaitForReadiness(definition);

        DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, SHARD_ID);
        IndexStatsTask task = new IndexStatsTask(TEST_NAMESPACE, TEST_BUCKET, definition.id());
        Versionstamp taskId = TaskStorage.create(context, taskSubspace, JSONUtil.writeValueAsBytes(task));
        System.out.println(task);
        System.out.println(taskId);
        Thread.sleep(3000);
    }
}