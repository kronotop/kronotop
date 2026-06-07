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

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;

class BucketMetadataWatcherTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    private DirectorySubspace openLastSeenVersionsSubspace(Transaction tr, int shardId, Map<Integer, DirectorySubspace> subspaces) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                bucket().
                shard(shardId).
                lastSeenVersions();
        return subspaces.computeIfAbsent(shardId,
                (ignored) -> context.getDirectoryLayer().open(tr, directory.toList()).join()
        );
    }

    @Test
    void shouldUpdateLastSeenVersion() {
        createBucket(TEST_BUCKET);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            tr.commit().join();
        }

        final Map<Integer, DirectorySubspace> subspaces = new HashMap<>();

        List<Integer> shardIds = context.getShardRegistry().getShardIds(ShardKind.BUCKET);
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            int total = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                for (int shardId : shardIds) {
                    DirectorySubspace subspace = openLastSeenVersionsSubspace(tr, shardId, subspaces);
                    byte[] key = subspace.pack(Tuple.from(metadata.uuid()));
                    byte[] value = tr.get(key).join();
                    long version = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
                    if (version == metadata.version()) {
                        total++;
                    }
                }
            }
            // Updated by all shard owners
            return total == shardIds.size();
        });
    }
}