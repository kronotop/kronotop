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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.bucket.vector.VectorGraphIndexGroup;
import com.kronotop.cluster.CollectStalePrefixesTask;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.journal.JournalName;
import com.kronotop.journal.JournalService;
import com.kronotop.volume.Prefix;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class StaleBucketCleanupTest extends BaseBucketHandlerTest {

    private boolean registryExists(UUID uuid) {
        List<String> path = BucketMetadataUtil.registryPath(context.getClusterName(), uuid);
        try (Transaction tr = context.getTransactionManager().create()) {
            return context.getDirectoryLayer().exists(tr, path).join();
        }
    }

    private boolean lastSeenVersionExists(int shardId, UUID uuid) {
        List<String> path = KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata()
                .shards().bucket().shard(shardId).lastSeenVersions().toList();
        try (Transaction tr = context.getTransactionManager().create()) {
            DirectorySubspace subspace = context.getDirectoryLayer().open(tr, path).join();
            return tr.get(subspace.pack(Tuple.from(uuid))).join() != null;
        }
    }

    private int countTasks(int shardId) {
        DirectorySubspace subspace = IndexTaskUtil.openTasksSubspace(context, shardId);
        try (Transaction tr = context.getTransactionManager().create()) {
            byte[] begin = subspace.pack(Tuple.from(TaskStorage.TASKS_MAGIC));
            List<KeyValue> entries = tr.getRange(new Range(begin, ByteArrayUtil.strinc(begin))).asList().join();
            return entries.size();
        }
    }

    @Test
    void shouldRemoveBucketLeftoversAfterNamespacePurge() throws IOException {
        // Behavior: a bucket with a vector index and a pending index building task lives in a
        // namespace that is removed and purged.NAMESPACE PURGE leaves the bucket's registry directory,
        // its index maintenance tasks, its lastSeenVersions records and its vector files on disk behind.
        // Running the stale prefix collector publishes a stale bucket event, and the watcher removes
        // all four leftovers.
        String namespace = newNamespace();
        useNamespace(channel, namespace);
        assertOK(run(bucketCmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes(
                "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"
        ))));
        insertRaw("{\"label\": \"test\", \"embedding\": [1.0, 0.0, 0.0]}");
        // Creating an index on a bucket with data schedules an index building task.
        assertOK(run(bucketCmd.indexCreate(TEST_BUCKET, "{\"label\": {\"name\": \"label-index\", \"bson_type\": \"string\"}}")));

        BucketMetadata metadata;
        try (Transaction tr = context.getTransactionManager().create()) {
            metadata = BucketMetadataUtil.reload(context, tr, namespace, TEST_BUCKET);
        }
        UUID uuid = metadata.uuid();
        List<Integer> shards = metadata.shards();

        BucketService bucketService = context.getService(BucketService.NAME);
        Path vectorDir = VectorGraphIndexGroup.resolveVectorDir(
                bucketService.getBucketDataDir(),
                VectorGraphIndexGroup.ownerShardId(metadata)
        ).resolve(uuid.toString());
        Files.createDirectories(vectorDir);
        Files.writeString(vectorDir.resolve("test.index"), "dummy");

        assertTrue(registryExists(uuid));
        assertTrue(shards.stream().anyMatch(shardId -> lastSeenVersionExists(shardId, uuid)));
        assertTrue(shards.stream().anyMatch(shardId -> countTasks(shardId) > 0));

        KronotopCommandBuilder<String, String> nsCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        assertOK(run(nsCmd.namespaceRemove(namespace)));
        assertOK(run(nsCmd.namespacePurge(namespace)));

        new CollectStalePrefixesTask(context).run();

        await().atMost(Duration.ofSeconds(15)).until(() -> !registryExists(uuid));
        for (int shardId : shards) {
            assertFalse(lastSeenVersionExists(shardId, uuid));
            assertEquals(0, countTasks(shardId));
        }
        assertFalse(Files.exists(vectorDir));
    }

    @Test
    void shouldDrainBucketEventsInBatches() {
        // Behavior: more stale bucket events than two batches are published in one transaction for
        // buckets that have no registry directory. Each event's prefix has a binding in the bucket
        // metadata cache. The watcher consumes the journal batch by batch, and the binding of every
        // event, including the last one, is removed from the cache.
        BucketMetadataCache cache = context.getBucketMetadataCache();
        List<Prefix> prefixes = new ArrayList<>();
        JournalService journal = context.getService(JournalService.NAME);
        try (Transaction tr = context.getTransactionManager().create()) {
            for (int i = 0; i < 2 * BucketEventsWatcher.BUCKET_EVENTS_BATCH_SIZE + 1; i++) {
                Prefix prefix = Prefix.fromLong(ThreadLocalRandom.current().nextLong());
                cache.setBinding(prefix, "ns", "bucket");
                prefixes.add(prefix);
                StaleBucketEvent event = new StaleBucketEvent(prefix.asLong(), UUID.randomUUID().toString());
                journal.getPublisher().publish(tr, JournalName.BUCKET_EVENTS, event);
            }
            tr.commit().join();
        }

        await().atMost(Duration.ofSeconds(15)).until(
                () -> prefixes.stream().allMatch(prefix -> cache.getBinding(prefix) == null)
        );
    }
}
