/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.redis.storage.syncer;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.handlers.hash.HashFieldValue;
import com.kronotop.redis.handlers.hash.HashValue;
import com.kronotop.redis.handlers.string.StringValue;
import com.kronotop.redis.storage.*;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import com.kronotop.redis.storage.syncer.jobs.AppendHashFieldJob;
import com.kronotop.redis.storage.syncer.jobs.AppendStringJob;
import com.kronotop.redis.storage.syncer.jobs.DeleteByVersionstampJob;
import com.kronotop.redis.storage.syncer.jobs.VolumeSyncJob;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class VolumeSyncerTest extends BaseStorageTest {

    @Test
    void shouldDoNothingWhenQueueIsEmpty() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);

        assertTrue(volumeSyncer.isQueueEmpty());
        volumeSyncer.run();
        assertTrue(volumeSyncer.isQueueEmpty());
    }

    @Test
    void shouldProcessMultipleJobsInSingleBatch() throws IOException {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);

        int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            shard.storage().put(key, new RedisValueContainer(new StringValue(value.getBytes(), 0L)));
            shard.volumeSyncQueue().add(new AppendStringJob(key));
        }

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        assertFalse(volumeSyncer.isQueueEmpty());
        volumeSyncer.run();
        assertTrue(volumeSyncer.isQueueEmpty());

        int count = 0;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            for (VolumeEntry entry : shard.volume().getRange(session)) {
                StringPack pack = StringPack.unpack(entry.entry());
                assertNotNull(pack.key());
                assertNotNull(pack.stringValue());
                count++;
            }
        }
        assertEquals(numEntries, count);
    }

    @Test
    void shouldDeleteExistingEntry() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);

        // First, append an entry
        String key = "key-to-delete";
        String value = "value-to-delete";
        shard.storage().put(key, new RedisValueContainer(new StringValue(value.getBytes(), 0L)));
        shard.volumeSyncQueue().add(new AppendStringJob(key));

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        volumeSyncer.run();

        // Get the versionstamp of the appended entry
        RedisValueContainer container = shard.storage().get(key);
        assertNotNull(container.string().versionstamp());

        // Now delete the entry
        shard.volumeSyncQueue().add(new DeleteByVersionstampJob(container.string().versionstamp()));
        volumeSyncer.run();

        // Verify no entries remain
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            int count = 0;
            for (VolumeEntry ignored : shard.volume().getRange(session)) {
                count++;
            }
            assertEquals(0, count);
        }
    }

    @Test
    void shouldHandleMixedAppendAndDeleteOperations() throws IOException {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);

        // Append first entry
        String keyToDelete = "key-to-delete";
        shard.storage().put(keyToDelete, new RedisValueContainer(new StringValue("value-to-delete".getBytes(), 0L)));
        shard.volumeSyncQueue().add(new AppendStringJob(keyToDelete));
        volumeSyncer.run();

        // Get versionstamp of the entry to delete
        Versionstamp versionstampToDelete = shard.storage().get(keyToDelete).string().versionstamp();
        assertNotNull(versionstampToDelete);

        // Queue both a delete and a new append in the same batch
        String keyToKeep = "key-to-keep";
        shard.storage().put(keyToKeep, new RedisValueContainer(new StringValue("value-to-keep".getBytes(), 0L)));
        shard.volumeSyncQueue().add(new DeleteByVersionstampJob(versionstampToDelete));
        shard.volumeSyncQueue().add(new AppendStringJob(keyToKeep));
        volumeSyncer.run();

        // Verify only the new entry remains
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            int count = 0;
            for (VolumeEntry entry : shard.volume().getRange(session)) {
                StringPack pack = StringPack.unpack(entry.entry());
                assertEquals(keyToKeep, pack.key());
                count++;
            }
            assertEquals(1, count);
        }
    }

    @Test
    void shouldRequeueJobsOnSyncFailure() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);

        VolumeSyncJob failingJob = new VolumeSyncJob() {
            @Override
            public void run(VolumeSyncSession session) {
                throw new RuntimeException("Simulated failure");
            }

            @Override
            public void postHook(VolumeSyncSession session) {
            }
        };

        shard.volumeSyncQueue().add(failingJob);

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        assertFalse(volumeSyncer.isQueueEmpty());

        assertThrows(RuntimeException.class, volumeSyncer::run);

        // Job should be re-queued after failure
        assertFalse(volumeSyncer.isQueueEmpty());
    }

    @Test
    void shouldReturnVersionstampsAfterSync() {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);

        int numEntries = 3;
        for (int i = 0; i < numEntries; i++) {
            String key = "key-" + i;
            shard.storage().put(key, new RedisValueContainer(new StringValue(("value-" + i).getBytes(), 0L)));
            shard.volumeSyncQueue().add(new AppendStringJob(key));
        }

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        volumeSyncer.run();

        // Verify versionstamps are set on each entry
        for (int i = 0; i < numEntries; i++) {
            String key = "key-" + i;
            RedisValueContainer container = shard.storage().get(key);
            assertNotNull(container.string().versionstamp(), "Versionstamp should be set for " + key);
        }
    }

    @Test
    void shouldSyncStringValue() throws IOException {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        String expectedKey = "key-1";
        String expectedValue = "value-1";

        shard.storage().put(expectedKey, new RedisValueContainer(new StringValue(expectedValue.getBytes(), 0L)));
        shard.volumeSyncQueue().add(new AppendStringJob("key-1"));

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        assertFalse(volumeSyncer.isQueueEmpty());
        volumeSyncer.run();
        assertTrue(volumeSyncer.isQueueEmpty());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = shard.volume().getRange(session);
            for (VolumeEntry keyEntry : iterable) {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                assertEquals(expectedKey, pack.key());
                assertArrayEquals(expectedValue.getBytes(), pack.stringValue().value());
                assertEquals(0L, pack.stringValue().ttl());
            }
        }
    }

    @Test
    void shouldSyncHashField() throws IOException {

        String expectedKey = "hash-name";
        String expectedField = "field-name";
        String expectedValue = "value";

        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        HashValue hashValue = new HashValue();

        hashValue.put(expectedField, new HashFieldValue(expectedValue.getBytes()));
        shard.storage().put(expectedKey, new RedisValueContainer(hashValue));
        AppendHashFieldJob job = new AppendHashFieldJob("hash-name", "field-name");
        shard.volumeSyncQueue().add(job);

        VolumeSyncer volumeSyncer = new VolumeSyncer(context, shard);
        assertFalse(volumeSyncer.isQueueEmpty());
        volumeSyncer.run();
        assertTrue(volumeSyncer.isQueueEmpty());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            Iterable<VolumeEntry> iterable = shard.volume().getRange(session);
            for (VolumeEntry keyEntry : iterable) {
                HashFieldPack pack = HashFieldPack.unpack(keyEntry.entry());
                assertEquals(expectedKey, pack.key());
                assertEquals(expectedField, pack.field());
                assertArrayEquals(expectedValue.getBytes(), pack.hashFieldValue().value());
            }
        }
    }
}
