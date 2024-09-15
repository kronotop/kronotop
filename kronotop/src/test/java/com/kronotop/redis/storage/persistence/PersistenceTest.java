/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.Transaction;
import com.kronotop.redis.hash.HashFieldValue;
import com.kronotop.redis.hash.HashValue;
import com.kronotop.redis.storage.BaseStorageTest;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.impl.OnHeapRedisShardImpl;
import com.kronotop.redis.storage.persistence.jobs.AppendHashFieldJob;
import com.kronotop.redis.storage.persistence.jobs.AppendStringJob;
import com.kronotop.redis.string.StringValue;
import com.kronotop.volume.KeyEntry;
import com.kronotop.volume.Session;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class PersistenceTest extends BaseStorageTest {

    @Test
    public void test_STRING() throws IOException {
        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        String expectedKey = "key-1";
        String expectedValue = "value-1";

        shard.storage().put(expectedKey, new RedisValueContainer(new StringValue(expectedValue.getBytes(), 0L)));
        shard.persistenceQueue().add(new AppendStringJob("key-1"));

        Persistence persistence = new Persistence(context, shard);
        assertFalse(persistence.isQueueEmpty());
        persistence.run();
        assertTrue(persistence.isQueueEmpty());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr);
            Iterable<KeyEntry> iterable = shard.volume().getRange(session);
            for (KeyEntry keyEntry : iterable) {
                StringPack pack = StringPack.unpack(keyEntry.entry());
                assertEquals(expectedKey, pack.key());
                assertArrayEquals(expectedValue.getBytes(), pack.stringValue().value());
                assertEquals(0L, pack.stringValue().ttl());
            }
        }
    }

    @Test
    public void test_HASH() throws IOException {

        String expectedKey = "hash-name";
        String expectedField = "field-name";
        String expectedValue = "value";

        RedisShard shard = new OnHeapRedisShardImpl(context, 0);
        HashValue hashValue = new HashValue();

        hashValue.put(expectedField, new HashFieldValue(expectedValue.getBytes()));
        shard.storage().put(expectedKey, new RedisValueContainer(hashValue));
        AppendHashFieldJob job = new AppendHashFieldJob("hash-name", "field-name");
        shard.persistenceQueue().add(job);

        Persistence persistence = new Persistence(context, shard);
        assertFalse(persistence.isQueueEmpty());
        persistence.run();
        assertTrue(persistence.isQueueEmpty());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Session session = new Session(tr);
            Iterable<KeyEntry> iterable = shard.volume().getRange(session);
            for (KeyEntry keyEntry : iterable) {
                HashFieldPack pack = HashFieldPack.unpack(keyEntry.entry());
                assertEquals(expectedKey, pack.key());
                assertEquals(expectedField, pack.field());
                assertArrayEquals(expectedValue.getBytes(), pack.hashFieldValue().value());
            }
        }
    }
}
